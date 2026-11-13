/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import io.trino.Session;
import io.trino.block.BlockJsonSerde;
import io.trino.metadata.HandleJsonModule;
import io.trino.metadata.HandleResolver;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.IcebergSplit;
import io.trino.plugin.iceberg.IcebergFileFormat;
import io.trino.spi.Plugin;
import io.trino.spi.SplitWeight;
import io.trino.spi.block.Block;
import io.trino.spi.connector.*;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.spi.type.Type;
import io.trino.spi.variant.Variant;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.testing.MaterializedResult;
import io.trino.testing.PlanTester;
import io.trino.type.TypeDeserializer;
import org.locationtech.geomesa.trino.variant.GeoMesaVariantFunctions;
import org.locationtech.geomesa.trino.variant.VariantPredicate;
import org.locationtech.geomesa.trino.variant.VariantPredicateCompiler;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Stream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.VariantType.VARIANT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.*;
import static org.locationtech.geomesa.trino.spatial.iceberg.connector.VariantTestFixtures.*;

/** Runs against a complete Trino test classpath against real Parquet without starting a server or Docker. */
public final class VariantSqlSmoke {
    private static final List<Variant> DOCS = Arrays.asList(
        Variant.ofObject(Map.of(utf8Slice("a"), Variant.ofLong(7))),
        Variant.ofObject(Map.of(utf8Slice("a"), Variant.ofLong(8))),
        Variant.ofLong(7), null,
        Variant.ofObject(Map.of(utf8Slice("a"), Variant.NULL_VALUE)),
        Variant.EMPTY_OBJECT,
        Variant.ofObject(Map.of(utf8Slice("items"), Variant.ofArray(List.of(
            Variant.ofObject(Map.of(utf8Slice("a"), Variant.ofLong(7), utf8Slice("b"), Variant.ofLong(0))),
            Variant.ofObject(Map.of(utf8Slice("a"), Variant.ofLong(0), utf8Slice("b"), Variant.ofLong(8))))))));

    private static final List<ConnectorPageSource> SOURCES = new java.util.ArrayList<>();
    private static final Map<String, PlanTester> EXECUTORS = new java.util.LinkedHashMap<>();
    private VariantSqlSmoke() {}

    static PlanTester tester(Map<String, String> properties, byte[] bytes, int rows, java.util.function.Consumer<ConnectorPageSource> sources) {
        return tester(properties, bytes, rows, sources, false);
    }
    static PlanTester tester(Map<String, String> properties, byte[] bytes, int rows, java.util.function.Consumer<ConnectorPageSource> sources, boolean composed) {
        var builder = testSessionBuilder().setCatalog("variant_sql").setSchema("s");
        properties.forEach((name, value) -> builder.setCatalogSessionProperty("variant_sql", name, value));
        PlanTester tester = PlanTester.create(builder.build());
        tester.installPlugin(new Plugin() { @Override public Set<Class<?>> getFunctions() { return Set.of(GeoMesaVariantFunctions.class); } });
        tester.createCatalog("variant_sql", factory(bytes, rows, sources, composed), Map.of());
        return tester;
    }

    static PlanTester tester(Map<String, String> properties, byte[] bytes, int rows,
            java.util.function.Consumer<ConnectorPageSource> sources, org.apache.iceberg.Schema schema) {
        var builder = testSessionBuilder().setCatalog("variant_sql").setSchema("s");
        properties.forEach((name, value) -> builder.setCatalogSessionProperty("variant_sql", name, value));
        PlanTester tester = PlanTester.create(builder.build());
        tester.installPlugin(new Plugin() { @Override public Set<Class<?>> getFunctions() { return Set.of(GeoMesaVariantFunctions.class); } });
        tester.createCatalog("variant_sql", factory(bytes, rows, sources, false, schema), Map.of());
        return tester;
    }

    public static void main(String[] args) {
        try (PlanTester tester = PlanTester.create(testSessionBuilder().setCatalog("variant_sql").setSchema("s").build())) {
            tester.installPlugin(new Plugin() {
                @Override public Set<Class<?>> getFunctions() { return Set.of(GeoMesaVariantFunctions.class); }
            });
            tester.createCatalog("variant_sql", factory(), Map.of());
            // SQL-null invocation and empty/key/list path semantics, with optimization disabled.
            check(tester, "SELECT geomesa_variant_equals(NULL, ARRAY['a'], CAST(7 AS VARIANT))", false);
            check(tester, "SELECT geomesa_variant_equals(CAST(7 AS VARIANT), NULL, CAST(7 AS VARIANT))", false);
            check(tester, "SELECT geomesa_variant_equals(CAST(7 AS VARIANT), ARRAY[CAST(NULL AS VARCHAR)], CAST(7 AS VARIANT))", false);
            check(tester, "SELECT geomesa_variant_equals(CAST(7 AS VARIANT), ARRAY[], CAST(7 AS VARIANT))", true);
            check(tester, "SELECT geomesa_variant_any_equals(NULL, ARRAY[], ARRAY[], CAST(7 AS VARIANT))", false);
            check(tester, "SELECT geomesa_variant_get(CAST(7 AS VARIANT), ARRAY['a']) IS NULL", true);
            check(tester, "SELECT geomesa_variant_get(NULL, ARRAY[]) IS NULL", true);
            check(tester, "SELECT geomesa_variant_equals(doc, ARRAY['a'], CAST(json_parse('null') AS VARIANT)) FROM docs WHERE id=4", true);

            String exact = "geomesa_variant_equals(doc, ARRAY['a'], CAST(7 AS VARIANT))";
            List<String> queries = List.of(
                "SELECT id FROM docs WHERE " + exact,
                "SELECT count(*) FROM docs WHERE " + exact,
                "SELECT CAST(doc['a'] AS BIGINT) FROM docs WHERE " + exact,
                "SELECT doc FROM docs WHERE " + exact,
                "SELECT id FROM docs WHERE " + exact + " AND TRY(CAST(doc['a'] AS BIGINT)) = 7",
                "SELECT id FROM docs WHERE " + exact + " AND CASE WHEN id = 0 THEN true ELSE false END",
                "SELECT id FROM docs WHERE " + exact + " AND IF(id = 0, TRY(CAST(doc['a'] AS BIGINT)) = 7, false)",
                "SELECT count(*) FROM docs WHERE geomesa_variant_equals(doc, ARRAY['missing'], CAST(7 AS VARIANT))",
                "SELECT id FROM docs WHERE " + exact + " ORDER BY id DESC LIMIT 1",
                "SELECT CAST(doc['a'] AS BIGINT), count(*) FROM docs WHERE " + exact + " GROUP BY 1",
                "SELECT id FROM docs WHERE " + exact + " AND random() >= 0",
                "SELECT id FROM docs WHERE " + exact + " AND CASE WHEN id=0 THEN true ELSE TRY(1/(id-id)) > 0 END",
                "SELECT l.id, r.id FROM docs l JOIN docs r ON l.id=r.id WHERE " + exact.replace("doc", "l.doc") + " AND " + exact.replace("doc", "r.doc"),
                "SELECT id FROM docs WHERE geomesa_variant_any_equals(doc, ARRAY['items'], ARRAY['a'], CAST(7 AS VARIANT))"
                    + " AND geomesa_variant_any_equals(doc, ARRAY['items'], ARRAY['b'], CAST(8 AS VARIANT))");
            for (String query : queries) {
                MaterializedResult off = execute(tester, query, false);
                MaterializedResult on = execute(tester, query, true);
                assertThat(on.getMaterializedRows()).as(query).isEqualTo(off.getMaterializedRows());
                for (boolean projection : List.of(false, true)) {
                    assertThat(execute(tester, query, "typed", projection).getMaterializedRows()).as("typed: " + query).isEqualTo(off.getMaterializedRows());
                    if (!query.contains(" JOIN ")) assertThat(metric("Variant typed rows") + metric("Variant fallback rows")).as("typed runtime: " + query).isPositive();
                }
                Session session = Session.builder(tester.getDefaultSession()).setCatalogSessionProperty("variant_sql", "variant_filter_mode", "reference").build();
                var plan = tester.inTransaction(session, s -> tester.createPlan(s, query));
                String filters = nodes(plan.getRoot()).filter(FilterNode.class::isInstance).map(FilterNode.class::cast)
                    .map(f -> f.getPredicate().toString()).toList().toString();
                if (!query.contains(" JOIN ")) assertThat(filters).as(query).doesNotContain("geomesa_variant_equals", "geomesa_variant_any_equals");
                else assertThat(off.getMaterializedRows()).extracting(row -> row.getFields()).containsExactly(List.of(0L, 0L));
            }
            assertThat(execute(tester, queries.getLast(), true).getOnlyColumnAsSet()).containsExactly(6L);
            metadataOptimizationChecks(tester);
            projectionChecks(tester);
            nativeChecks(tester);
            codecs(tester);
            phaseOneChecks(tester);
            System.out.println("VARIANT SQL smoke passed: 8 SQL semantics checks, 14 off/reference result+plan pairs, 28 typed comparisons, handle codecs");
        } finally {
            EXECUTORS.values().forEach(PlanTester::close);
            EXECUTORS.clear();
        }
    }

    private static void metadataOptimizationChecks(PlanTester tester) {
        String predicate = "geomesa_variant_equals(doc, ARRAY['a'], CAST(7 AS VARIANT))";
        for (String query : List.of("SELECT DISTINCT id FROM docs WHERE " + predicate,
                "SELECT min(id) FROM docs WHERE " + predicate,
                "SELECT max(id) FROM docs WHERE " + predicate,
                "SELECT count(DISTINCT id) FROM docs WHERE " + predicate)) {
            var off = execute(tester, query, false);
            var on = tester.executePlan(session -> tester.createPlan(session.withProperties(
                Map.of("optimize_metadata_queries", "true"), Map.of("variant_sql", Map.of("variant_filter_mode", "reference"))), query),
                new PlanTester.MaterializedResultOutput());
            assertThat(on.getMaterializedRows()).as(query).isEqualTo(off.getMaterializedRows());
        }
        System.out.println("VARIANT metadata optimizer smoke passed: DISTINCT/min/max/count(DISTINCT) retain row filtering");
    }

    private static void projectionChecks(PlanTester tester) {
        List<String> queries = List.of(
            "SELECT doc['a'] FROM docs WHERE id IN (0, 1)",
            "SELECT CAST(doc['a'] AS BIGINT) FROM docs WHERE id IN (0, 1)",
            "SELECT doc['a'], doc['missing'] FROM docs WHERE id IN (0, 1)",
            "SELECT doc['a'], doc FROM docs WHERE id IN (0, 1)",
            "SELECT TRY(CAST(doc['a'] AS BIGINT)) FROM docs",
            "SELECT CASE WHEN id = 2 THEN CAST(99 AS VARIANT) ELSE doc['a'] END FROM docs",
            "SELECT IF(id = 2, CAST(99 AS VARIANT), doc['a']) FROM docs",
            "SELECT geomesa_variant_get(doc, ARRAY['a']) FROM docs",
            "SELECT geomesa_variant_any_equals(doc, ARRAY['items'], ARRAY['a'], CAST(7 AS VARIANT)) FROM docs",
            "SELECT doc['a'] FROM docs WHERE geomesa_variant_equals(doc, ARRAY['a'], CAST(7 AS VARIANT))",
            "SELECT doc['a'] FROM docs WHERE doc = CAST(json_parse('{\"a\":7}') AS VARIANT)");
        for (String query : queries) {
            for (boolean filter : List.of(false, true)) {
                var off = execute(tester, query, filter, false);
                var on = execute(tester, query, filter, true);
                assertThat(on.getMaterializedRows()).as(query).isEqualTo(off.getMaterializedRows());
            }
        }
        // This plan must actually carry a restricted read, not just produce the right result.
        var session = Session.builder(tester.getDefaultSession()).setCatalogSessionProperty("variant_sql", "variant_projection_pushdown_enabled", "true").build();
        for (String query : List.of("SELECT geomesa_variant_get(doc, ARRAY['a']) FROM docs", "SELECT doc['a'] FROM docs",
                "SELECT CAST(doc['a'] AS BIGINT) FROM docs")) {
            assertThat(scanColumns(tester, session, query)).as(query).anySatisfy(column ->
                assertThat(column.getVariantPaths()).contains(Set.of(io.trino.parquet.VariantPath.keys("a"))));
        }
        for (String query : List.of("SELECT doc['a'], doc FROM docs WHERE id IN (0, 1)",
                "SELECT doc['a'] FROM docs WHERE doc = CAST(json_parse('{\"a\":7}') AS VARIANT)")) {
            assertThat(scanColumns(tester, session, query)).as(query).filteredOn(column -> column.getType().equals(VARIANT))
                .isNotEmpty().allSatisfy(column -> assertThat(column.getVariantPaths()).isEmpty());
        }
        String throwing = "SELECT doc['a'] FROM docs";
        Throwable off = catchThrowable(() -> execute(tester, throwing, false, false));
        Throwable on = catchThrowable(() -> execute(tester, throwing, false, true));
        assertThat(off).isInstanceOf(io.trino.spi.TrinoException.class);
        assertThat(on).isInstanceOf(off.getClass()).hasMessage(off.getMessage());
        System.out.println("VARIANT projection SQL smoke passed: 22 off/on comparisons on real Parquet, pruned scan plan and error equivalence");
    }

    private static List<IcebergColumnHandle> scanColumns(PlanTester tester, Session session, String query) {
        var plan = tester.inTransaction(session, transaction -> tester.createPlan(transaction, query));
        return nodes(plan.getRoot()).filter(io.trino.sql.planner.plan.TableScanNode.class::isInstance)
            .map(io.trino.sql.planner.plan.TableScanNode.class::cast).flatMap(scan -> scan.getAssignments().values().stream())
            .filter(IcebergColumnHandle.class::isInstance).map(IcebergColumnHandle.class::cast).toList();
    }

    private static void nativeChecks(PlanTester tester) {
        // The scalar-root row is excluded by an ordinary column predicate in positive cases.
        List<String> predicates = List.of("doc['a'] = CAST(7 AS VARIANT)",
            "doc['a'] IN (CAST(7 AS VARIANT), CAST(8 AS VARIANT))",
            "doc['a'] IN (CAST(7 AS VARIANT), CAST(NULL AS VARIANT))",
            "doc['a'] IS NULL", "doc['a'] IS NOT NULL",
            "doc['a'] = CAST(json_parse('null') AS VARIANT)",
            "doc['a'] = CAST(7 AS VARIANT) AND geomesa_variant_equals(doc, ARRAY['a'], CAST(7 AS VARIANT))");
        var nativeSession = Session.builder(tester.getDefaultSession())
            .setCatalogSessionProperty("variant_sql", "variant_filter_mode", "typed")
            .setCatalogSessionProperty("variant_sql", "variant_native_prefilter_enabled", "true").build();
        for (String predicate : predicates) {
            for (String output : List.of("id", "doc", "count(*)")) {
                String query = "SELECT " + output + " FROM docs WHERE id <> 2 AND " + predicate;
                var off = execute(tester, query, false);
                for (boolean projection : List.of(false, true)) {
                    assertThat(execute(tester, query, "typed", projection, true).getMaterializedRows()).as(query).isEqualTo(off.getMaterializedRows());
                    assertThat(metric("Variant native decided rows") + metric("Variant unknown rows")).as("native runtime: " + query).isPositive();
                }
                var plan = tester.inTransaction(nativeSession, session -> tester.createPlan(session, query));
                assertThat(nodes(plan.getRoot()).filter(FilterNode.class::isInstance).toList()).as(query).isNotEmpty();
                assertThat(nodes(plan.getRoot()).filter(io.trino.sql.planner.plan.TableScanNode.class::isInstance)
                    .map(io.trino.sql.planner.plan.TableScanNode.class::cast).map(scan -> scan.getTable().connectorHandle()).toList())
                    .as(query).anySatisfy(handle -> assertThat(((VariantTableHandle) handle).hints()).isNotEmpty());
            }
        }
        for (String query : List.of(
                "SELECT id FROM docs WHERE TRY(CAST(doc['a'] AS BIGINT)) = 7 AND doc['a'] = CAST(7 AS VARIANT)",
                "SELECT id FROM docs WHERE CASE WHEN id=2 THEN false ELSE random() >= 0 END AND doc['a'] = CAST(7 AS VARIANT)")) {
            var result = new java.util.concurrent.atomic.AtomicReference<MaterializedResult>();
            Throwable off = catchThrowable(() -> result.set(execute(tester, query, false)));
            System.out.println("Native residual baseline: " + (off == null ? "success" : off.getMessage()) + " -- " + query);
            for (boolean projection : List.of(false, true)) {
                if (off == null) {
                    assertThat(execute(tester, query, "typed", projection, true).getMaterializedRows()).as(query).isEqualTo(result.get().getMaterializedRows());
                    assertThat(metric("Variant unknown rows")).as("opaque residual keeps undecidable rows: " + query).isPositive();
                } else {
                    Throwable on = catchThrowable(() -> execute(tester, query, "typed", projection, true));
                    assertThat(on).as(query).isInstanceOf(off.getClass()).hasMessage(off.getMessage());
                }
            }
        }
        for (String predicate : predicates.subList(0, 5)) {
            String query = "SELECT id FROM docs WHERE " + predicate;
            Throwable off = catchThrowable(() -> execute(tester, query, false));
            Throwable on = catchThrowable(() -> execute(tester, query, "typed", true, true));
            assertThat(off).as(query).isInstanceOf(io.trino.spi.TrinoException.class);
            assertThat(on).as(query).isInstanceOf(off.getClass()).hasMessage(off.getMessage());
        }
        System.out.println("VARIANT native SQL smoke passed: 42 result comparisons, 4 residual outcome comparisons, native residual plans, 5 error comparisons");
    }


    private static void phaseOneChecks(PlanTester tester) {
        check(tester, "SELECT geomesa_variant_may_equal(NULL, ARRAY['a'], CAST('x' AS VARIANT))", true);
        check(tester, "SELECT geomesa_variant_any_may_equal(CAST(JSON '[]' AS VARIANT), ARRAY[], ARRAY[], CAST('x' AS VARIANT))", true);
        check(tester, "SELECT geomesa_variant_fields_equal(CAST(JSON '{\"a\":{\"v\":\"x\"}}' AS VARIANT), ARRAY[], ARRAY['v'], CAST('x' AS VARIANT))", true);
        check(tester, "SELECT geomesa_variant_fields_may_equal(CAST(JSON '{\"a\":\"other\"}' AS VARIANT), ARRAY[], ARRAY[], CAST('x' AS VARIANT))", false);
        check(tester, "SELECT geomesa_variant_contains(CAST(JSON '[1,null]' AS VARIANT), ARRAY[], CAST(JSON 'null' AS VARIANT))", true);
        for (String predicate : List.of("geomesa_variant_may_equal(doc, ARRAY['a'], CAST('other' AS VARIANT))",
                "geomesa_variant_any_may_equal(doc, ARRAY['items'], ARRAY['a'], CAST('other' AS VARIANT))",
                "geomesa_variant_contains(doc, ARRAY['items'], CAST(7 AS VARIANT))",
                "geomesa_variant_fields_may_equal(doc, ARRAY[], ARRAY[], CAST('other' AS VARIANT))",
                "geomesa_variant_fields_equal(doc, ARRAY[], ARRAY[], CAST(7 AS VARIANT))")) {
            for (String output : List.of("id", "doc")) {
                String sql = "SELECT " + output + " FROM docs WHERE " + predicate;
                var expected = execute(tester, sql, false).getMaterializedRows();
                for (String mode : List.of("reference", "typed")) {
                    for (boolean projection : List.of(false, true)) {
                        assertThat(execute(tester, sql, mode, projection).getMaterializedRows()).as(sql).isEqualTo(expected);
                        if (mode.equals("typed") && !predicate.contains("fields_")) {
                            assertThat(metric("Variant typed rows") + metric("Variant fallback rows")).as(sql).isPositive();
                        }
                    }
                }
            }
        }
        System.out.println("Phase 1 SQL checks passed: 5 literal cases, 40 mode/projection comparisons and runtime takeover counters");
    }

    private static void codecs(PlanTester tester) {
        var context = tester.getPlannerContext();
        var mapper = new JsonMapperProvider()
            .withJsonSerializers(Map.of(Block.class, new BlockJsonSerde.Serializer(context.getBlockEncodingSerde())))
            .withJsonDeserializers(Map.of(Type.class, new TypeDeserializer(context.getTypeManager()),
                Block.class, new BlockJsonSerde.Deserializer(context.getBlockEncodingSerde())))
            .get();
        mapper.registerModule(HandleJsonModule.tableHandleModule(new HandleResolver()));
        var codec = new JsonCodecFactory(mapper).jsonCodec(ConnectorTableHandle.class);
        for (var kind : VariantPredicate.Kind.values()) {
            var predicate = new VariantPredicate(kind, DOC, List.of("a"), List.of(), new io.trino.spi.expression.Constant(Variant.ofString("001"), VARIANT));
            var handle = new VariantTableHandle(TABLE, List.of(predicate));
            assertThat(codec.fromJson(codec.toJson(handle))).isEqualTo(handle);
        }
        for (Variant expected : List.of(Variant.ofBinary(utf8Slice("raw")), Variant.ofDecimal(new BigDecimal("1.00")),
                Variant.ofDouble(Double.NaN), Variant.ofDouble(Double.POSITIVE_INFINITY), Variant.ofDouble(-0.0),
                Variant.NULL_VALUE, Variant.ofTimestampNanosUtc(123456789), Variant.ofObject(Map.of(utf8Slice("a"), Variant.ofLong(7))))) {
            VariantPredicate predicate = VariantPredicateCompiler.compile(equalsCall("d", expected, "a"), ASSIGNMENTS).orElseThrow();
            for (ConnectorTableHandle inner : List.of(TABLE, new SpatialTableHandle(TABLE, ID, List.of(), 0, 0, 1, 1),
                    new VisibilityTableHandle(TABLE, VIS, List.of("A")),
                    new VisibilityTableHandle(new SpatialTableHandle(TABLE, ID, List.of(), 0, 0, 1, 1), VIS, List.of("A")))) {
                var handle = new VariantTableHandle(inner, List.of(predicate));
                var copy = codec.fromJson(codec.toJson(handle));
                assertThat(copy).isEqualTo(handle);
                Variant actual = (Variant) ((VariantTableHandle) copy).predicates().getFirst().expected().getValue();
                assertThat(actual.primitiveType()).isEqualTo(expected.primitiveType());
                assertThat(actual.data()).isEqualTo(expected.data());
                assertThat(actual.metadata()).isEqualTo(expected.metadata());
                var hint = new org.locationtech.geomesa.trino.variant.VariantNativeHint(
                    org.locationtech.geomesa.trino.variant.VariantNativeHint.Kind.IN, DOC, List.of("a"),
                    List.of(new io.trino.spi.expression.Constant(expected, VARIANT), new io.trino.spi.expression.Constant(null, VARIANT)));
                for (var claims : List.of(List.<VariantPredicate>of(), List.of(predicate))) {
                    var nativeHandle = new VariantTableHandle(inner, claims, List.of(hint));
                    assertThat(codec.fromJson(codec.toJson(nativeHandle))).isEqualTo(nativeHandle);
                }
            }
        }
    }

    private static MaterializedResult execute(PlanTester tester, String sql, boolean enabled) {
        return execute(tester, sql, enabled, false);
    }
    private static MaterializedResult execute(PlanTester tester, String sql, boolean filter, boolean projection) {
        return execute(tester, sql, filter ? "reference" : "off", projection);
    }
    private static MaterializedResult execute(PlanTester tester, String sql, String mode, boolean projection) {
        return execute(tester, sql, mode, projection, false);
    }
    private static MaterializedResult execute(PlanTester tester, String sql, String mode, boolean projection, boolean nativePrefilter) {
        String key = mode + ":" + projection + ":" + nativePrefilter;
        PlanTester executor = EXECUTORS.computeIfAbsent(key, ignored -> tester(Map.of("variant_filter_mode", mode,
            "variant_projection_pushdown_enabled", Boolean.toString(projection), "variant_native_prefilter_enabled", Boolean.toString(nativePrefilter)),
            parquetDocuments(), DOCS.size(), SOURCES::add));
        SOURCES.clear();
        return executor.executePlan(session -> executor.createPlan(session, sql), new PlanTester.MaterializedResultOutput());
    }
    private static long metric(String name) {
        return SOURCES.stream().map(source -> source.getMetrics().getMetrics().get(name)).filter(java.util.Objects::nonNull)
            .mapToLong(value -> ((io.trino.plugin.base.metrics.LongCount) value).getTotal()).sum();
    }
    private static void check(PlanTester tester, String sql, Object expected) {
        assertThat(execute(tester, sql, false).getOnlyValue()).as(sql).isEqualTo(expected);
    }
    private static Stream<PlanNode> nodes(PlanNode node) {
        return Stream.concat(Stream.of(node), node.getSources().stream().flatMap(VariantSqlSmoke::nodes));
    }
    private enum Transaction implements ConnectorTransactionHandle { INSTANCE }

    private static byte[] parquetDocuments() {
        var schema = org.apache.parquet.schema.MessageTypeParser.parseMessageType("""
            message table {
                optional group doc (VARIANT(1)) = 1 {
                    required binary metadata;
                    optional binary value;
                    optional group typed_value {
                        required group a { optional binary value; optional int64 typed_value; }
                        required group spare { optional binary value; optional int64 typed_value; }
                    }
                }
                required int64 id = 2;
            }
            """);
        try {
            java.nio.file.Path directory = java.nio.file.Files.createTempDirectory("variant-sql-");
            java.nio.file.Path file = directory.resolve("docs.parquet");
            try {
                try (var writer = org.apache.parquet.hadoop.example.ExampleParquetWriter.builder(new org.apache.parquet.io.LocalOutputFile(file))
                        .withType(schema).build()) {
                    for (int id = 0; id < DOCS.size(); id++) {
                        var row = new org.apache.parquet.example.data.simple.SimpleGroup(schema).append("id", (long) id);
                        Variant value = DOCS.get(id);
                        if (value != null) {
                            var doc = row.addGroup("doc");
                            doc.append("metadata", org.apache.parquet.io.api.Binary.fromConstantByteArray(
                                (id < 2 ? Variant.NULL_VALUE : value).metadata().toSlice().getBytes()));
                            if (id < 2) {
                                var object = doc.addGroup("typed_value");
                                object.addGroup("a").append("typed_value", id == 0 ? 7L : 8L);
                                object.addGroup("spare");
                            } else doc.append("value", org.apache.parquet.io.api.Binary.fromConstantByteArray(value.data().getBytes()));
                        }
                        writer.write(row);
                    }
                }
                return java.nio.file.Files.readAllBytes(file);
            } finally {
                java.nio.file.Files.deleteIfExists(file);
                java.nio.file.Files.delete(directory);
            }
        } catch (java.io.IOException e) { throw new java.io.UncheckedIOException(e); }
    }

    private static ConnectorFactory factory() {
        return factory(parquetDocuments(), DOCS.size(), source -> {});
    }

    static ConnectorFactory factory(byte[] bytes, int rowCount, java.util.function.Consumer<ConnectorPageSource> sources) {
        return factory(bytes, rowCount, sources, false);
    }
    private static ConnectorFactory factory(byte[] bytes, int rowCount, java.util.function.Consumer<ConnectorPageSource> sources, boolean composed) {
        var schema = new org.apache.iceberg.Schema(
            org.apache.iceberg.types.Types.NestedField.optional(1, "doc", org.apache.iceberg.types.Types.VariantType.get()),
            org.apache.iceberg.types.Types.NestedField.required(2, "id", org.apache.iceberg.types.Types.LongType.get()),
            org.apache.iceberg.types.Types.NestedField.optional(3, "__vis__", org.apache.iceberg.types.Types.StringType.get()),
            org.apache.iceberg.types.Types.NestedField.optional(4, "geom", org.apache.iceberg.types.Types.BinaryType.get()),
            org.apache.iceberg.types.Types.NestedField.optional(5, "x", org.apache.iceberg.types.Types.FloatType.get()),
            org.apache.iceberg.types.Types.NestedField.optional(6, "y", org.apache.iceberg.types.Types.FloatType.get()));
        return factory(bytes, rowCount, sources, composed, schema);
    }

    private static ConnectorFactory factory(byte[] bytes, int rowCount, java.util.function.Consumer<ConnectorPageSource> sources,
            boolean composed, org.apache.iceberg.Schema schema) {
        var idColumn = IcebergColumnHandle.optional(io.trino.plugin.iceberg.ColumnIdentity.primitiveColumnIdentity(schema.findField("id").fieldId(), "id"))
                .columnType(io.trino.spi.type.BigintType.BIGINT).build();
        var visibilityColumn = IcebergColumnHandle.optional(io.trino.plugin.iceberg.ColumnIdentity.primitiveColumnIdentity(schema.findField("__vis__").fieldId(), "__vis__"))
                .columnType(io.trino.spi.type.VarcharType.VARCHAR).build();
        var table = new io.trino.plugin.iceberg.IcebergTableHandle("s", "docs", io.trino.plugin.iceberg.TableType.DATA,
            OptionalLong.empty(), org.apache.iceberg.SchemaParser.toJson(schema), java.util.OptionalInt.of(0),
            Map.of(0, org.apache.iceberg.PartitionSpecParser.toJson(org.apache.iceberg.PartitionSpec.unpartitioned())), 3,
            TupleDomain.all(), TupleDomain.all(), OptionalLong.empty(), Set.of(), Optional.empty(), "memory:///", Map.of(),
            Optional.empty(), false, Optional.empty(), Set.of(), Optional.empty());
        var filesystem = new io.trino.filesystem.memory.MemoryFileSystem();
        var location = io.trino.filesystem.Location.of("memory:///docs.parquet");
        try { filesystem.newOutputFile(location).createOrOverwrite(bytes); }
        catch (java.io.IOException e) { throw new java.io.UncheckedIOException(e); }
        var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
        return new ConnectorFactory() {
            @Override public String getName() { return "variant_sql"; }
            @Override public Connector create(String name, Map<String, String> config, ConnectorContext context) {
                var docColumn = IcebergColumnHandle.optional(io.trino.plugin.iceberg.ColumnIdentity.createColumnIdentity(schema.findField("doc")))
                        .columnType(io.trino.plugin.iceberg.TypeConverter.toTrinoType(schema.findField("doc").type(), context.getTypeManager())).build();
                return new SpatialConnector(new Connector() {
                    @Override public void shutdown() { executor.shutdownNow(); }
                    @Override public ConnectorTransactionHandle beginTransaction(IsolationLevel level, boolean readOnly, boolean autoCommit) { return Transaction.INSTANCE; }
                    @Override public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transaction) {
                        return new ConnectorMetadata() {
                            @Override public ConnectorTableHandle getTableHandle(ConnectorSession s, SchemaTableName name,
                                    Optional<ConnectorTableVersion> start, Optional<ConnectorTableVersion> end) {
                                if (!composed) return table;
                                var geom = IcebergColumnHandle.optional(io.trino.plugin.iceberg.ColumnIdentity.primitiveColumnIdentity(4, "geom")).columnType(io.trino.spi.type.VarbinaryType.VARBINARY).build();
                                var x = IcebergColumnHandle.optional(io.trino.plugin.iceberg.ColumnIdentity.primitiveColumnIdentity(5, "x")).columnType(io.trino.spi.type.RealType.REAL).build();
                                var y = IcebergColumnHandle.optional(io.trino.plugin.iceberg.ColumnIdentity.primitiveColumnIdentity(6, "y")).columnType(io.trino.spi.type.RealType.REAL).build();
                                return new VisibilityTableHandle(new SpatialTableHandle(table, geom, List.of(x, y, x, y), -1, -1, 7.5, 1), visibilityColumn, List.of("A"));
                            }
                            @Override public SchemaTableName getTableName(ConnectorSession s, ConnectorTableHandle h) { return table.getSchemaTableName(); }
                            @Override public ConnectorTableMetadata getTableMetadata(ConnectorSession s, ConnectorTableHandle h) {
                                return new ConnectorTableMetadata(table.getSchemaTableName(), List.of(
                                    new ColumnMetadata("id", idColumn.getType()), new ColumnMetadata("doc", docColumn.getType())));
                            }
                            @Override public Map<String, ColumnHandle> getColumnHandles(ConnectorSession s, ConnectorTableHandle h) { return Map.of("id", idColumn, "doc", docColumn); }
                            @Override public ColumnMetadata getColumnMetadata(ConnectorSession s, ConnectorTableHandle h, ColumnHandle c) {
                                var column = (IcebergColumnHandle) c;
                                return new ColumnMetadata(column.getName(), column.getType());
                            }
                            @Override public Optional<ConnectorTableCredentials> getTableCredentials(ConnectorSession s, ConnectorTableHandle h) {
                                return Optional.of(new io.trino.plugin.iceberg.IcebergTableCredentials(Map.of(), List.of()));
                            }
                            @Override public ConnectorTableProperties getTableProperties(ConnectorSession s, ConnectorTableHandle h) {
                                var partitions = java.util.stream.IntStream.range(0, rowCount).mapToObj(id ->
                                    TupleDomain.<ColumnHandle>withColumnDomains(Map.of(idColumn, io.trino.spi.predicate.Domain.singleValue(idColumn.getType(), (long) id)))).toList();
                                return new ConnectorTableProperties(TupleDomain.all(), Optional.empty(),
                                    Optional.of(new DiscretePredicates(List.of(idColumn), partitions)), List.of());
                            }
                        };
                    }
                    @Override public ConnectorSplitManager getSplitManager() {
                        return new ConnectorSplitManager() {
                            @Override public ConnectorSplitSource getSplits(ConnectorTransactionHandle t, ConnectorSession s,
                                    ConnectorTableHandle h, Set<ColumnHandle> dynamicColumns, Constraint c) {
                                var split = new IcebergSplit(location.toString(), 0, bytes.length, bytes.length, rowCount, IcebergFileFormat.PARQUET,
                                    0, List.of(), List.of(), SplitWeight.standard(), TupleDomain.all(), Optional.empty(),
                                    OptionalLong.empty(), OptionalLong.empty(), Optional.empty());
                                return new FixedSplitSource(List.of(split));
                            }
                        };
                    }
                    @Override public List<io.trino.spi.session.PropertyMetadata<?>> getSessionProperties() {
                        return new io.trino.plugin.iceberg.IcebergSessionProperties(new io.trino.plugin.iceberg.IcebergConfig(),
                            new io.trino.plugin.iceberg.encryption.IcebergEncryptionConfig(),
                            new io.trino.plugin.hive.orc.OrcReaderConfig(), new io.trino.plugin.hive.orc.OrcWriterConfig(),
                            new io.trino.plugin.hive.parquet.ParquetReaderConfig(), new io.trino.plugin.hive.parquet.ParquetWriterConfig()).getSessionProperties();
                    }
                    @Override public ConnectorPageSourceProvider getPageSourceProvider() {
                        var operators = new io.trino.spi.type.TypeOperators();
                        var hashes = new io.trino.operator.FlatHashStrategyCompiler(operators,
                            new io.trino.operator.NullSafeHashCompiler(operators)).createBlocksHashFactory();
                        var provider = new CustomFooterProviderFactory((identity, properties) -> filesystem,
                            new io.trino.plugin.iceberg.fileio.ForwardingFileIoFactory(executor),
                            new io.trino.plugin.base.metrics.FileFormatDataSourceStats(), new io.trino.plugin.hive.orc.OrcReaderConfig(),
                            new io.trino.plugin.hive.parquet.ParquetReaderConfig(), context.getTypeManager(),
                            io.trino.parquet.cache.ParquetFooterCache.noop(), hashes, new io.trino.plugin.iceberg.IcebergConfig(),
                            (keys, properties) -> org.apache.iceberg.encryption.PlaintextEncryptionManager.instance())
                            .create(composed, false, MemoryContext.NO_LIMIT);
                        return provider;
                    }
                }, null, null, false) {
                    @Override public ConnectorPageSourceProvider getPageSourceProvider() {
                        return observe(super.getPageSourceProvider(), sources);
                    }
                    @Override public ConnectorPageSourceProviderFactory getPageSourceProviderFactory() {
                        var factory = super.getPageSourceProviderFactory();
                        return memory -> observe(factory.createPageSourceProvider(memory), sources);
                    }
                };
            }
        };
    }

    private static ConnectorPageSourceProvider observe(ConnectorPageSourceProvider provider, java.util.function.Consumer<ConnectorPageSource> sources) {
        return new ConnectorPageSourceProvider() {
            @Override public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session,
                    ConnectorSplit split, ConnectorTableHandle handle, Optional<ConnectorTableCredentials> credentials,
                    List<ColumnHandle> columns, DynamicFilter dynamicFilter, MemoryContext memory) {
                var source = provider.createPageSource(transaction, session, split, handle, credentials, columns, dynamicFilter, memory);
                sources.accept(source);
                return source;
            }
        };
    }
}

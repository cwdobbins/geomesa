/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.Table;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.parquet.TypeToMessageType;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.LocalInputFile;
import org.geotools.api.data.DataStoreFinder;
import org.geotools.api.data.Query;
import org.geotools.api.data.Transaction;
import org.geotools.filter.text.ecql.ECQL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.locationtech.geomesa.features.ScalaSimpleFeature;
import org.locationtech.geomesa.fs.data.FileSystemDataStore;
import org.locationtech.geomesa.index.conf.QueryHints;
import org.locationtech.geomesa.security.VisibilityUtils;
import org.locationtech.geomesa.trino.datastore.testcontainers.GeoMesaTrinoContainer;
import org.locationtech.geomesa.trino.datastore.testcontainers.IcebergRestContainer;
import org.locationtech.geomesa.trino.datastore.testcontainers.SeaweedFsContainer;
import org.locationtech.geomesa.utils.geotools.FeatureUtils;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.trino.TrinoContainer;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Explicit stock-server gate for the source manifests and real files produced by VariantShreddingExporterIT. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ExporterVariantEndToEndIT {
    private static final String FIXTURES = "geomesa.variant.exporter-fixture-directory";
    private static final String PROTECTED_FIXTURES = "geomesa.variant.protected-exporter-fixture-directory";
    private static final String MIXED = "variant_mixed";
    private static final String DELETED = "variant_deleted";
    private static final String PROTECTED_DELETED = "protected_deleted";
    private static final List<String> PROTECTED_EXPORTED = List.of("protected_plain", "protected_sampled");
    private static final Set<Integer> DELETED_IDS = Set.of(0, 41, 44, 80);
    private static final String CATALOG = "spatial_iceberg";
    private static final String SCHEMA = "geomesa";
    private static final List<String> EXPORTED = List.of("variant_plain", "variant_sampled");
    private static final String REFERENCE = "variant_reference";
    private static final String PROTECTED = "variant_protected";
    private static final ObjectMapper JSON = new ObjectMapper()
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));

    private Network network;
    private SeaweedFsContainer s3;
    private IcebergRestContainer iceberg;
    private TrinoContainer coordinator;
    private TrinoContainer worker;
    private final Map<Integer, String> source = new TreeMap<>();
    private final Map<Integer, String> sourceVisibility = new TreeMap<>();

    private record ImportedFile(DataFile file, List<Integer> ordinals) {}

    private static List<String> allExportedTables() {
        return List.of("variant_plain", "variant_sampled", MIXED, "protected_plain", "protected_sampled");
    }

    private static class Node extends GeoMesaTrinoContainer {
        @Override protected void waitUntilContainerStarted() {
            Wait.forHttp("/v1/info").forPort(8080).forResponsePredicate(body -> {
                try { return !JSON.readTree(body).path("starting").asBoolean(true); }
                catch (java.io.IOException e) { return false; }
            }).withStartupTimeout(Duration.ofMinutes(2)).waitUntilReady(this);
        }
    }

    @BeforeAll
    void start() throws Exception {
        String configured = System.getProperty(FIXTURES);
        assertThat(configured).as("Set -D%s to retained exporter fixtures", FIXTURES).isNotBlank();
        Path root = Path.of(configured);
        for (String name : EXPORTED) {
            Map<Integer, String> rows = new TreeMap<>();
            for (String line : Files.readAllLines(root.resolve(name).resolve("expected.jsonl"))) {
                JsonNode row = JSON.readTree(line);
                assertThat(row.has("ordinal") && row.has("id") && row.has("payload")).isTrue();
                int ordinal = row.get("ordinal").intValue();
                assertThat(row.get("id").asText()).isEqualTo(Integer.toString(ordinal));
                assertThat(rows).doesNotContainKey(ordinal);
                assertThat(row.get("payload").isNull() || row.get("payload").isTextual()).isTrue();
                rows.put(ordinal, row.get("payload").isNull() ? null : row.get("payload").textValue());
            }
            assertThat(rows).hasSize(81);
            if (source.isEmpty()) source.putAll(rows);
            else assertThat(rows).isEqualTo(source);
        }

        String protectedConfigured = System.getProperty(PROTECTED_FIXTURES);
        assertThat(protectedConfigured).as("Set -D%s to retained protected exporter fixtures", PROTECTED_FIXTURES).isNotBlank();
        Path protectedRoot = Path.of(protectedConfigured);
        for (String name : EXPORTED) {
            Map<Integer, String> rows = new TreeMap<>();
            Map<Integer, String> visibilities = new TreeMap<>();
            for (String line : Files.readAllLines(protectedRoot.resolve(name).resolve("expected.jsonl"))) {
                JsonNode row = JSON.readTree(line);
                int id = row.get("ordinal").intValue();
                assertThat(rows).doesNotContainKey(id);
                assertThat(row.get("id").asText()).isEqualTo(Integer.toString(id));
                rows.put(id, row.get("payload").isNull() ? null : row.get("payload").textValue());
                String visibility = row.path("visibility").asText();
                assertThat(visibility).isIn("U", "S", "U&S", "U|S");
                visibilities.put(id, visibility);
            }
            assertThat(rows).isEqualTo(source);
            if (sourceVisibility.isEmpty()) sourceVisibility.putAll(visibilities);
            else assertThat(visibilities).isEqualTo(sourceVisibility);
        }

        network = Network.newNetwork();
        s3 = new SeaweedFsContainer("admin", "admin").withNetwork(network).withNetworkAliases("seaweed");
        iceberg = new IcebergRestContainer("admin", "admin").withNetwork(network).withNetworkAliases("rest-catalog");
        s3.start();
        iceberg.start();
        String properties = String.join("\n", "type=rest",
                "uri=http://" + iceberg.getHost() + ":" + iceberg.getFirstMappedPort() + "/",
                "iceberg.namespace=" + SCHEMA, "fs.s3.region=us-east-1", "fs.s3.endpoint=" + s3.getS3URL(),
                "fs.s3.access-key-id=admin", "fs.s3.secret-access-key=admin", "fs.s3.force-path-style=true");
        var fs = (FileSystemDataStore) DataStoreFinder.getDataStore(Map.of("fs.config.properties", properties, "geomesa.security.auths", "U,S"));
        try {
            for (String name : EXPORTED) {
                Path directory = root.resolve(name);
                fs.createSchema(SimpleFeatureTypes.createType(name, Files.readString(directory.resolve("schema.txt"))));
                appendFiles(fs.storage(name).table(), retainedFiles(directory), name.equals("variant_sampled"), false);
            }
            for (int i = 0; i < EXPORTED.size(); i++) {
                Path directory = protectedRoot.resolve(EXPORTED.get(i));
                String name = PROTECTED_EXPORTED.get(i);
                fs.createSchema(SimpleFeatureTypes.createType(name, Files.readString(directory.resolve("schema.txt"))));
                appendFiles(fs.storage(name).table(), retainedFiles(directory), i == 1, true);
            }
            Path protectedSampled = protectedRoot.resolve("variant_sampled");
            fs.createSchema(SimpleFeatureTypes.createType(PROTECTED_DELETED, Files.readString(protectedSampled.resolve("schema.txt"))));
            Table protectedDeleted = fs.storage(PROTECTED_DELETED).table();
            addDeletes(protectedDeleted, appendFiles(protectedDeleted, retainedFiles(protectedSampled), true, true));
            String spec = Files.readString(root.resolve(EXPORTED.get(0)).resolve("schema.txt"));
            List<Path> mixedFiles = new ArrayList<>();
            for (Path file : retainedFiles(root.resolve("variant_plain"))) {
                if (ordinals(file).get(0) < 40) mixedFiles.add(file);
            }
            for (Path file : retainedFiles(root.resolve("variant_sampled"))) {
                if (ordinals(file).get(0) >= 40) mixedFiles.add(file);
            }
            assertThat(mixedFiles).hasSize(3);
            for (String name : List.of(MIXED, DELETED)) {
                fs.createSchema(SimpleFeatureTypes.createType(name, spec));
                Table table = fs.storage(name).table();
                List<ImportedFile> files = appendFiles(table, mixedFiles, true, false);
                if (name.equals(DELETED)) addDeletes(table, files);
            }
            for (String name : List.of(REFERENCE, PROTECTED)) {
                var sft = SimpleFeatureTypes.createType(name, spec);
                fs.createSchema(sft);
                try (var writer = fs.getFeatureWriterAppend(name, Transaction.AUTO_COMMIT)) {
                    for (var row : source.entrySet()) {
                        var feature = new ScalaSimpleFeature(sft, row.getKey().toString(), null, null);
                        feature.setAttribute("ordinal", row.getKey());
                        feature.setAttribute("payload", row.getValue());
                        feature.setAttribute("dtg", "2026-02-18T00:00:00.000Z");
                        feature.setAttribute("geom", "POINT (1 2)");
                        if (name.equals(PROTECTED)) feature.getUserData().put("geomesa.feature.visibility", "U");
                        FeatureUtils.write(writer, feature, true);
                    }
                }
            }
        } finally { fs.dispose(); }

        coordinator = node("phase3-coordinator.properties", "trino");
        worker = node("phase3-worker.properties", "trino-worker");
        coordinator.start();
        worker.start();
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (!query("SELECT count(*) FROM system.runtime.nodes WHERE state = 'active' AND NOT coordinator", "off", false, "authorized")
                .equals(List.of(List.of("1")))) {
            assertThat(System.nanoTime()).as("Worker did not register").isLessThan(deadline);
            Thread.sleep(250);
        }
    }

    private TrinoContainer node(String config, String alias) throws Exception {
        byte[] catalog;
        byte[] server;
        try (var in = getClass().getResourceAsStream("/docker/trino/spatial_iceberg.properties")) { catalog = in.readAllBytes(); }
        try (var in = getClass().getResourceAsStream("/docker/trino/" + config)) { server = in.readAllBytes(); }
        String secured = new String(catalog, StandardCharsets.UTF_8) + "\ngeomesa.security.auth-resolver=file\n" +
                "geomesa.security.auth-mapping-file=/etc/trino/geomesa-auths.properties\n";
        return new Node().withGeoMesaPlugin().withNetwork(network).withNetworkAliases(alias)
                .withCopyToContainer(Transferable.of(server, 0644), "/etc/trino/config.properties")
                .withCopyToContainer(Transferable.of(secured, 0644), "/etc/trino/catalog/spatial_iceberg.properties")
                .withCopyToContainer(Transferable.of("user.authorized=U,S\nuser.u=U\nuser.s=S\n", 0644), "/etc/trino/geomesa-auths.properties");
    }

    private static List<Path> retainedFiles(Path directory) throws Exception {
        try (var paths = Files.list(directory)) {
            List<Path> files = paths.filter(p -> p.toString().endsWith(".parquet")).sorted().toList();
            assertThat(files).hasSize(3);
            return files;
        }
    }

    private static List<Integer> ordinals(Path file) throws Exception {
        List<Integer> ids = new ArrayList<>();
        try (var reader = new ParquetReader.Builder<Group>(new LocalInputFile(file)) {
            @Override protected ReadSupport<Group> getReadSupport() { return new GroupReadSupport(); }
        }.build()) {
            Group row;
            while ((row = reader.read()) != null) ids.add(row.getInteger("ordinal", 0));
        }
        assertThat(ids).isSorted().doesNotHaveDuplicates();
        return ids;
    }

    private List<ImportedFile> appendFiles(Table table, List<Path> files, boolean sampled, boolean protectedRows) throws Exception {
        var expectedSchema = new TypeToMessageType().convert(table.schema(), "table");
        assertThat(table.spec().fields()).hasSize(1);
        assertThat(table.spec().fields().get(0).transform().toString()).isEqualTo("hour");
        assertThat(table.spec().fields().get(0).sourceId()).isEqualTo(table.schema().findField("dtg").fieldId());
        List<ImportedFile> imported = new ArrayList<>();
        Set<Integer> importedIds = new HashSet<>();
        Set<String> layouts = new HashSet<>();
        var append = table.newAppend();
        long records = 0;
        long typed = 0;
        long fallback = 0;
        for (Path file : files) {
            PartitionData partition = new PartitionData(table.spec().partitionType());
            long rows;
            List<Integer> ids = ordinals(file);
            for (int id : ids) assertThat(importedIds.add(id)).as("duplicate imported ordinal %s", id).isTrue();
            try (var reader = ParquetFileReader.open(new LocalInputFile(file))) {
                var footer = reader.getFooter();
                var schema = footer.getFileMetaData().getSchema();
                layouts.add(schema.getType("payload").toString());
                assertThat(schema.getFields()).hasSize(expectedSchema.getFieldCount());
                for (var field : expectedSchema.getFields()) {
                    var actual = schema.getType(field.getName());
                    assertThat(actual.getId()).isEqualTo(field.getId());
                    if (field.getName().equals("payload")) {
                        assertThat(actual.getLogicalTypeAnnotation()).isEqualTo(field.getLogicalTypeAnnotation());
                    } else assertThat(actual).isEqualTo(field);
                }
                JsonNode key = JSON.readTree(footer.getFileMetaData().getKeyValueMetaData().get("geomesa.fs.partition"));
                assertThat(key).hasSize(1);
                assertThat(key.get(0).get("name").asText()).isEqualTo("hours:attribute=dtg");
                partition.set(0, Integer.parseInt(key.get(0).get("value").asText()));
                rows = footer.getBlocks().stream().mapToLong(block -> block.getRowCount()).sum();
                for (var block : footer.getBlocks()) {
                    for (var column : block.getColumns()) {
                        assertThat(column.getStatistics().isNumNullsSet()).isTrue();
                        long count = column.getValueCount() - column.getStatistics().getNumNulls();
                        String path = column.getPath().toDotString();
                        if (path.equals("payload.typed_value.n.typed_value")) typed += count;
                        if (path.equals("payload.typed_value.n.value")) fallback += count;
                        if (path.equals("__vis__")) {
                            if (protectedRows) assertThat(count).isEqualTo(block.getRowCount());
                            else assertThat(count).isZero();
                        }
                    }
                }
            }
            if (protectedRows) {
                try (var reader = new ParquetReader.Builder<Group>(new LocalInputFile(file)) {
                    @Override protected ReadSupport<Group> getReadSupport() { return new GroupReadSupport(); }
                }.build()) {
                    Group row;
                    while ((row = reader.read()) != null) {
                        assertThat(row.getFieldRepetitionCount("__vis__")).isEqualTo(1);
                        int id = row.getInteger("ordinal", 0);
                        assertThat(VisibilityUtils.normalize(row.getBinary("__vis__", 0).toStringUsingUTF8()))
                                .isEqualTo(VisibilityUtils.normalize(sourceVisibility.get(id)));
                    }
                }
            }
            String location = table.locationProvider().newDataLocation("exporter-" + UUID.randomUUID() + ".parquet");
            try (var out = table.io().newOutputFile(location).create()) { Files.copy(file, out); }
            var input = table.io().newInputFile(location);
            var metrics = ParquetUtil.fileMetrics(input, MetricsConfig.forTable(table));
            assertThat(metrics.recordCount()).isEqualTo(rows);
            assertThat(input.getLength()).isEqualTo(Files.size(file));
            DataFile dataFile = DataFiles.builder(table.spec()).withPartition(partition).withFormat(FileFormat.PARQUET)
                    .withPath(location).withFileSizeInBytes(input.getLength()).withMetrics(metrics).build();
            append.appendFile(dataFile);
            imported.add(new ImportedFile(dataFile, ids));
            records += rows;
        }
        assertThat(records).isEqualTo(source.size());
        assertThat(importedIds).containsExactlyInAnyOrderElementsOf(source.keySet());
        if (table.name().endsWith("." + MIXED) || table.name().endsWith("." + DELETED)) assertThat(layouts).hasSize(3);
        if (sampled) { assertThat(typed).isPositive(); assertThat(fallback).isPositive(); }
        else assertThat(typed).isZero();
        append.commit();
        table.refresh();
        assertThat(table.currentSnapshot().summary().get("total-records")).isEqualTo("81");
        return imported;
    }

    private static void addDeletes(Table table, List<ImportedFile> files) throws Exception {
        var delta = table.newRowDelta();
        int positions = 0;
        int equalities = 0;
        for (ImportedFile imported : files) {
            List<Integer> positionIds = imported.ordinals().stream().filter(id -> id == 0 || id == 44).toList();
            if (!positionIds.isEmpty()) {
                // VARIANT requires format V3, whose position deletes use Puffin deletion vectors.
                DVFileWriter writer = new BaseDVFileWriter(
                        OutputFileFactory.builderFor(table, 0, 0).format(FileFormat.PUFFIN).build(),
                        ignored -> PositionDeleteIndex.empty());
                try (writer) {
                    for (int id : positionIds) {
                        writer.delete(imported.file().location(), imported.ordinals().indexOf(id),
                                table.spec(), imported.file().partition());
                        positions++;
                    }
                }
                assertThat(writer.result().deleteFiles()).hasSize(1);
                var delete = writer.result().deleteFiles().get(0);
                assertThat(delete.recordCount()).isEqualTo(positionIds.size());
                assertThat(delete.format()).isEqualTo(FileFormat.PUFFIN);
                delta.addDeletes(delete);
            }
            List<Integer> equalityIds = imported.ordinals().stream().filter(id -> id == 41 || id == 80).toList();
            if (!equalityIds.isEmpty()) {
                var schema = table.schema().select("ordinal");
                String location = table.locationProvider().newDataLocation("equalities-" + UUID.randomUUID() + ".parquet");
                EqualityDeleteWriter<Record> writer = Parquet.writeDeletes(table.io().newOutputFile(location))
                        .forTable(table).withPartition(imported.file().partition()).rowSchema(schema)
                        .equalityFieldIds(List.of(schema.findField("ordinal").fieldId()))
                        .createWriterFunc(GenericParquetWriter::create).buildEqualityWriter();
                try (writer) {
                    for (int id : equalityIds) {
                        var row = GenericRecord.create(schema);
                        row.setField("ordinal", id);
                        writer.write(row);
                        equalities++;
                    }
                }
                assertThat(writer.toDeleteFile().recordCount()).isEqualTo(equalityIds.size());
                delta.addDeletes(writer.toDeleteFile());
            }
        }
        assertThat(positions).isEqualTo(2);
        assertThat(equalities).isEqualTo(2);
        delta.commit();
        table.refresh();
        try (var tasks = table.newScan().planFiles()) {
            assertThat(tasks.iterator().hasNext()).isTrue();
        }
    }

    @Test
    void actualExporterValuesProjectionsAndFiltersMatchSourceOnStockWorkers() throws Exception {
        List<List<Object>> whole = new ArrayList<>();
        List<List<Object>> projected = new ArrayList<>();
        List<List<Object>> selected = new ArrayList<>();
        for (var row : source.entrySet()) {
            JsonNode value = row.getValue() == null ? null : JSON.readTree(row.getValue());
            var full = new ArrayList<Object>();
            full.add(row.getKey().toString()); full.add(row.getKey().toString()); full.add(value); whole.add(full);
            var paths = new ArrayList<Object>();
            paths.add(row.getKey().toString());
            paths.add(row.getKey().toString());
            for (String key : List.of("n", "Mixed.Key", "arr")) paths.add(value == null ? null : value.get(key));
            projected.add(paths);
            JsonNode n = value == null ? null : value.get("n");
            if (row.getKey() >= 10 && row.getKey() < 76 && n != null && n.isNumber() &&
                    n.decimalValue().compareTo(java.math.BigDecimal.valueOf(128)) == 0) selected.add(full);
        }
        assertThat(selected).isNotEmpty();
        Map<String, List<List<Object>>> queries = new LinkedHashMap<>();
        queries.put("SELECT ordinal, \"__fid__\", CAST(payload AS JSON) FROM %s ORDER BY ordinal", whole);
        // Nonobject roots deliberately occur in the source fixture; TRY gives them SQL-null projections.
        queries.put("SELECT ordinal, \"__fid__\", TRY(CAST(payload['n'] AS JSON)), TRY(CAST(payload['Mixed.Key'] AS JSON)), TRY(CAST(payload['arr'] AS JSON)) FROM %s ORDER BY ordinal", projected);
        queries.put("SELECT ordinal, \"__fid__\", CAST(payload AS JSON) FROM %s WHERE ordinal >= 10 AND ordinal < 76 AND geomesa_variant_equals(payload, ARRAY['n'], CAST(128 AS VARIANT)) ORDER BY ordinal", selected);
        queries.put("SELECT ordinal FROM %s WHERE geomesa_variant_equals(payload, ARRAY['n'], CAST('absent' AS VARIANT))", List.of());
        for (var entry : queries.entrySet()) {
            assertThat(query(entry.getKey().formatted(table(REFERENCE)), "off", false, "authorized")).isEqualTo(entry.getValue());
            for (String name : allExportedTables()) {
                for (String mode : List.of("off", "reference", "typed")) {
                    for (boolean projection : List.of(false, true)) {
                        assertThat(query(entry.getKey().formatted(table(name)), mode, projection, "authorized"))
                                .as("%s mode=%s projection=%s", name, mode, projection).isEqualTo(entry.getValue());
                    }
                }
            }
        }
    }

    @Test
    void visibilityStaysEnabledWithProtectedControlAndDatastoreProjection() throws Exception {
        for (String mode : List.of("off", "reference", "typed")) {
            for (String name : EXPORTED) {
                assertThat(query("SELECT count(*) FROM " + table(name), mode, true, "nobody")).isEqualTo(List.of(List.of("81")));
            }
            assertThat(query("SELECT count(*) FROM " + table(PROTECTED), mode, true, "authorized")).isEqualTo(List.of(List.of("81")));
            assertThat(query("SELECT count(*) FROM " + table(PROTECTED), mode, true, "nobody")).isEqualTo(List.of(List.of("0")));
        }
        var ds = DataStoreFinder.getDataStore(Map.<String, Object>of(
                TrinoDataStoreFactory.HOST.key, coordinator.getHost(), TrinoDataStoreFactory.PORT.key, coordinator.getFirstMappedPort(),
                TrinoDataStoreFactory.SCHEMA.key, SCHEMA, TrinoDataStoreFactory.USER.key, "authorized", "geomesa.security.auths", "U,S"));
        try {
            List<String> expected = new ArrayList<>();
            for (var row : source.entrySet()) {
                if (row.getValue() != null && JSON.readTree(row.getValue()).path("n").asText().equals("text")) expected.add(row.getKey().toString());
            }
            assertThat(expected).hasSize(4);
            var tables = new ArrayList<>(allExportedTables());
            tables.add(DELETED);
            tables.add(PROTECTED_DELETED);
            for (String name : tables) {
                List<String> matching = expected.stream().filter(id -> !hasDeletes(name) || !DELETED_IDS.contains(Integer.parseInt(id))).toList();
                for (boolean prefilter : List.of(false, true)) {
                    // Retain an ordinary pushdown when the optional VARIANT prefilter is disabled.
                    var query = new Query(name, ECQL.toFilter("ordinal >= 0 AND \"$.payload.n\" = 'text'"), new String[]{"ordinal"});
                    query.getHints().put(QueryHints.TRINO_VARIANT_PREFILTER(), prefilter);
                    query.setSortBy(org.geotools.api.filter.sort.SortBy.NATURAL_ORDER);
                    assertThat(ds.getFeatureSource(name).getCount(query)).isEqualTo(-1);
                    List<String> ids = new ArrayList<>();
                    try (var reader = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)) {
                        while (reader.hasNext()) {
                            var feature = reader.next();
                            assertThat(feature.getAttributeCount()).isEqualTo(1);
                            ids.add(feature.getID());
                        }
                    }
                    assertThat(ids).containsExactlyElementsOf(matching);
                    query.setStartIndex(1);
                    query.setMaxFeatures(1);
                    List<String> page = new ArrayList<>();
                    try (var reader = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)) {
                        while (reader.hasNext()) {
                            var feature = reader.next();
                            assertThat(feature.getAttributeCount()).isEqualTo(1);
                            page.add(feature.getID());
                        }
                    }
                    assertThat(page).containsExactlyElementsOf(matching.stream().skip(1).limit(1).toList());
                }
            }
        } finally { ds.dispose(); }
    }

    private static boolean hasDeletes(String name) { return name.equals(DELETED) || name.equals(PROTECTED_DELETED); }

    private static boolean visible(String visibility, Set<String> auths) {
        for (String alternative : visibility.split("\\|")) {
            boolean allowed = true;
            for (String label : alternative.split("&")) allowed &= auths.contains(label);
            if (allowed) return true;
        }
        return false;
    }

    private List<List<Object>> expectedRows(List<Integer> ids, boolean includePayload) throws Exception {
        List<List<Object>> rows = new ArrayList<>();
        for (int id : ids) {
            var row = new ArrayList<Object>();
            row.add(Integer.toString(id));
            if (includePayload) {
                row.add(Integer.toString(id));
                row.add(source.get(id) == null ? null : JSON.readTree(source.get(id)));
            }
            rows.add(row);
        }
        return rows;
    }

    @Test
    void mixedLayoutsAndBothDeleteKindsPreserveMetadataCountsAndPaging() throws Exception {
        for (String name : List.of(MIXED, DELETED, PROTECTED_DELETED)) {
            List<Integer> ids = source.keySet().stream().filter(id -> !hasDeletes(name) || !DELETED_IDS.contains(id)).toList();
            List<Integer> selected = new ArrayList<>();
            for (int id : ids) {
                if (source.get(id) != null && JSON.readTree(source.get(id)).path("n").asText().equals("text")) selected.add(id);
            }
            assertThat(ids).hasSize(hasDeletes(name) ? 77 : 81);
            assertThat(selected).hasSize(hasDeletes(name) ? 3 : 4);
            long nonNull = ids.stream().filter(id -> source.get(id) != null).count();
            Map<String, List<List<Object>>> checks = new LinkedHashMap<>();
            checks.put("SELECT ordinal, \"__fid__\", CAST(payload AS JSON) FROM %s ORDER BY ordinal", expectedRows(ids, true));
            checks.put("SELECT count(*), count(payload), count(DISTINCT ordinal), min(ordinal), max(ordinal) FROM %s",
                    List.of(List.of(Integer.toString(ids.size()), Long.toString(nonNull), Integer.toString(ids.size()),
                            ids.get(0).toString(), ids.get(ids.size() - 1).toString())));
            String predicate = " WHERE geomesa_variant_equals(payload, ARRAY['n'], CAST('text' AS VARIANT))";
            checks.put("SELECT count(*), count(payload), count(DISTINCT ordinal), min(ordinal), max(ordinal) FROM %s" + predicate,
                    List.of(List.of(Integer.toString(selected.size()), Integer.toString(selected.size()), Integer.toString(selected.size()),
                            selected.get(0).toString(), selected.get(selected.size() - 1).toString())));
            checks.put("SELECT DISTINCT ordinal FROM %s" + predicate + " ORDER BY ordinal", expectedRows(selected, false));
            checks.put("SELECT ordinal FROM %s" + predicate + " ORDER BY ordinal OFFSET 1 LIMIT 2",
                    expectedRows(selected.stream().skip(1).limit(2).toList(), false));
            for (String mode : List.of("off", "reference", "typed")) {
                for (boolean projection : List.of(false, true)) {
                    for (boolean metadata : List.of(false, true)) {
                        for (var check : checks.entrySet()) {
                            assertThat(query(check.getKey().formatted(table(name)), mode, projection, "authorized", metadata))
                                    .as("%s mode=%s projection=%s metadata=%s sql=%s", name, mode, projection, metadata, check.getKey())
                                    .isEqualTo(check.getValue());
                        }
                    }
                }
            }
        }
    }

    @Test
    void actualProtectedExporterFilesEnforceLabelsWithDeletesFiltersAndMetadata() throws Exception {
        Map<String, Set<String>> users = Map.of("authorized", Set.of("U", "S"), "u", Set.of("U"), "s", Set.of("S"), "nobody", Set.of());
        for (String name : List.of("protected_plain", "protected_sampled", PROTECTED_DELETED)) {
            for (var user : users.entrySet()) {
                List<Integer> ids = source.keySet().stream()
                        .filter(id -> !hasDeletes(name) || !DELETED_IDS.contains(id))
                        .filter(id -> visible(sourceVisibility.get(id), user.getValue())).toList();
                List<Integer> filtered = new ArrayList<>();
                for (int id : ids) {
                    if (source.get(id) != null && JSON.readTree(source.get(id)).path("n").asText().equals("text")) filtered.add(id);
                }
                for (String mode : List.of("off", "reference", "typed")) {
                    assertThat(query("SELECT ordinal, \"__fid__\", CAST(payload AS JSON) FROM " + table(name) + " ORDER BY ordinal",
                            mode, true, user.getKey(), true)).isEqualTo(expectedRows(ids, true));
                    assertThat(query("SELECT count(*) FROM " + table(name), mode, true, user.getKey(), true))
                            .isEqualTo(List.of(List.of(Integer.toString(ids.size()))));
                    String sql = "SELECT ordinal FROM " + table(name) +
                            " WHERE geomesa_variant_equals(payload, ARRAY['n'], CAST('text' AS VARIANT)) ORDER BY ordinal OFFSET 1 LIMIT 2";
                    assertThat(query(sql, mode, true, user.getKey(), true))
                            .isEqualTo(expectedRows(filtered.stream().skip(1).limit(2).toList(), false));
                }
            }
        }
    }

    private List<List<Object>> query(String sql, String mode, boolean projection, String user) throws Exception {
        return query(sql, mode, projection, user, false);
    }

    private List<List<Object>> query(String sql, String mode, boolean projection, String user, boolean metadata) throws Exception {
        var props = new Properties(); props.setProperty("user", user);
        List<List<Object>> rows = new ArrayList<>();
        try (var connection = DriverManager.getConnection("jdbc:trino://" + coordinator.getHost() + ":" + coordinator.getFirstMappedPort(), props);
                var statement = connection.createStatement()) {
            statement.execute("SET SESSION optimize_metadata_queries = " + metadata);
            statement.execute("SET SESSION " + CATALOG + ".variant_filter_mode = '" + mode + "'");
            statement.execute("SET SESSION " + CATALOG + ".variant_projection_pushdown_enabled = " + projection);
            try (var result = statement.executeQuery(sql)) {
                while (result.next()) {
                    List<Object> row = new ArrayList<>();
                    for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) {
                        String value = result.getString(i);
                        row.add(value != null && result.getMetaData().getColumnTypeName(i).equalsIgnoreCase("json") ? JSON.readTree(value) : value);
                    }
                    rows.add(row);
                }
            }
        }
        return rows;
    }

    private static String table(String name) { return CATALOG + "." + SCHEMA + "." + name; }

    @AfterAll
    void stop() {
        if (worker != null) worker.stop();
        if (coordinator != null) coordinator.stop();
        if (iceberg != null) iceberg.stop();
        if (s3 != null) s3.stop();
        if (network != null) network.close();
    }
}

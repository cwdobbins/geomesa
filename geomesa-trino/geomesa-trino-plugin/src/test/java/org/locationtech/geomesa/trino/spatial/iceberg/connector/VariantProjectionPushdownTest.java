/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import io.trino.parquet.VariantPath;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.spi.connector.*;
import io.trino.spi.expression.*;
import io.trino.spi.variant.Variant;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;
import org.locationtech.geomesa.trino.spatial.iceberg.GeoMesaColumnCatalog;
import org.locationtech.geomesa.trino.variant.VariantPredicateCompiler;
import org.locationtech.geomesa.trino.variant.VariantProjectionPushdown;
import org.locationtech.geomesa.trino.variant.VariantPushdownConfig;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VariantType.VARIANT;
import static org.assertj.core.api.Assertions.*;
import static org.locationtech.geomesa.trino.spatial.iceberg.connector.VariantTestFixtures.*;

class VariantProjectionPushdownTest {
    static ConnectorExpression subscript(ConnectorExpression document, String key) {
        return new Call(VARIANT, new FunctionName("$operator$subscript"), List.of(document, new Constant(utf8Slice(key), VARCHAR)));
    }
    static Variable document(String name) { return new Variable(name, VARIANT); }
    static Map<String, ColumnHandle> bindings(ProjectionApplicationResult<ConnectorTableHandle> result) {
        return result.getAssignments().stream().collect(Collectors.toMap(Assignment::getVariable, Assignment::getColumn));
    }
    private static IcebergColumnHandle projected(List<ConnectorExpression> expressions) {
        return (IcebergColumnHandle) bindings(VariantProjectionPushdown.apply(TABLE, expressions, Map.of("d", DOC)).orElseThrow()).get("d");
    }
    @Test void maximalPathsUnionAcrossAliasesWithoutChangingExpressions() {
        var expressions = List.of(subscript(subscript(document("d"), "user"), "country"), subscript(document("alias"), "status"));
        var result = VariantProjectionPushdown.apply(TABLE, expressions, Map.of("d", DOC, "alias", DOC)).orElseThrow();
        var expected = DOC.withVariantPaths(Set.of(VariantPath.keys("user", "country"), VariantPath.keys("status")));
        assertThat(result.getProjections()).containsExactlyElementsOf(expressions);
        assertThat(bindings(result)).containsEntry("d", expected).containsEntry("alias", expected);
        assertThat(((IcebergTableHandle) result.getHandle()).getProjectedColumns()).containsExactly(expected);
    }
    @Test void wholeUsesAbsorbPathDemand() {
        for (ConnectorExpression whole : List.of(document("d"),
                new Call(VARIANT, new FunctionName("unknown_function"), List.of(document("d"))),
                new Call(VARIANT, new FunctionName("$operator$subscript"), List.of(document("d"), new Variable("key", VARCHAR))))) {
            assertThat(VariantProjectionPushdown.apply(TABLE, List.of(subscript(document("d"), "a"), whole), Map.of("d", DOC))).isEmpty();
        }
    }
    @Test void unknownOuterOperationsKeepTheirWholeKnownSubdocument() {
        var inner = subscript(document("d"), "a");
        assertThat(projected(List.of(new Call(VARIANT, new FunctionName("unknown"), List.of(inner)), subscript(inner, "b")))
            .getVariantPaths()).contains(Set.of(VariantPath.keys("a")));
        assertThat(projected(List.of(new Call(VARIANT, new FunctionName("$operator$subscript"), List.of(inner, new Variable("key", VARCHAR)))))
            .getVariantPaths()).contains(Set.of(VariantPath.keys("a")));
    }
    @Test void exactAwkwardKeysAndForeignFunctions() {
        assertThat(projected(List.of(subscript(subscript(document("d"), ""), "a.b['é']"))).getVariantPaths())
            .contains(Set.of(VariantPath.keys("", "a.b['é']")));
        var foreign = new Call(VARIANT, new FunctionName(Optional.of(new CatalogSchemaName("foreign", "schema")), "$operator$subscript"),
            List.of(document("d"), new Constant(utf8Slice("a"), VARCHAR)));
        assertThat(VariantProjectionPushdown.apply(TABLE, List.of(foreign), Map.of("d", DOC))).isEmpty();
        var invalidUtf8 = new Call(VARIANT, new FunctionName("$operator$subscript"),
            List.of(document("d"), new Constant(wrappedBuffer(new byte[]{(byte) 0xff}), VARCHAR)));
        assertThat(VariantProjectionPushdown.apply(TABLE, List.of(invalidUtf8), Map.of("d", DOC))).isEmpty();
    }
    @Test void ownFunctionsSupplyKeyAndListElementDemands() {
        var get = new Call(VARIANT, new FunctionName("geomesa_variant_get"), List.of(document("d"), path("user", "country")));
        var any = new Call(BOOLEAN, new FunctionName("geomesa_variant_any_equals"),
            List.of(document("d"), path("items"), path("sku"), new Constant(Variant.ofString("x"), VARIANT)));
        assertThat(projected(List.of(get, any)).getVariantPaths()).contains(Set.of(VariantPath.keys("user", "country"),
            VariantPath.of(VariantPath.Step.key("items"), VariantPath.Step.element(), VariantPath.Step.key("sku"))));
        assertThat(VariantProjectionPushdown.apply(TABLE, List.of(new Call(VARIANT, new FunctionName("geomesa_variant_get"),
            List.of(document("d"), path((String) null)))), Map.of("d", DOC))).isEmpty();
    }
    @Test void repeatedPassesPreserveExistingPathAssignments() {
        var expressions = List.of(subscript(document("d"), "a"));
        var first = VariantProjectionPushdown.apply(TABLE, expressions, Map.of("d", DOC)).orElseThrow();
        assertThat(VariantProjectionPushdown.apply((IcebergTableHandle) first.getHandle(), expressions, bindings(first))).isEmpty();
        assertThat(VariantProjectionPushdown.apply((IcebergTableHandle) first.getHandle(), List.of(document("d")), bindings(first))).isEmpty();
        var b = DOC.withVariantPaths(Set.of(VariantPath.keys("b")));
        var mixed = VariantProjectionPushdown.apply(TABLE, List.of(subscript(document("d"), "a"), document("old")),
            Map.of("d", DOC, "old", b)).orElseThrow();
        assertThat(bindings(mixed).get("old")).isEqualTo(b);
        assertThat(((IcebergColumnHandle) bindings(mixed).get("d")).getVariantPaths()).contains(Set.of(VariantPath.keys("a"), VariantPath.keys("b")));
    }
    @Test void declinesOversizedCandidatesWithoutTruncatingPaths() {
        ConnectorExpression deep = document("d");
        for (int i = 0; i < 65; i++) deep = subscript(deep, "a");
        assertThat(VariantProjectionPushdown.apply(TABLE, List.of(deep), Map.of("d", DOC))).isEmpty();
        assertThat(VariantProjectionPushdown.apply(TABLE, java.util.Collections.nCopies(257, subscript(document("d"), "a")), Map.of("d", DOC))).isEmpty();
    }
    @Test void ordinaryScanAnalyzeMarkerAllowsProjectionButAnalyzeDoesNot() {
        var expressions = List.<ConnectorExpression>of(subscript(document("d"), "a"));
        Map<String, ColumnHandle> assignments = Map.of("d", DOC);
        var session = TestingConnectorSession.builder().setPropertyMetadata(VariantPushdownConfig.DEFAULT.sessionProperties())
            .setPropertyValues(Map.of(VariantPushdownConfig.PROJECTION_ENABLED, true)).build();
        var metadata = new SpatialConnectorMetadata(new ConnectorMetadata() {}, new GeoMesaColumnCatalog(),
            false, false, null, false, Set.of(), true);
        for (var marker : List.of(Optional.<Boolean>empty(), Optional.of(false))) {
            var input = table(java.util.OptionalLong.empty(), marker);
            for (var result : List.of(
                    VariantProjectionPushdown.apply(input, expressions, assignments).orElseThrow(),
                    metadata.applyProjection(session, input, expressions, assignments).orElseThrow())) {
                assertThat(((IcebergColumnHandle) bindings(result).get("d")).getVariantPaths())
                    .contains(Set.of(VariantPath.keys("a")));
                assertThat(((IcebergTableHandle) result.getHandle()).getForAnalyze()).isEqualTo(marker);
            }
        }
        assertThat(VariantProjectionPushdown.apply(TABLE.forAnalyze(), expressions, assignments)).isEmpty();
        assertThat(metadata.applyProjection(session, TABLE.forAnalyze(), expressions, assignments)).isEmpty();
    }
    @Test void wrappersDelegateChangesAndProjectionFlagArePreserved() {
        var metadata = new SpatialConnectorMetadata(new ConnectorMetadata() {
            @Override public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(ConnectorSession s,
                    ConnectorTableHandle h, List<ConnectorExpression> p, Map<String, ColumnHandle> a) {
                assertThat(h).isSameAs(TABLE);
                return Optional.of(new ProjectionApplicationResult<>(TABLE, p,
                    a.entrySet().stream().map(e -> new Assignment(e.getKey(), e.getValue(), VARIANT)).toList(), true));
            }
        }, new GeoMesaColumnCatalog(), false, false, null, false, Set.of(), true);
        var predicate = VariantPredicateCompiler.compile(equalsCall("d", Variant.ofLong(7), "a"), ASSIGNMENTS).orElseThrow();
        var handle = new VariantTableHandle(new VisibilityTableHandle(new SpatialTableHandle(TABLE, ID, List.of(), 0, 0, 1, 1), VIS, List.of("A")), List.of(predicate));
        var session = TestingConnectorSession.builder().setPropertyMetadata(VariantPushdownConfig.DEFAULT.sessionProperties())
            .setPropertyValues(Map.of(VariantPushdownConfig.PROJECTION_ENABLED, true)).build();
        var result = metadata.applyProjection(session, handle, List.of(subscript(document("d"), "a")), Map.of("d", DOC)).orElseThrow();
        assertThat(result.isPrecalculateStatistics()).isTrue();
        assertThat(((VariantTableHandle) result.getHandle()).predicates()).containsExactly(predicate);
        assertThat(((VisibilityTableHandle) ((VariantTableHandle) result.getHandle()).delegate()).auths()).containsExactly("A");
        assertThat(((IcebergTableHandle) SpatialTableHandle.unwrap(result.getHandle())).getProjectedColumns())
            .containsExactly(DOC.withVariantPaths(Set.of(VariantPath.keys("a"))));
        var off = metadata.applyProjection(session(VariantPushdownConfig.Mode.OFF), handle,
            List.of(subscript(document("d"), "a")), Map.of("d", DOC)).orElseThrow();
        assertThat(bindings(off).get("d")).isSameAs(DOC);
    }
    @Test void projectionConfigIsIndependentAndStrict() {
        var config = VariantPushdownConfig.from(Map.of("geomesa.variant.projection-pushdown-enabled", "true"));
        assertThat(config.projectionPushdownEnabled()).isTrue();
        assertThat(config.filterMode()).isEqualTo(VariantPushdownConfig.Mode.OFF);
        assertThat(VariantPushdownConfig.DEFAULT.projectionPushdownEnabled()).isFalse();
        assertThatThrownBy(() -> VariantPushdownConfig.from(Map.of("geomesa.variant.projection-pushdown-enabled", "yes")))
            .isInstanceOf(IllegalArgumentException.class);
    }
}

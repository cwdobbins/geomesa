/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import io.trino.spi.connector.*;
import io.trino.spi.expression.*;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.variant.Variant;
import org.junit.jupiter.api.Test;
import org.locationtech.geomesa.trino.spatial.iceberg.GeoMesaColumnCatalog;
import org.locationtech.geomesa.trino.variant.VariantPredicateCompiler;
import org.locationtech.geomesa.trino.variant.VariantPushdownConfig;
import org.locationtech.geomesa.trino.variant.VariantFilterPushdown;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.expression.StandardFunctions.*;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.*;
import static org.locationtech.geomesa.trino.spatial.iceberg.connector.VariantTestFixtures.*;
import static org.locationtech.geomesa.trino.variant.VariantPushdownConfig.Mode.*;

class VariantFilterPushdownTest {
    private static class Delegate implements ConnectorMetadata {
        @Override public SchemaTableName getTableName(ConnectorSession s, ConnectorTableHandle h) {
            assertThat(h).isInstanceOf(io.trino.plugin.iceberg.IcebergTableHandle.class);
            return new SchemaTableName("s", "docs");
        }
        @Override public Map<String, ColumnHandle> getColumnHandles(ConnectorSession s, ConnectorTableHandle h) {
            return Map.of("doc", DOC, "id", ID, "__vis__", VIS);
        }
        @Override public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(ConnectorSession s,
                ConnectorTableHandle h, List<ConnectorExpression> p, Map<String, ColumnHandle> a) {
            assertThat(h).isSameAs(TABLE);
            return Optional.of(new ProjectionApplicationResult<>(TABLE, p, List.of(), false));
        }
    }
    private static SpatialConnectorMetadata metadata(ConnectorMetadata delegate) {
        var catalog = new GeoMesaColumnCatalog();
        catalog.recordVisibilityColumn(new SchemaTableName("s", "docs"), Set.of("__vis__"));
        return new SpatialConnectorMetadata(delegate, catalog, false, false, null, false, Set.of(), true);
    }
    private static Constraint constraint(ConnectorExpression expression) {
        return new Constraint(TupleDomain.all(), expression, ASSIGNMENTS);
    }
    @Test void ordinaryScanAnalyzeMarkerAllowsClaimsButAnalyzeDoesNot() {
        var metadata = metadata(new Delegate());
        var call = equalsCall("d", Variant.ofLong(7), "a");
        for (var marker : List.of(Optional.<Boolean>empty(), Optional.of(false))) {
            var input = table(OptionalLong.empty(), marker);
            for (var mode : List.of(REFERENCE, TYPED)) {
                var result = metadata.applyFilter(session(mode), input, constraint(call)).orElseThrow();
                assertThat(result.getHandle()).isInstanceOf(VariantTableHandle.class);
                assertThat(((VariantTableHandle) result.getHandle()).delegate()).isSameAs(input);
                assertThat(((VariantTableHandle) result.getHandle()).predicates()).hasSize(1);
                assertThat(result.getRemainingExpression()).contains(Constant.TRUE);
            }
        }
        for (var mode : List.of(REFERENCE, TYPED)) {
            assertThat(metadata.applyFilter(session(mode), TABLE.forAnalyze(), constraint(call))).isEmpty();
        }
    }
    @Test void exactClaimsReachAFixedPointAcrossAliasesIncludingNan() {
        var metadata = metadata(new Delegate());
        var call = equalsCall("d", Variant.ofDouble(Double.NaN), "a");
        var result = metadata.applyFilter(session(REFERENCE), TABLE, constraint(call)).orElseThrow();
        assertThat(result.getRemainingExpression()).contains(Constant.TRUE);
        var handle = (VariantTableHandle) result.getHandle();
        assertThat(handle.predicates()).hasSize(1);
        assertThat(handle.predicates().getFirst().test(Variant.ofObject(Map.of(utf8Slice("a"), Variant.ofDouble(Double.NaN))))).isFalse();
        var alias = equalsCall("alias", Variant.ofDouble(Double.NaN), "a");
        var repeated = metadata.applyFilter(session(REFERENCE), handle, constraint(alias)).orElseThrow();
        assertThat(repeated.getHandle()).isEqualTo(handle);
        assertThat(metadata.applyFilter(session(REFERENCE), handle, constraint(Constant.TRUE))).isEmpty();
    }
    @Test void offAndPriorLimitDeclineNewClaimsButExistingClaimsSurviveOff() {
        var metadata = metadata(new Delegate());
        var call = equalsCall("d", Variant.ofLong(7), "a");
        assertThat(metadata.applyFilter(session(OFF), TABLE, constraint(call))).isEmpty();
        assertThat(metadata.applyFilter(session(REFERENCE), table(OptionalLong.of(1)), constraint(call))).isEmpty();
        var handle = metadata.applyFilter(session(REFERENCE), TABLE, constraint(call)).orElseThrow().getHandle();
        assertThat(metadata.applyFilter(session(OFF), handle, constraint(call)).orElseThrow().getHandle()).isEqualTo(handle);
    }
    @Test void incomingLimitCannotBeMadeEligibleByADelegateHandleChange() {
        var metadata = metadata(new Delegate() {
            @Override public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession s,
                    ConnectorTableHandle h, Constraint c) {
                return Optional.of(new ConstraintApplicationResult<>(TABLE, c.getSummary(), c.getExpression(), false));
            }
        });
        var call = equalsCall("d", Variant.ofLong(7), "a");
        var result = metadata.applyFilter(session(REFERENCE), table(OptionalLong.of(1)), constraint(call)).orElseThrow();
        assertThat(result.getHandle()).isSameAs(TABLE);
        assertThat(result.getRemainingExpression()).contains(call);
    }
    @Test void preservesTransparentRemainderAndDelegateDomainsWhenRemovingOpaqueCopy() {
        var residual = new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME,
            List.of(new Variable("v", VARCHAR), new Constant(utf8Slice("A"), VARCHAR)));
        var call = equalsCall("d", Variant.ofLong(7), "a");
        var opaque = new Call(BOOLEAN, new FunctionName("$engine_expression"), List.of(new Constant(utf8Slice("opaque"), VARCHAR)));
        var metadata = metadata(new Delegate() {
            @Override public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession s,
                    ConnectorTableHandle h, Constraint c) {
                return Optional.of(new ConstraintApplicationResult<>(h, TupleDomain.none(), c.getExpression(), true));
            }
        });
        var result = metadata.applyFilter(session(REFERENCE), TABLE,
            constraint(new Call(BOOLEAN, AND_FUNCTION_NAME, List.of(call, residual, opaque)))).orElseThrow();
        assertThat(result.getRemainingExpression()).contains(residual);
        assertThat(result.getRemainingFilter().isNone()).isTrue();
        assertThat(result.isPrecalculateStatistics()).isTrue();
    }
    @Test void rejectsOrNotForeignFunctionsInvalidPathsAndUnexpectedOpaqueShapes() {
        var call = equalsCall("d", Variant.ofLong(7), "a");
        var opaque = new Call(BOOLEAN, new FunctionName("$engine_expression"), List.of(new Constant(utf8Slice("opaque"), VARCHAR)));
        for (var expression : List.of(
                new Call(BOOLEAN, OR_FUNCTION_NAME, List.of(call, Constant.TRUE)),
                new Call(BOOLEAN, NOT_FUNCTION_NAME, List.of(call)),
                new Call(BOOLEAN, new FunctionName(Optional.of(new CatalogSchemaName("other", "schema")), "geomesa_variant_equals"), call.getArguments()),
                equalsCall("d", Variant.ofLong(7), (String) null),
                equalsCall("d", null, "a"),
                new Call(BOOLEAN, AND_FUNCTION_NAME, List.of(call, new Call(BOOLEAN, NOT_FUNCTION_NAME, List.of(opaque)))),
                new Call(BOOLEAN, AND_FUNCTION_NAME, List.of(call, opaque, opaque)))) {
            assertThat(metadata(new Delegate()).applyFilter(session(REFERENCE), TABLE, constraint(expression))).isEmpty();
        }
        assertThat(VariantPredicateCompiler.compile(equalsCall("id", Variant.ofLong(7), "a"), ASSIGNMENTS)).isEmpty();
    }
    @Test void plannerLimitsDeclineWholeCandidate() {
        assertThat(VariantPredicateCompiler.compile(equalsCall("d", Variant.ofLong(7),
            java.util.Collections.nCopies(65, "a").toArray(String[]::new)), ASSIGNMENTS)).isEmpty();
        var many = new Call(BOOLEAN, AND_FUNCTION_NAME, java.util.Collections.nCopies(257, equalsCall("d", Variant.ofLong(7), "a")));
        assertThat(VariantFilterPushdown.candidates(many, ASSIGNMENTS)).isEmpty();
    }
    @Test void canonicalCompositionSurvivesProjectionAndRejectsScanReplacingPushdowns() {
        var metadata = metadata(new Delegate());
        var spatial = new SpatialTableHandle(TABLE, ID, List.of(), 0, 0, 1, 1);
        var visible = new Call(BOOLEAN, new FunctionName("is_visible"),
            List.of(new Variable("v", VARCHAR), new Constant(utf8Slice("A"), VARCHAR)));
        var result = metadata.applyFilter(session(REFERENCE), spatial, constraint(new Call(BOOLEAN, AND_FUNCTION_NAME,
            List.of(visible, equalsCall("d", Variant.ofLong(7), "a"))))).orElseThrow();
        var variant = (VariantTableHandle) result.getHandle();
        assertThat(((VisibilityTableHandle) variant.delegate()).delegate()).isEqualTo(spatial);
        assertThat(SpatialTableHandle.unwrap(variant)).isSameAs(TABLE);
        assertThat(metadata.applyProjection(session(REFERENCE), variant, List.of(), Map.of()).orElseThrow().getHandle()).isEqualTo(variant);
        assertThat(metadata.applyLimit(null, variant, 1)).isEmpty();
        assertThat(metadata.applyAggregation(null, variant, List.of(), Map.of(), List.of())).isEmpty();
        assertThat(metadata.applyTopN(null, variant, 1, List.of(), Map.of())).isEmpty();
    }
    @Test void configRejectsUnknownAndInvalidValuesAndDefaultsOff() {
        assertThat(VariantPushdownConfig.from(Map.of())).isEqualTo(VariantPushdownConfig.DEFAULT);
        assertThat(VariantPushdownConfig.from(Map.of("geomesa.variant.filter-mode", "reference")).filterMode()).isEqualTo(REFERENCE);
        assertThatThrownBy(() -> VariantPushdownConfig.from(Map.of("geomesa.variant.filter-mode", "typo"))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> VariantPushdownConfig.from(Map.of("geomesa.variant.typo", "true"))).isInstanceOf(IllegalArgumentException.class);
    }

    private static ConnectorSession nativeSession() {
        return io.trino.testing.TestingConnectorSession.builder().setPropertyMetadata(VariantPushdownConfig.DEFAULT.sessionProperties())
            .setPropertyValues(Map.of(VariantPushdownConfig.FILTER_MODE, "typed", VariantPushdownConfig.NATIVE_ENABLED, true)).build();
    }
    private static Call nativeEquals(Variant value) {
        var access = new Call(io.trino.spi.type.VariantType.VARIANT, new FunctionName("$operator$subscript"),
            List.of(new Variable("d", io.trino.spi.type.VariantType.VARIANT), new Constant(utf8Slice("a"), VARCHAR)));
        return new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, List.of(access, new Constant(value, io.trino.spi.type.VariantType.VARIANT)));
    }
    @Test void nativeOnlyPreservesEntireResidualAndReachesFixedPoint() {
        var metadata = metadata(new Delegate());
        var nativeCall = nativeEquals(Variant.ofDouble(Double.NaN));
        var opaque = new Call(BOOLEAN, new FunctionName("$engine_expression"), List.of(new Constant(utf8Slice("opaque"), VARCHAR)));
        var expression = new Call(BOOLEAN, AND_FUNCTION_NAME, List.of(nativeCall, opaque));
        var result = metadata.applyFilter(nativeSession(), TABLE, constraint(expression)).orElseThrow();
        assertThat(result.getRemainingExpression()).containsSame(expression);
        var handle = (VariantTableHandle) result.getHandle();
        assertThat(handle.predicates()).isEmpty();
        assertThat(handle.hints()).hasSize(1);
        assertThat(metadata.applyFilter(nativeSession(), handle, constraint(expression))).isEmpty();
        assertThat(metadata.applyProjection(nativeSession(), handle, List.of(), Map.of()).orElseThrow().getHandle()).isEqualTo(handle);
        assertThat(metadata.applyLimit(nativeSession(), handle, 1)).isEmpty();
    }
    @Test void nativeRequiresOptInAndEligibleScanAndMixedClaimsKeepNativeResidual() {
        var metadata = metadata(new Delegate());
        var nativeCall = nativeEquals(Variant.ofLong(7));
        for (var mode : List.of(OFF, REFERENCE, TYPED)) assertThat(metadata.applyFilter(session(mode), TABLE, constraint(nativeCall))).isEmpty();
        assertThat(metadata.applyFilter(nativeSession(), table(OptionalLong.of(1)), constraint(nativeCall))).isEmpty();
        var exact = equalsCall("d", Variant.ofLong(7), "a");
        var opaque = new Call(BOOLEAN, new FunctionName("$engine_expression"), List.of(new Constant(utf8Slice("opaque"), VARCHAR)));
        var result = metadata.applyFilter(nativeSession(), TABLE, constraint(new Call(BOOLEAN, AND_FUNCTION_NAME, List.of(exact, nativeCall, opaque)))).orElseThrow();
        assertThat(result.getRemainingExpression()).contains(nativeCall);
        var handle = (VariantTableHandle) result.getHandle();
        assertThat(handle.predicates()).hasSize(1);
        assertThat(handle.hints()).hasSize(1);
    }
    @Test void integerCandidatesCompileAndClaimOnlyWhenEnabled() {
        var template = equalsCall("d", Variant.ofInt(100000), "outer", "score");
        for (String function : List.of("geomesa_variant_may_gt", "geomesa_variant_may_gte",
                "geomesa_variant_may_lt", "geomesa_variant_may_lte")) {
            var call = new Call(BOOLEAN, new FunctionName(function), template.getArguments());
            var predicate = VariantPredicateCompiler.compile(call, ASSIGNMENTS).orElseThrow();
            assertThat(predicate.kind().conservative()).isTrue();
            assertThat(predicate.kind().array()).isFalse();
            var metadata = metadata(new Delegate());
            assertThat(metadata.applyFilter(session(OFF), TABLE, constraint(call))).isEmpty();
            for (var mode : List.of(REFERENCE, TYPED)) {
                var result = metadata.applyFilter(session(mode), TABLE, constraint(call)).orElseThrow();
                assertThat(result.getRemainingExpression()).contains(Constant.TRUE);
                assertThat(((VariantTableHandle) result.getHandle()).predicates()).containsExactly(predicate);
                assertThat(metadata.applyFilter(session(mode), result.getHandle(), constraint(call)).orElseThrow().getHandle()).isEqualTo(result.getHandle());
                assertThat(metadata.applyFilter(session(mode), result.getHandle(), constraint(Constant.TRUE))).isEmpty();
            }
            assertThat(VariantPredicateCompiler.compile(new Call(BOOLEAN, new FunctionName(function),
                    template.getArguments().subList(0, 2)), ASSIGNMENTS)).isEmpty();
        }
    }

    @Test void candidateFunctionsAndContainmentCompileWithoutChangingTheirMeaning() {
        var template = equalsCall("d", Variant.ofString("001"), "a");
        for (String function : List.of("geomesa_variant_may_equal", "geomesa_variant_any_may_equal", "geomesa_variant_contains")) {
            var args = new java.util.ArrayList<>(template.getArguments());
            if (function.contains("any_")) args.add(2, path("name"));
            var call = new Call(BOOLEAN, new FunctionName(function), args);
            var predicate = VariantPredicateCompiler.compile(call, ASSIGNMENTS).orElseThrow();
            assertThat(predicate.kind().array()).isEqualTo(!function.equals("geomesa_variant_may_equal"));
            assertThat(predicate.kind().conservative()).isEqualTo(!function.endsWith("contains"));
            var result = metadata(new Delegate()).applyFilter(session(TYPED), TABLE, constraint(call)).orElseThrow();
            assertThat(result.getRemainingExpression()).contains(Constant.TRUE);
            assertThat(((VariantTableHandle) result.getHandle()).predicates()).containsExactly(predicate);
            assertThat(VariantPredicateCompiler.compile(new Call(BOOLEAN,
                new FunctionName(Optional.of(new CatalogSchemaName("other", "schema")), function), args), ASSIGNMENTS)).isEmpty();
        }
        assertThat(VariantPredicateCompiler.compile(new Call(BOOLEAN, new FunctionName("geomesa_variant_fields_may_equal"),
            List.of(template.getArguments().getFirst(), path("bag"), path("name"), template.getArguments().getLast())), ASSIGNMENTS)).isEmpty();
    }

}

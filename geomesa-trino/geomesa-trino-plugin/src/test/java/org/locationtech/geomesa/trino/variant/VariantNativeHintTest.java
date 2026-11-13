/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.variant;

import io.trino.parquet.VariantPath;
import io.trino.parquet.reader.VariantBatchAccess;
import io.trino.parquet.reader.VariantPathProbe;
import io.trino.parquet.reader.VariantPredicateEvaluator.Decision;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.*;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.spi.variant.Variant;
import org.junit.jupiter.api.Test;
import java.util.*;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.iceberg.ColumnIdentity.primitiveColumnIdentity;
import static io.trino.spi.expression.StandardFunctions.*;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VariantType.VARIANT;
import static org.assertj.core.api.Assertions.*;

class VariantNativeHintTest {
    private static final IcebergColumnHandle DOC = IcebergColumnHandle.optional(primitiveColumnIdentity(1, "doc")).columnType(VARIANT).build();
    private static final Map<String, ColumnHandle> ASSIGNMENTS = Map.of("d", DOC);
    private static final ConnectorExpression ACCESS = new Call(VARIANT, new FunctionName("$operator$subscript"),
        List.of(new Variable("d", VARIANT), new Constant(utf8Slice("a"), VARCHAR)));
    private static Constant value(Variant v) { return new Constant(v, VARIANT); }
    private static Call eq(ConnectorExpression a, ConnectorExpression b) { return new Call(BOOLEAN, EQUAL_OPERATOR_FUNCTION_NAME, List.of(a, b)); }
    private static VariantNativeHint hint(VariantNativeHint.Kind kind, Variant... values) {
        return new VariantNativeHint(kind, DOC, List.of("a"), Arrays.stream(values).map(VariantNativeHintTest::value).toList());
    }
    private static VariantPathProbe typed(Type type, Object value) {
        return new VariantPathProbe(() -> {}, VariantPathProbe.State.TYPED_VALUE, "doc", type, writeNativeValue(type, value), 0);
    }
    private static VariantPathProbe state(VariantPathProbe.State state) {
        return new VariantPathProbe(() -> {}, state, "doc", null, null, 0);
    }
    private static VariantBatchAccess batch(VariantPathProbe probe) {
        return new VariantBatchAccess() {
            @Override public List<VariantPathProbe> probe(VariantPath path, int position) { return List.of(probe); }
            @Override public Optional<Variant> reference(int position) { throw new AssertionError("Native hints must not reconstruct a document"); }
        };
    }
    @Test void equalityAndInUseSpiEqualityIncludingLargeIntegersAndNullMembers() {
        for (var probe : List.of(typed(BIGINT, (1L << 53) + 1), typed(BIGINT, Long.MAX_VALUE), typed(BOOLEAN, true), typed(VARCHAR, utf8Slice("a.é漢字")))) {
            for (Variant expected : List.of(probe.value(), Variant.ofDouble((double) ((1L << 53) + 1)), Variant.ofLong(7), Variant.NULL_VALUE)) {
                assertThat(new VariantNativeHintEvaluator(List.of(hint(VariantNativeHint.Kind.EQUALS, expected))).evaluate(batch(probe), 0))
                    .isEqualTo(probe.value().equals(expected) ? Decision.TRUE : Decision.FALSE);
                assertThat(new VariantNativeHintEvaluator(List.of(hint(VariantNativeHint.Kind.IN, expected, null))).evaluate(batch(probe), 0))
                    .isEqualTo(probe.value().equals(expected) ? Decision.TRUE : Decision.SQL_NULL);
            }
        }
    }
    @Test void nullAbsenceAndUnresolvedRemainDistinct() {
        for (var state : List.of(VariantPathProbe.State.SQL_NULL, VariantPathProbe.State.MISSING)) {
            assertThat(new VariantNativeHintEvaluator(List.of(hint(VariantNativeHint.Kind.EQUALS, Variant.NULL_VALUE))).evaluate(batch(state(state)), 0)).isEqualTo(Decision.SQL_NULL);
            assertThat(new VariantNativeHintEvaluator(List.of(hint(VariantNativeHint.Kind.IS_NULL))).evaluate(batch(state(state)), 0)).isEqualTo(Decision.TRUE);
            assertThat(new VariantNativeHintEvaluator(List.of(hint(VariantNativeHint.Kind.IS_NOT_NULL))).evaluate(batch(state(state)), 0)).isEqualTo(Decision.FALSE);
        }
        for (var probe : List.of(state(VariantPathProbe.State.UNRESOLVED), state(VariantPathProbe.State.WRONG_SHAPE), typed(DOUBLE, Double.NaN))) {
            var evaluator = new VariantNativeHintEvaluator(List.of(hint(VariantNativeHint.Kind.EQUALS, Variant.ofLong(7))));
            assertThat(evaluator.evaluate(batch(probe), 0)).isEqualTo(Decision.UNKNOWN);
            assertThat(evaluator.evaluateReference(Variant.ofLong(8))).isEqualTo(Decision.UNKNOWN);
            assertThat(evaluator.unknownRows()).isEqualTo(2);
            assertThat(evaluator.fallbackRows()).isZero();
            assertThat(evaluator.exact()).isFalse();
        }
        assertThat(new VariantNativeHintEvaluator(List.of(hint(VariantNativeHint.Kind.IS_NULL))).evaluate(batch(typed(DOUBLE, Double.NaN)), 0)).isEqualTo(Decision.FALSE);
    }
    @Test void compilerAdmitsOnlyMandatoryNativeConstantKeyPredicates() {
        Call equality = eq(ACCESS, value(Variant.ofLong(7)));
        var expected = hint(VariantNativeHint.Kind.EQUALS, Variant.ofLong(7));
        assertThat(VariantNativeHintCompiler.compile(equality, ASSIGNMENTS)).contains(expected);
        assertThat(VariantNativeHintCompiler.compile(eq(value(Variant.ofLong(7)), ACCESS), ASSIGNMENTS)).contains(expected);
        var isNull = new Call(BOOLEAN, IS_NULL_FUNCTION_NAME, List.of(ACCESS));
        assertThat(VariantNativeHintCompiler.compile(isNull, ASSIGNMENTS)).contains(hint(VariantNativeHint.Kind.IS_NULL));
        assertThat(VariantNativeHintCompiler.compile(new Call(BOOLEAN, NOT_FUNCTION_NAME, List.of(isNull)), ASSIGNMENTS)).contains(hint(VariantNativeHint.Kind.IS_NOT_NULL));
        for (var expression : List.of(new Call(BOOLEAN, OR_FUNCTION_NAME, List.of(equality, Constant.TRUE)),
                new Call(BOOLEAN, NOT_FUNCTION_NAME, List.of(equality)),
                eq(ACCESS, new Variable("d", VARIANT)), eq(ACCESS, new Constant(7L, BIGINT)),
                eq(new Call(VARIANT, new FunctionName("try"), List.of(ACCESS)), value(Variant.ofLong(7))))) {
            assertThat(VariantNativeHintCompiler.candidates(expression, ASSIGNMENTS)).isEmpty();
        }
        var opaque = new Call(BOOLEAN, new FunctionName("$engine_expression"), List.of(new Constant(utf8Slice("opaque"), VARCHAR)));
        assertThat(VariantNativeHintCompiler.candidates(new Call(BOOLEAN, AND_FUNCTION_NAME, List.of(equality, opaque)), ASSIGNMENTS)).containsExactly(expected);
        assertThat(VariantNativeHintCompiler.candidates(new Call(BOOLEAN, AND_FUNCTION_NAME, List.of(equality, opaque, opaque)), ASSIGNMENTS)).isEmpty();
    }
    @Test void inSupportsBothTranslatedArrayFormsAndDeclinesOversizedInputs() {
        var array = new ArrayType(VARIANT);
        var builder = VARIANT.createBlockBuilder(null, 2);
        VARIANT.writeObject(builder, Variant.ofLong(7)); builder.appendNull();
        for (ConnectorExpression constants : List.of(new Constant(builder.build(), array),
                new Call(array, ARRAY_CONSTRUCTOR_FUNCTION_NAME, List.of(value(Variant.ofLong(7)), value(null))))) {
            assertThat(VariantNativeHintCompiler.compile(new Call(BOOLEAN, IN_PREDICATE_FUNCTION_NAME, List.of(ACCESS, constants)), ASSIGNMENTS))
                .contains(hint(VariantNativeHint.Kind.IN, Variant.ofLong(7), null));
        }
        var oversized = new Call(array, ARRAY_CONSTRUCTOR_FUNCTION_NAME, Collections.nCopies(257, value(Variant.ofLong(7))));
        assertThat(VariantNativeHintCompiler.compile(new Call(BOOLEAN, IN_PREDICATE_FUNCTION_NAME, List.of(ACCESS, oversized)), ASSIGNMENTS)).isEmpty();
        ConnectorExpression deep = ACCESS;
        for (int i = 0; i < 64; i++) deep = new Call(VARIANT, new FunctionName("$operator$subscript"), List.of(deep, new Constant(utf8Slice("a"), VARCHAR)));
        assertThat(VariantNativeHintCompiler.compile(eq(deep, value(Variant.ofLong(7))), ASSIGNMENTS)).isEmpty();
    }
    @Test void nanIdentityIsStableAndConfigRequiresBothTypedAndNative() {
        assertThat(hint(VariantNativeHint.Kind.EQUALS, Variant.ofDouble(Double.NaN)))
            .isEqualTo(hint(VariantNativeHint.Kind.EQUALS, Variant.ofDouble(Double.NaN)))
            .hasSameHashCodeAs(hint(VariantNativeHint.Kind.EQUALS, Variant.ofDouble(Double.NaN)));
        for (var mode : VariantPushdownConfig.Mode.values()) for (boolean enabled : List.of(false, true)) {
            var session = io.trino.testing.TestingConnectorSession.builder().setPropertyMetadata(VariantPushdownConfig.DEFAULT.sessionProperties())
                .setPropertyValues(Map.of(VariantPushdownConfig.FILTER_MODE, mode.name(), VariantPushdownConfig.NATIVE_ENABLED, enabled)).build();
            assertThat(VariantPushdownConfig.nativeEnabled(session)).isEqualTo(mode == VariantPushdownConfig.Mode.TYPED && enabled);
        }
        assertThatThrownBy(() -> VariantPushdownConfig.from(Map.of("geomesa.variant.native-prefilter-enabled", "typo"))).isInstanceOf(IllegalArgumentException.class);
    }
}

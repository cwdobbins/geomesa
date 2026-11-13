/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.variant;

import io.airlift.slice.Slice;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.variant.Variant;
import org.junit.jupiter.api.Test;
import org.locationtech.geomesa.trino.spatial.SpatialIcebergPlugin;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

class GeoMesaVariantFunctionsTest {

    @Test
    void registersNullableArgumentsAndTotalBooleanResults() {
        assertThat(new SpatialIcebergPlugin().getFunctions()).contains(GeoMesaVariantFunctions.class);
        var functions = InternalFunctionBundle.builder().functions(GeoMesaVariantFunctions.class).build().getFunctions();
        assertThat(functions).hasSize(12);
        for (var function : functions) {
            assertThat(function.getFunctionNullability().getArgumentNullable()).allMatch(Boolean::booleanValue);
            assertThat(function.getFunctionNullability().isReturnNullable())
                .isEqualTo(function.getCanonicalName().equals("geomesa_variant_get"));
        }
    }

    @FunctionalInterface
    private interface RangeFunction { boolean test(Variant document, Block path, Variant expected); }

    @Test
    void integerRangeFunctionsKeepNullInvalidAndUnsupportedInputs() {
        List<RangeFunction> functions = List.of(GeoMesaVariantFunctions::mayGreaterThan, GeoMesaVariantFunctions::mayGreaterThanOrEqual,
                GeoMesaVariantFunctions::mayLessThan, GeoMesaVariantFunctions::mayLessThanOrEqual);
        for (var function : functions) {
            var document = object("a", Variant.ofInt(10));
            assertThat(function.test(null, path("a"), Variant.ofInt(10))).isTrue();
            assertThat(function.test(document, null, Variant.ofInt(10))).isTrue();
            assertThat(function.test(document, path("a", null), Variant.ofInt(10))).isTrue();
            assertThat(function.test(document, path("a"), null)).isTrue();
            for (var expected : List.of(Variant.NULL_VALUE, Variant.ofDouble(Double.NaN), Variant.ofString("10"))) {
                assertThat(function.test(document, path("a"), expected)).isTrue();
            }
        }
        var value = Variant.ofInt(10);
        assertThat(GeoMesaVariantFunctions.mayGreaterThan(value, path(), value)).isFalse();
        assertThat(GeoMesaVariantFunctions.mayGreaterThanOrEqual(value, path(), value)).isTrue();
        assertThat(GeoMesaVariantFunctions.mayLessThan(value, path(), value)).isFalse();
        assertThat(GeoMesaVariantFunctions.mayLessThanOrEqual(value, path(), value)).isTrue();
    }

    @Test
    void missingAndNullRemainDistinct() {
        Variant document = object("null", Variant.NULL_VALUE, "value", Variant.ofLong(7));
        assertThat(GeoMesaVariantFunctions.get(document, path("missing"))).isNull();
        assertThat(GeoMesaVariantFunctions.get(document, path("null"))).isEqualTo(Variant.NULL_VALUE);
        assertThat(GeoMesaVariantFunctions.equals(document, path("missing"), Variant.NULL_VALUE)).isFalse();
        assertThat(GeoMesaVariantFunctions.equals(document, path("null"), Variant.NULL_VALUE)).isTrue();
        assertThat(GeoMesaVariantFunctions.equals(document, path("value"), null)).isFalse();
    }

    @Test
    void nullArgumentsAndInvalidPathsAreTotal() {
        Variant document = object("a", Variant.ofLong(7));
        assertThat(GeoMesaVariantFunctions.get(null, path())).isNull();
        assertThat(GeoMesaVariantFunctions.get(document, null)).isNull();
        assertThat(GeoMesaVariantFunctions.get(document, path("a", null))).isNull();
        assertThat(GeoMesaVariantFunctions.equals(null, path(), Variant.NULL_VALUE)).isFalse();
        assertThat(GeoMesaVariantFunctions.equals(document, null, document)).isFalse();
        assertThat(GeoMesaVariantFunctions.equals(document, path((String) null), document)).isFalse();
        assertThat(GeoMesaVariantFunctions.anyEquals(document, null, path(), document)).isFalse();
        assertThat(GeoMesaVariantFunctions.anyEquals(Variant.ofArray(List.of(document)), path(), null, document)).isFalse();
        assertThat(GeoMesaVariantFunctions.anyEquals(Variant.ofArray(List.of(document)), path(), path((String) null), document)).isFalse();
    }

    @Test
    void emptyPathSelectsTheRootWithoutChangingItsKind() {
        for (Variant value : List.of(Variant.NULL_VALUE, Variant.ofLong(7), Variant.ofString("x"),
                object("a", Variant.ofLong(1)), Variant.ofArray(List.of(Variant.ofLong(2))))) {
            assertThat(GeoMesaVariantFunctions.get(value, path())).isSameAs(value);
            assertThat(GeoMesaVariantFunctions.equals(value, path(), value)).isTrue();
        }
    }

    @Test
    void keysAreExactAndNotPathSyntax() {
        for (String key : List.of("", "a.b", "it's", "[*]", "value", "typed_value", "metadata", "é漢字", "a\u0000b")) {
            Variant document = object(key, Variant.ofLong(7));
            assertThat(GeoMesaVariantFunctions.equals(document, path(key), Variant.ofLong(7))).as(key).isTrue();
        }
        assertThat(GeoMesaVariantFunctions.get(object("A", Variant.ofLong(7)), path("a"))).isNull();
        assertThat(GeoMesaVariantFunctions.get(object("a.b", Variant.ofLong(7)), path("a", "b"))).isNull();
    }

    @Test
    void wrongShapesAtEveryDepthGiveMissing() {
        for (Variant wrong : List.of(Variant.ofLong(7), Variant.NULL_VALUE, Variant.ofArray(List.of()), Variant.ofString("x"))) {
            assertThat(GeoMesaVariantFunctions.get(wrong, path("a"))).isNull();
            assertThat(GeoMesaVariantFunctions.get(object("a", wrong), path("a", "b"))).isNull();
            assertThat(GeoMesaVariantFunctions.equals(object("a", wrong), path("a", "b"), Variant.NULL_VALUE)).isFalse();
        }
        assertThat(GeoMesaVariantFunctions.get(object("a", object("b", Variant.ofLong(7))), path("a", "b")))
            .isEqualTo(Variant.ofLong(7));
    }

    @Test
    void equalityUsesTrinoNumericSemantics() {
        Variant one = object("n", Variant.ofByte((byte) 1));
        for (Variant expected : List.of(Variant.ofShort((short) 1), Variant.ofInt(1), Variant.ofLong(1),
                Variant.ofFloat(1), Variant.ofDouble(1), Variant.ofDecimal(new BigDecimal("1.00")))) {
            assertThat(GeoMesaVariantFunctions.equals(one, path("n"), expected)).isTrue();
        }
        Variant decimal = Variant.ofDecimal(new BigDecimal("1.00"));
        Variant extracted = GeoMesaVariantFunctions.get(object("n", decimal), path("n"));
        assertThat(extracted.primitiveType()).isEqualTo(decimal.primitiveType());
        assertThat(extracted.getDecimal()).isEqualTo(new BigDecimal("1.00"));
        assertThat(GeoMesaVariantFunctions.equals(object("n", Variant.ofLong(9007199254740993L)), path("n"), Variant.ofDouble(9007199254740992d))).isFalse();
        assertThat(GeoMesaVariantFunctions.equals(one, path("n"), Variant.ofString("1"))).isFalse();
    }

    @Test
    void otherScalarKindsUseSpiEqualityWithoutCoercion() {
        for (Variant value : List.of(Variant.ofDouble(Double.POSITIVE_INFINITY),
                Variant.ofDouble(-0.0), Variant.ofDate(42), Variant.ofTimeMicrosNtz(123),
                Variant.ofTimestampMicrosUtc(456), Variant.ofTimestampNanosNtz(789),
                Variant.ofBinary(utf8Slice("bytes")), Variant.ofUuid(io.trino.spi.type.UuidType.javaUuidToTrinoUuid(java.util.UUID.fromString("01234567-89ab-cdef-0123-456789abcdef"))))) {
            assertThat(GeoMesaVariantFunctions.equals(object("v", value), path("v"), value)).isTrue();
            assertThat(GeoMesaVariantFunctions.get(object("v", value), path("v")).primitiveType()).isEqualTo(value.primitiveType());
            assertThat(GeoMesaVariantFunctions.equals(object("v", value), path("v"), Variant.ofString("different"))).isFalse();
        }
    }

    @Test
    void nanIsNotEqualAndSignedZerosAreEqual() {
        assertThat(GeoMesaVariantFunctions.equals(object("v", Variant.ofDouble(Double.NaN)), path("v"), Variant.ofDouble(Double.NaN))).isFalse();
        assertThat(GeoMesaVariantFunctions.equals(object("v", Variant.ofDouble(-0.0)), path("v"), Variant.ofDouble(0.0))).isTrue();
        assertThat(GeoMesaVariantFunctions.equals(object("v", Variant.ofBinary(utf8Slice("x"))), path("v"), Variant.ofString("x"))).isFalse();
    }

    @Test
    void arraysKeepElementScopeAndVariantNull() {
        Variant document = object("items", Variant.ofArray(List.of(
            object("sku", Variant.ofString("a"), "color", Variant.ofString("red")),
            object("sku", Variant.ofString("b"), "color", Variant.ofString("blue")),
            Variant.NULL_VALUE, Variant.ofLong(9))));
        assertThat(GeoMesaVariantFunctions.anyEquals(document, path("items"), path("sku"), Variant.ofString("a"))).isTrue();
        assertThat(GeoMesaVariantFunctions.anyEquals(document, path("items"), path("color"), Variant.ofString("blue"))).isTrue();
        // These two true calls match different elements; combining them does not mean one element matches both.
        assertThat(GeoMesaVariantFunctions.anyEquals(document, path("items"), path("sku"), Variant.ofString("missing"))).isFalse();
        assertThat(GeoMesaVariantFunctions.anyEquals(document, path("items"), path(), Variant.NULL_VALUE)).isTrue();
        assertThat(GeoMesaVariantFunctions.anyEquals(document, path("items"), path("missing"), Variant.NULL_VALUE)).isFalse();
        assertThat(GeoMesaVariantFunctions.anyEquals(document, path("items"), path(), Variant.ofLong(9))).isTrue();
    }

    @Test
    void absentEmptyAndNonArrayListsAreFalse() {
        for (Variant document : List.of(object("items", Variant.ofArray(List.of())), object("items", Variant.NULL_VALUE),
                object("items", Variant.ofLong(7)), object("other", Variant.ofLong(7)))) {
            assertThat(GeoMesaVariantFunctions.anyEquals(document, path("items"), path(), Variant.NULL_VALUE)).isFalse();
        }
        assertThat(GeoMesaVariantFunctions.anyEquals(null, path(), path(), Variant.NULL_VALUE)).isFalse();
        assertThat(GeoMesaVariantFunctions.anyEquals(Variant.ofArray(List.of(Variant.NULL_VALUE)), path(), path(), null)).isFalse();
    }

    @Test
    void compiledPathsOwnTheirInput() {
        Slice mutable = utf8Slice("a");
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, 1);
        VARCHAR.writeSlice(builder, mutable);
        var compiled = VariantReferenceEvaluator.compilePath(builder.build());
        mutable.setByte(0, 'b');
        assertThat(VariantReferenceEvaluator.get(object("a", Variant.ofLong(7)), compiled)).isEqualTo(Variant.ofLong(7));
        assertThat(VariantReferenceEvaluator.compilePath(Arrays.asList("a", null))).isNull();
    }


    @Test
    void containmentAndFieldWildcardHaveExactTotalSemantics() {
        Variant items = Variant.ofArray(List.of(Variant.ofString("x"), Variant.NULL_VALUE, Variant.ofLong(1)));
        assertThat(GeoMesaVariantFunctions.contains(object("items", items), path("items"), Variant.NULL_VALUE)).isTrue();
        assertThat(GeoMesaVariantFunctions.contains(items, path(), Variant.ofString("1"))).isFalse();
        assertThat(GeoMesaVariantFunctions.contains(null, path(), Variant.NULL_VALUE)).isFalse();
        Variant fields = object("a", object("v", Variant.ofString("x")), "b", object("v", Variant.ofLong(1)));
        assertThat(GeoMesaVariantFunctions.fieldsEqual(fields, path(), path("v"), Variant.ofString("x"))).isTrue();
        assertThat(GeoMesaVariantFunctions.fieldsEqual(fields, path(), path("v"), Variant.ofString("1"))).isFalse();
        assertThat(GeoMesaVariantFunctions.fieldsEqual(fields, path(), path("missing"), Variant.NULL_VALUE)).isFalse();
        assertThat(GeoMesaVariantFunctions.fieldsEqual(items, path(), path(), Variant.NULL_VALUE)).isFalse();
        assertThat(GeoMesaVariantFunctions.fieldsEqual(fields, null, path(), fields)).isFalse();
        assertThat(GeoMesaVariantFunctions.fieldsEqual(fields, path(), null, fields)).isFalse();
    }

    @Test
    void candidateFunctionsKeepUncertaintyAndOnlyRejectUnequalStrings() {
        Variant expected = Variant.ofString("001");
        for (Variant value : Arrays.asList(null, Variant.NULL_VALUE, Variant.ofLong(1), Variant.ofDouble(1),
                Variant.ofBoolean(true), object("v", Variant.ofString("001")), Variant.ofArray(List.of(Variant.ofString("001"))))) {
            assertThat(GeoMesaVariantFunctions.mayEqual(value, path(), expected)).isTrue();
        }
        assertThat(GeoMesaVariantFunctions.mayEqual(Variant.ofString("001"), path(), expected)).isTrue();
        assertThat(GeoMesaVariantFunctions.mayEqual(Variant.ofString("1"), path(), expected)).isFalse();
        assertThat(GeoMesaVariantFunctions.mayEqual(Variant.ofString("001"), path(), Variant.ofLong(1))).isTrue();
        assertThat(GeoMesaVariantFunctions.mayEqual(Variant.ofString("x"), null, expected)).isTrue();
        assertThat(GeoMesaVariantFunctions.mayEqual(Variant.ofString("x"), path(), null)).isTrue();
        assertThat(GeoMesaVariantFunctions.mayEqual(object("a", Variant.ofLong(1)), path("a", "b"), expected)).isTrue();
        assertThat(GeoMesaVariantFunctions.anyMayEqual(Variant.ofArray(List.of(Variant.ofString("x"), Variant.ofString("y"))), path(), path(), expected)).isFalse();
        assertThat(GeoMesaVariantFunctions.anyMayEqual(Variant.ofArray(List.of(Variant.ofString("x"), Variant.ofLong(1))), path(), path(), expected)).isTrue();
        assertThat(GeoMesaVariantFunctions.anyMayEqual(Variant.ofArray(List.of()), path(), path(), expected)).isTrue();
        assertThat(GeoMesaVariantFunctions.anyMayEqual(null, path(), path(), expected)).isTrue();
        assertThat(GeoMesaVariantFunctions.fieldsMayEqual(object("a", Variant.ofString("x"), "b", Variant.ofString("y")), path(), path(), expected)).isFalse();
        assertThat(GeoMesaVariantFunctions.fieldsMayEqual(object("a", Variant.ofString("x"), "b", Variant.ofLong(1)), path(), path(), expected)).isTrue();
        assertThat(GeoMesaVariantFunctions.fieldsMayEqual(object(), path(), path(), expected)).isTrue();
        assertThat(GeoMesaVariantFunctions.fieldsMayEqual(null, path(), path(), expected)).isTrue();
        assertThat(GeoMesaVariantFunctions.fieldsMayEqual(object("a", Variant.ofString("x")), path(), null, expected)).isTrue();
    }

    private static Block path(String... keys) {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, keys.length);
        for (String key : keys) {
            if (key == null) builder.appendNull();
            else VARCHAR.writeString(builder, key);
        }
        return builder.build();
    }

    private static Variant object(Object... pairs) {
        Map<Slice, Variant> fields = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) fields.put(utf8Slice((String) pairs[i]), (Variant) pairs[i + 1]);
        return Variant.ofObject(fields);
    }
}

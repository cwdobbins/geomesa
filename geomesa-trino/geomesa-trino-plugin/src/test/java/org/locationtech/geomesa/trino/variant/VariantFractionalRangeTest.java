/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.variant;

import io.trino.spi.variant.Variant;
import org.geotools.factory.CommonFactoryFinder;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class VariantFractionalRangeTest {
    @Test void candidatesNeverDiscardGeoToolsNumericMatches() {
        var values = new ArrayList<Number>(List.of((byte) -1, (short) 1, 0, 16777217, 9007199254740992L,
                -0.0f, 0.1f, Math.nextUp(0.1f), Float.MIN_VALUE, Float.MAX_VALUE,
                -0.0d, 0.1d, Math.nextDown(0.1d), Math.nextUp(0.1d), Double.MIN_VALUE, Double.MAX_VALUE,
                Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY,
                new BigDecimal("0.10000000000000000000000000000000000001"),
                new BigDecimal("0.10"), new BigDecimal("1E-38"), new BigDecimal("99999999999999999999999999999999999999")));
        var random = new java.util.Random(19);
        for (int i = 0; i < 40; i++) {
            double value = Math.scalb(random.nextDouble() - 0.5, random.nextInt(80) - 40);
            values.add(value);
            values.add((float) value);
            values.add(BigDecimal.valueOf(value).setScale(20, java.math.RoundingMode.HALF_EVEN));
        }
        var ff = CommonFactoryFinder.getFilterFactory();
        for (Number left : values) {
            for (Number right : values) {
                for (var comparison : VariantReferenceEvaluator.Comparison.values()) {
                    var a = ff.literal(left);
                    var b = ff.literal(right);
                    var filter = switch (comparison) {
                        case GREATER_THAN -> ff.greater(a, b);
                        case GREATER_THAN_OR_EQUAL -> ff.greaterOrEqual(a, b);
                        case LESS_THAN -> ff.less(a, b);
                        case LESS_THAN_OR_EQUAL -> ff.lessOrEqual(a, b);
                    };
                    if (filter.evaluate(null)) {
                        assertThat(VariantReferenceEvaluator.mayCompareValues(variant(left), variant(right), comparison))
                                .as("%s(%s) %s %s(%s)", left, left.getClass(), comparison, right, right.getClass()).isTrue();
                    }
                }
            }
        }
    }

    @Test void decimalRoundingSignedZeroAndFloatTextRemainConservative() {
        var gt = VariantReferenceEvaluator.Comparison.GREATER_THAN;
        var gte = VariantReferenceEvaluator.Comparison.GREATER_THAN_OR_EQUAL;
        var lt = VariantReferenceEvaluator.Comparison.LESS_THAN;
        var a = Variant.ofDecimal(new BigDecimal("0.10000000000000000000000000000000000001"));
        var b = Variant.ofDecimal(new BigDecimal("0.1"));
        assertThat(VariantReferenceEvaluator.mayCompareValues(a, b, gt)).isFalse();
        assertThat(VariantReferenceEvaluator.mayCompareValues(b, a, gte)).isTrue();
        assertThat(VariantReferenceEvaluator.mayCompareValues(Variant.ofDouble(-0.0), Variant.ofDouble(0.0), lt)).isFalse();
        assertThat(VariantReferenceEvaluator.mayCompareValues(Variant.ofFloat(0.1f), Variant.ofDouble(0.1), gte)).isTrue();
        assertThat(VariantReferenceEvaluator.mayCompareValues(Variant.ofDouble(0.1), Variant.ofFloat(0.1f), gte)).isTrue();
        assertThat(VariantReferenceEvaluator.mayCompareValues(Variant.ofDecimal(new BigDecimal("1.25")), Variant.ofDouble(2.5), gt)).isFalse();
        assertThat(VariantReferenceEvaluator.mayCompareValues(Variant.ofDouble(2.5), Variant.ofDecimal(new BigDecimal("1.25")), lt)).isFalse();
        for (double value : new double[]{Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY}) {
            for (var comparison : VariantReferenceEvaluator.Comparison.values()) {
                assertThat(VariantReferenceEvaluator.mayCompareValues(Variant.ofDouble(value), b, comparison)).isTrue();
                assertThat(VariantReferenceEvaluator.mayCompareValues(b, Variant.ofDouble(value), comparison)).isTrue();
            }
        }
    }

    private static Variant variant(Number value) {
        if (value instanceof BigDecimal decimal) return Variant.ofDecimal(decimal);
        if (value instanceof Float number) return Variant.ofFloat(number);
        if (value instanceof Double number) return Variant.ofDouble(number);
        if (value instanceof Byte number) return Variant.ofByte(number);
        if (value instanceof Short number) return Variant.ofShort(number);
        if (value instanceof Integer number) return Variant.ofInt(number);
        return Variant.ofLong(value.longValue());
    }
}

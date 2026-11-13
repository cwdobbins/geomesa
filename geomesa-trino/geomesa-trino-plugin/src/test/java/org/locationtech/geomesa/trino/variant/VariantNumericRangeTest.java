/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.variant;

import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.spi.expression.Constant;
import io.trino.spi.variant.Variant;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.LongFunction;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.iceberg.ColumnIdentity.primitiveColumnIdentity;
import static io.trino.spi.type.VariantType.VARIANT;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.assertj.core.api.Assertions.assertThat;

class VariantNumericRangeTest {
    private static final long LIMIT = 1L << 53;
    private static final IcebergColumnHandle DOC = IcebergColumnHandle.optional(primitiveColumnIdentity(1, "doc")).columnType(VARIANT).build();
    private static final List<VariantPredicate.Kind> KINDS = List.of(
            VariantPredicate.Kind.MAY_GREATER_THAN, VariantPredicate.Kind.MAY_GREATER_THAN_OR_EQUAL,
            VariantPredicate.Kind.MAY_LESS_THAN, VariantPredicate.Kind.MAY_LESS_THAN_OR_EQUAL);

    @Test void allOperatorsAndWidthsRespectInclusiveBoundaries() {
        List<LongFunction<Variant>> factories = List.of(x -> Variant.ofByte((byte) x), x -> Variant.ofShort((short) x),
                x -> Variant.ofInt((int) x), Variant::ofLong);
        for (var factory : factories) {
            for (var kind : KINDS) {
                for (var boundFactory : factories) {
                var predicate = predicate(kind, boundFactory.apply(10));
                for (long value : new long[] {-20, 9, 10, 11, 100}) {
                    assertThat(predicate.test(document(factory.apply(value)))).as("%s %s", kind, value)
                            .isEqualTo(matches(kind, value, 10));
                }
                }
            }
        }
    }

    @Test void uncertainValuesAndUnsafeIntegersAreRetained() {
        List<Variant> uncertain = List.of(Variant.NULL_VALUE, Variant.ofString("11"),
                Variant.ofDouble(Double.NaN), Variant.ofDouble(Double.POSITIVE_INFINITY), Variant.ofBoolean(false),
                Variant.ofArray(List.of(Variant.ofInt(11))), Variant.EMPTY_OBJECT);
        for (var kind : KINDS) {
            var predicate = predicate(kind, Variant.ofInt(10));
            for (var value : uncertain) assertThat(predicate.test(document(value))).as("%s %s", kind, value).isTrue();
            assertThat(predicate.test(null)).isTrue();
            assertThat(predicate.test(Variant.EMPTY_OBJECT)).isTrue();
            for (long value : new long[] {Long.MIN_VALUE, -LIMIT - 1, LIMIT + 1, Long.MAX_VALUE}) {
                assertThat(predicate(kind, Variant.ofLong(10)).test(document(Variant.ofLong(value)))).isTrue();
                assertThat(predicate(kind, Variant.ofLong(value)).test(document(Variant.ofLong(10)))).isTrue();
            }
        }
    }

    @Test void integerBoundsNeverExcludeAnActualMatch() {
        for (int width : new int[] {8, 16, 32, 64}) {
            for (var kind : KINDS) {
                for (int bound : new int[] {-21, -20, 0, 20, 21}) {
                    Variant expected = switch (width) {
                        case 8 -> Variant.ofByte((byte) bound);
                        case 16 -> Variant.ofShort((short) bound);
                        case 32 -> Variant.ofInt(bound);
                        default -> Variant.ofLong(bound);
                    };
                    boolean any = false;
                    for (long value = -20; value <= 20; value++) any |= matches(kind, value, bound);
                    assertThat(evaluator(kind, expected).cannotMatchRowGroup("doc", 41, columns(width, -20, 20, 0)))
                            .as("width=%s kind=%s bound=%s", width, kind, bound).isEqualTo(!any);
                }
            }
        }
    }

    @Test void everyFallbackAlongThePathMustBeProvenNull() {
        var evaluator = evaluator(VariantPredicate.Kind.MAY_GREATER_THAN, Variant.ofInt(100));
        var proven = columns(32, 1, 20, 0);
        assertThat(evaluator.cannotMatchRowGroup("doc", 41, proven)).isTrue();
        for (int index = 0; index < 3; index++) {
            var mixed = new ArrayList<>(proven);
            var path = List.of(mixed.get(index).getPath().toArray());
            mixed.set(index, fallback(path, 40));
            assertThat(evaluator.cannotMatchRowGroup("doc", 41, mixed)).as("fallback %s", path).isFalse();
            mixed.set(index, fallback(path, null));
            assertThat(evaluator.cannotMatchRowGroup("doc", 41, mixed)).isFalse();
            mixed.remove(index);
            assertThat(evaluator.cannotMatchRowGroup("doc", 41, mixed)).isFalse();
        }
    }

    @Test void candidateUnknownRowsCannotBeRemovedByStatistics() {
        var evaluator = evaluator(VariantPredicate.Kind.MAY_GREATER_THAN, Variant.ofInt(100));
        // Missing/null values make the candidate true even though the residual will decide whether they match.
        assertThat(evaluator.cannotMatchRowGroup("doc", 41, columns(32, 1, 20, 1))).isFalse();
        var missingNullCount = new ArrayList<>(columns(32, 1, 20, 0));
        missingNullCount.set(3, numeric(32, 1, 20, null));
        assertThat(evaluator.cannotMatchRowGroup("doc", 41, missingNullCount)).isFalse();
        assertThat(evaluator.cannotMatchRowGroup("doc", 42, columns(32, 1, 20, 0))).isFalse();
        assertThat(evaluator.cannotMatchRowGroup("doc", 41, columns(16, 1, 20, 0))).isTrue();
        assertThat(evaluator.cannotMatchRowGroup("doc", 41, columns(32, 20, 1, 0))).isFalse();
    }

    @Test void absentUnknownOrInvalidBoundsNeverExcludeRows() {
        var evaluator = evaluator(VariantPredicate.Kind.MAY_GREATER_THAN, Variant.ofInt(100));
        assertThat(evaluator.cannotMatchRowGroup("doc", 41, List.of())).isFalse();
        assertThat(evaluator.cannotMatchRowGroup("renamed", 41, columns(32, 1, 20, 0))).isFalse();
        assertThat(evaluator.cannotMatchRowGroup("doc", 0, columns(32, 1, 20, 0))).isFalse();
        var noBounds = new ArrayList<>(columns(32, 1, 20, 0));
        var old = noBounds.get(3);
        noBounds.set(3, chunk(List.of(old.getPath().toArray()), old.getPrimitiveType(), new IntStatistics()));
        assertThat(evaluator.cannotMatchRowGroup("doc", 41, noBounds)).isFalse();
        assertThat(evaluator(VariantPredicate.Kind.MAY_GREATER_THAN, Variant.ofByte((byte) 100))
                .cannotMatchRowGroup("doc", 41, columns(8, -200, 20, 0))).isFalse();
        assertThat(evaluator(VariantPredicate.Kind.MAY_GREATER_THAN, Variant.ofShort((short) 100))
                .cannotMatchRowGroup("doc", 41, columns(16, -40000, 20, 0))).isFalse();
        for (Variant value : List.of(Variant.ofString("100"), Variant.ofDouble(Double.NaN), Variant.NULL_VALUE)) {
            assertThat(evaluator(VariantPredicate.Kind.MAY_GREATER_THAN, value)
                    .cannotMatchRowGroup("doc", 41, columns(32, 1, 20, 0))).isFalse();
        }
    }

    @Test void integerSafetyBoundaryMustNotRoundThroughDouble() {
        for (long unsafe : new long[] {LIMIT + 1, -LIMIT - 1, Long.MAX_VALUE, Long.MIN_VALUE}) {
            var evaluator = evaluator(VariantPredicate.Kind.MAY_GREATER_THAN, Variant.ofLong(unsafe));
            assertThat(evaluator.cannotMatchRowGroup("doc", 41, columns(64, -10, 10, 0))).isFalse();
            var safe = evaluator(VariantPredicate.Kind.MAY_LESS_THAN, Variant.ofLong(-10));
            assertThat(safe.cannotMatchRowGroup("doc", 41, columns(64, unsafe, unsafe, 0))).isFalse();
        }
        assertThat(evaluator(VariantPredicate.Kind.MAY_GREATER_THAN, Variant.ofLong(LIMIT))
                .cannotMatchRowGroup("doc", 41, columns(64, LIMIT - 1, LIMIT, 0))).isTrue();
    }

    @Test void decimalBoundsUseScaleAndGeoToolsRounding() {
        for (int precision : new int[]{9, 18, 38}) {
            for (var physical : precision == 9 ? List.of(PrimitiveType.PrimitiveTypeName.INT32)
                    : precision == 18 ? List.of(PrimitiveType.PrimitiveTypeName.INT64)
                    : List.of(PrimitiveType.PrimitiveTypeName.BINARY, PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)) {
                for (var kind : KINDS) {
                    for (String literal : List.of("-2.01", "-2.00", "0.0", "2.00", "2.01")) {
                        var expected = Variant.ofDecimal(new java.math.BigDecimal(literal));
                        boolean any = false;
                        for (int i = -200; i <= 200; i++) {
                            any |= predicate(kind, expected).test(document(Variant.ofDecimal(java.math.BigDecimal.valueOf(i, 2))));
                        }
                        assertThat(evaluator(kind, expected).cannotMatchRowGroup("doc", 41,
                                decimalColumns(physical, precision, 2, "-200", "200", 0)))
                                .as("%s %s %s", physical, kind, literal).isEqualTo(!any);
                    }
                }
            }
        }
        var rounding = evaluator(VariantPredicate.Kind.MAY_GREATER_THAN_OR_EQUAL, Variant.ofDouble(0.1));
        // Exact decimal is lower, but GeoTools compares equal doubles. Never prune this match.
        assertThat(rounding.cannotMatchRowGroup("doc", 41, decimalColumns(PrimitiveType.PrimitiveTypeName.BINARY,
                38, 38, "9999999999999999999999999999999999999", "9999999999999999999999999999999999999", 0))).isFalse();
    }

    @Test void decimalNullsFallbackAndInvalidBoundsKeepGroups() {
        var evaluator = evaluator(VariantPredicate.Kind.MAY_GREATER_THAN, Variant.ofDouble(100));
        var physical = PrimitiveType.PrimitiveTypeName.INT32;
        assertThat(evaluator.cannotMatchRowGroup("doc", 41, decimalColumns(physical, 9, 2, "100", "200", 1))).isFalse();
        assertThat(evaluator.cannotMatchRowGroup("doc", 41, decimalColumns(physical, 9, 2, "100", "200", -1))).isFalse();
        assertThat(evaluator.cannotMatchRowGroup("doc", 41, decimalColumns(physical, 9, 2, "200", "100", 0))).isFalse();
        assertThat(evaluator.cannotMatchRowGroup("doc", 41, decimalColumns(physical, 2, 2, "100", "200", 0))).isFalse();
        var mixed = new ArrayList<>(decimalColumns(physical, 9, 2, "100", "200", 0));
        for (int i = 0; i < 3; i++) {
            var copy = new ArrayList<>(mixed);
            copy.set(i, fallback(List.of(copy.get(i).getPath().toArray()), 40));
            assertThat(evaluator.cannotMatchRowGroup("doc", 41, copy)).isFalse();
        }
    }

    @Test void floatingBoundsCannotProveAbsenceOfNan() {
        for (var physical : List.of(PrimitiveType.PrimitiveTypeName.FLOAT, PrimitiveType.PrimitiveTypeName.DOUBLE)) {
            var type = Types.optional(physical).named("typed_value");
            Statistics<?> stats = Statistics.createStats(type);
            if (physical == PrimitiveType.PrimitiveTypeName.FLOAT) {
                ((org.apache.parquet.column.statistics.FloatStatistics) stats).updateStats(1f);
                ((org.apache.parquet.column.statistics.FloatStatistics) stats).updateStats(Float.NaN);
            } else {
                ((org.apache.parquet.column.statistics.DoubleStatistics) stats).updateStats(1d);
                ((org.apache.parquet.column.statistics.DoubleStatistics) stats).updateStats(Double.NaN);
            }
            stats.setNumNulls(0);
            var columns = new ArrayList<>(columns(32, 1, 20, 0));
            columns.set(3, chunk(List.of(columns.get(3).getPath().toArray()), type, stats));
            for (var kind : KINDS) {
                assertThat(evaluator(kind, Variant.ofDouble(100)).cannotMatchRowGroup("doc", 41, columns)).isFalse();
            }
        }
    }

    private static List<ColumnChunkMetadata> decimalColumns(PrimitiveType.PrimitiveTypeName physical,
            int precision, int scale, String min, String max, long nulls) {
        var builder = Types.optional(physical);
        if (physical == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) builder.length(16);
        var type = builder.as(LogicalTypeAnnotation.decimalType(scale, precision)).named("typed_value");
        Statistics<?> stats = Statistics.createStats(type);
        switch (physical) {
            case INT32 -> ((IntStatistics) stats).setMinMax(Integer.parseInt(min), Integer.parseInt(max));
            case INT64 -> ((LongStatistics) stats).setMinMax(Long.parseLong(min), Long.parseLong(max));
            default -> ((BinaryStatistics) stats).setMinMax(binaryDecimal(min, physical), binaryDecimal(max, physical));
        }
        stats.setNumNulls(nulls);
        var result = new ArrayList<>(columns(32, 1, 20, 0));
        result.set(3, chunk(List.of(result.get(3).getPath().toArray()), type, stats));
        return result;
    }

    private static org.apache.parquet.io.api.Binary binaryDecimal(String value, PrimitiveType.PrimitiveTypeName physical) {
        var number = new java.math.BigInteger(value);
        byte[] bytes = number.toByteArray();
        if (physical == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
            byte[] padded = new byte[16];
            java.util.Arrays.fill(padded, number.signum() < 0 ? (byte) -1 : 0);
            System.arraycopy(bytes, 0, padded, padded.length - bytes.length, bytes.length);
            bytes = padded;
        }
        return org.apache.parquet.io.api.Binary.fromConstantByteArray(bytes);
    }

    private static boolean matches(VariantPredicate.Kind kind, long actual, long expected) {
        return switch (kind) {
            case MAY_GREATER_THAN -> actual > expected;
            case MAY_GREATER_THAN_OR_EQUAL -> actual >= expected;
            case MAY_LESS_THAN -> actual < expected;
            case MAY_LESS_THAN_OR_EQUAL -> actual <= expected;
            default -> throw new AssertionError(kind);
        };
    }
    private static Variant document(Variant value) {
        return Variant.ofObject(Map.of(utf8Slice("outer"), Variant.ofObject(Map.of(utf8Slice("score"), value))));
    }
    private static VariantPredicate predicate(VariantPredicate.Kind kind, Variant expected) {
        return new VariantPredicate(kind, DOC, List.of("outer", "score"), List.of(), new Constant(expected, VARIANT));
    }
    private static VariantTypedEvaluator evaluator(VariantPredicate.Kind kind, Variant expected) {
        return new VariantTypedEvaluator(List.of(predicate(kind, expected)));
    }
    private static List<ColumnChunkMetadata> columns(int width, long min, long max, long nulls) {
        return List.of(fallback(List.of("doc", "value"), 41),
                fallback(List.of("doc", "typed_value", "outer", "value"), 41),
                fallback(List.of("doc", "typed_value", "outer", "typed_value", "score", "value"), 41),
                numeric(width, min, max, nulls));
    }
    private static ColumnChunkMetadata fallback(List<String> path, Integer nulls) {
        var type = Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).named("value");
        var stats = new BinaryStatistics();
        stats.setNumNulls(nulls == null ? -1 : nulls);
        return chunk(path, type, stats);
    }
    private static ColumnChunkMetadata numeric(int width, long min, long max, Long nulls) {
        var type = Types.optional(width == 64 ? PrimitiveType.PrimitiveTypeName.INT64 : PrimitiveType.PrimitiveTypeName.INT32)
                .as(LogicalTypeAnnotation.intType(width, true)).named("typed_value");
        Statistics<?> stats = Statistics.createStats(type);
        if (width == 64) ((LongStatistics) stats).setMinMax(min, max);
        else ((IntStatistics) stats).setMinMax((int) min, (int) max);
        stats.setNumNulls(nulls == null ? -1 : nulls);
        return chunk(List.of("doc", "typed_value", "outer", "typed_value", "score", "typed_value"), type, stats);
    }
    private static ColumnChunkMetadata chunk(List<String> path, PrimitiveType type, Statistics<?> stats) {
        return ColumnChunkMetadata.get(ColumnPath.get(path.toArray(String[]::new)), type, UNCOMPRESSED,
                null, java.util.Set.of(), stats, 0, 0, 41, 0, 0);
    }
}

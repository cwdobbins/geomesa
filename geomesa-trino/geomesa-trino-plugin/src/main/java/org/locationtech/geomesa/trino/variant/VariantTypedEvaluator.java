/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.variant;

import io.trino.parquet.reader.VariantBatchAccess;
import io.trino.parquet.reader.VariantPathProbe;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.spi.variant.Header.BasicType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.spi.variant.Variant;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.column.statistics.Statistics;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.locationtech.geomesa.trino.variant.VariantReferenceEvaluator.integer;
import static org.locationtech.geomesa.trino.variant.VariantReferenceEvaluator.supportedNumber;

/** Executes exact predicates and conservative candidates using typed probes, reference fallback and row-group bounds. */
public final class VariantTypedEvaluator implements VariantRuntimeEvaluator {
    private final List<VariantPredicate> predicates;
    private long directRows;
    private long fallbackRows;

    public VariantTypedEvaluator(List<VariantPredicate> predicates) {
        this.predicates = List.copyOf(predicates);
        if (predicates.isEmpty() || predicates.stream().anyMatch(p -> !p.column().equals(predicates.getFirst().column()))) {
            throw new IllegalArgumentException("Expected predicates on one physical VARIANT column");
        }
    }

    @Override public Decision evaluate(VariantBatchAccess batch, int originalPosition) {
        boolean fallback = false;
        boolean matches = true;
        for (VariantPredicate predicate : predicates) {
            List<VariantPathProbe> probes = batch.probe(predicate.demand(), originalPosition);
            boolean unresolved = probes.stream().anyMatch(probe -> probe.state() == VariantPathProbe.State.UNRESOLVED
                || (probe.state() == VariantPathProbe.State.TYPED_VALUE && !(supported(probe.type()) || isNumericRange(predicate.kind()) && supportedRangeType(probe.type()))));
            if (unresolved) {
                fallback = true;
                matches = predicate.test(batch.reference(originalPosition).orElse(null));
            } else {
                Variant expected = (Variant) predicate.expected().getValue();
                if (predicate.kind().conservative()) {
                    matches = probes.isEmpty() || probes.stream().anyMatch(probe ->
                        probe.state() != VariantPathProbe.State.TYPED_VALUE || switch (predicate.kind()) {
                            case MAY_EQUAL, ANY_MAY_EQUAL -> VariantReferenceEvaluator.stringsMayEqual(probe.value(), expected);
                            case MAY_GREATER_THAN -> VariantReferenceEvaluator.mayCompareValues(probe.value(), expected, VariantReferenceEvaluator.Comparison.GREATER_THAN);
                            case MAY_GREATER_THAN_OR_EQUAL -> VariantReferenceEvaluator.mayCompareValues(probe.value(), expected, VariantReferenceEvaluator.Comparison.GREATER_THAN_OR_EQUAL);
                            case MAY_LESS_THAN -> VariantReferenceEvaluator.mayCompareValues(probe.value(), expected, VariantReferenceEvaluator.Comparison.LESS_THAN);
                            case MAY_LESS_THAN_OR_EQUAL -> VariantReferenceEvaluator.mayCompareValues(probe.value(), expected, VariantReferenceEvaluator.Comparison.LESS_THAN_OR_EQUAL);
                            default -> throw new IllegalStateException("Unexpected conservative predicate: " + predicate.kind());
                        });
                } else {
                    matches = probes.stream().anyMatch(probe -> probe.state() == VariantPathProbe.State.TYPED_VALUE
                        && probe.value().equals(expected));
                }
            }
            if (!matches) break;
        }
        if (fallback) fallbackRows++; else directRows++;
        return matches ? Decision.TRUE : Decision.FALSE;
    }

    @Override public Decision evaluateReference(Variant value) {
        fallbackRows++;
        return predicates.stream().allMatch(predicate -> predicate.test(value)) ? Decision.TRUE : Decision.FALSE;
    }

    @Override
    public boolean cannotMatchRowGroup(String sourceColumn, long rowCount, List<ColumnChunkMetadata> columns)
    {
        for (VariantPredicate predicate : predicates) {
            if (predicate.kind().array()) {
                if (arrayStringRangeExcludes(predicate, sourceColumn, rowCount, columns)) return true;
                continue;
            }
            if (predicate.path().isEmpty()) {
                continue;
            }
            Variant expected = (Variant) predicate.expected().getValue();
            boolean stringEquality = predicate.kind() == VariantPredicate.Kind.EQUALS && isString(expected);
            boolean numericRange = isNumericRange(predicate.kind()) && supportedNumber(expected);
            if (!stringEquality && !numericRange) {
                continue;
            }

            // Fallback at any object or leaf can contain a matching value that typed bounds omit.
            List<String> typedPath = new java.util.ArrayList<>();
            typedPath.add(sourceColumn);
            boolean complete = allNull(columns, typedPath, rowCount);
            for (String key : predicate.path()) {
                typedPath.add("typed_value");
                typedPath.add(key);
                complete &= allNull(columns, typedPath, rowCount);
            }
            typedPath.add("typed_value");
            ColumnChunkMetadata typed = findColumn(columns, typedPath);
            if (!complete || typed == null || rowCount <= 0 || rowCount != typed.getValueCount()) {
                continue;
            }

            Statistics<?> typedStatistics = typed.getStatistics();
            if (typedStatistics == null || typedStatistics.isEmpty() || !typedStatistics.hasNonNullValue() ||
                    typedStatistics.genericGetMin() == null || typedStatistics.genericGetMax() == null) {
                continue;
            }
            if (stringEquality && !(typed.getPrimitiveType().getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation)) {
                continue;
            }
            // Candidate functions keep missing/null/unsupported values. Bounds alone cannot exclude those rows.
            if (numericRange && typedStatistics.isNumNullsSet() && typedStatistics.getNumNulls() == 0
                    && rowGroupNumericRangeExcludes(predicate, expected, typed, typedStatistics)) {
                return true;
            }
            if (!stringEquality) {
                continue;
            }
            Object minimum = typedStatistics.genericGetMin();
            Object maximum = typedStatistics.genericGetMax();
            if (!(minimum instanceof Binary minBinary) || !(maximum instanceof Binary maxBinary)) {
                continue;
            }
            Binary literal = Binary.fromConstantByteArray(expected.getStringUtf8().getBytes(StandardCharsets.UTF_8));
            if (minBinary.compareTo(maxBinary) <= 0 && (minBinary.compareTo(literal) > 0 || maxBinary.compareTo(literal) < 0)) {
                return true;
            }
        }
        return false;
    }

    private static boolean arrayStringRangeExcludes(VariantPredicate predicate, String sourceColumn,
            long rowCount, List<ColumnChunkMetadata> columns) {
        Variant expected = (Variant) predicate.expected().getValue();
        if (rowCount <= 0 || !isString(expected)) return false;
        List<String> node = new java.util.ArrayList<>(List.of(sourceColumn));
        if (!allNull(columns, node, rowCount)) return false;
        for (String key : predicate.path()) {
            node.add("typed_value");
            node.add(key);
            if (!allNull(columns, node, rowCount)) return false;
        }
        node.addAll(List.of("typed_value", "list", "element"));
        List<List<String>> repeatedNodes = new java.util.ArrayList<>();
        repeatedNodes.add(List.copyOf(node));
        for (String key : predicate.elementPath()) {
            node.add("typed_value");
            node.add(key);
            repeatedNodes.add(List.copyOf(node));
        }
        node.add("typed_value");
        ColumnChunkMetadata typed = findColumn(columns, node);
        if (typed == null || typed.getValueCount() < rowCount
                || typed.getPrimitiveType().getPrimitiveTypeName() != org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY
                || !(typed.getPrimitiveType().getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation)) return false;
        // One repetition level: leaf entries include null placeholders for empty/missing lists.
        // All repeated fallback nodes must account for the same entries, not merely rowCount.
        long entries = typed.getValueCount();
        for (List<String> repeated : repeatedNodes) {
            if (!allNull(columns, repeated, entries)) return false;
        }
        Statistics<?> statistics = typed.getStatistics();
        if (statistics == null || !statistics.hasNonNullValue()) return false;
        if (predicate.kind().conservative()
                && (!statistics.isNumNullsSet() || statistics.getNumNulls() != 0)) {
            // ANY_MAY_EQUAL keeps empty selections, missing keys, nulls and wrong shapes.
            // Zero leaf nulls proves a populated string in every entry and at least one per row.
            return false;
        }
        if (!(statistics.genericGetMin() instanceof Binary min) || !(statistics.genericGetMax() instanceof Binary max)
                || min.compareTo(max) > 0) return false;
        Binary literal = Binary.fromConstantByteArray(expected.getStringUtf8().getBytes(StandardCharsets.UTF_8));
        return min.compareTo(literal) > 0 || max.compareTo(literal) < 0;
    }

    private static boolean allNull(List<ColumnChunkMetadata> columns, List<String> node, long rowCount)
    {
        List<String> path = new java.util.ArrayList<>(node);
        path.add("value");
        ColumnChunkMetadata column = findColumn(columns, path);
        if (column == null || column.getValueCount() != rowCount) {
            return false;
        }
        Statistics<?> statistics = column.getStatistics();
        return statistics != null && statistics.isNumNullsSet() && statistics.getNumNulls() == rowCount;
    }

    private static ColumnChunkMetadata findColumn(List<ColumnChunkMetadata> columns, List<String> path)
    {
        return columns.stream().filter(column -> List.of(column.getPath().toArray()).equals(path)).findFirst().orElse(null);
    }

    private static boolean isString(Variant value)
    {
        return value.basicType() == BasicType.SHORT_STRING || value.basicType() == BasicType.PRIMITIVE
                && value.primitiveType() == io.trino.spi.variant.Header.PrimitiveType.STRING;
    }

    private static boolean isNumericRange(VariantPredicate.Kind kind)
    {
        return switch (kind) {
            case MAY_GREATER_THAN, MAY_GREATER_THAN_OR_EQUAL, MAY_LESS_THAN, MAY_LESS_THAN_OR_EQUAL -> true;
            default -> false;
        };
    }

    private static boolean rowGroupNumericRangeExcludes(
            VariantPredicate predicate,
            Variant expected,
            ColumnChunkMetadata typed,
            Statistics<?> statistics)
    {
        Variant minimum = numericBound(typed, statistics.genericGetMin());
        Variant maximum = numericBound(typed, statistics.genericGetMax());
        if (minimum == null || maximum == null) return false;
        // Comparing decimal bounds exactly also rejects corrupt or reversed bounds that round
        // to the same double. The candidate itself deliberately follows GeoTools coercion.
        java.math.BigDecimal min = decimalValue(minimum);
        java.math.BigDecimal max = decimalValue(maximum);
        if (min.compareTo(max) > 0) return false;
        var comparison = switch (predicate.kind()) {
            case MAY_GREATER_THAN -> VariantReferenceEvaluator.Comparison.GREATER_THAN;
            case MAY_GREATER_THAN_OR_EQUAL -> VariantReferenceEvaluator.Comparison.GREATER_THAN_OR_EQUAL;
            case MAY_LESS_THAN -> VariantReferenceEvaluator.Comparison.LESS_THAN;
            case MAY_LESS_THAN_OR_EQUAL -> VariantReferenceEvaluator.Comparison.LESS_THAN_OR_EQUAL;
            default -> throw new IllegalArgumentException("Not a range predicate");
        };
        Variant bound = switch (predicate.kind()) {
            case MAY_GREATER_THAN, MAY_GREATER_THAN_OR_EQUAL -> maximum;
            default -> minimum;
        };
        return !VariantReferenceEvaluator.mayCompareValues(bound, expected, comparison);
    }

    private static java.math.BigDecimal decimalValue(Variant value) {
        var integral = integer(value);
        return integral.isPresent() ? java.math.BigDecimal.valueOf(integral.getAsLong()) : value.getDecimal();
    }

    private static Variant numericBound(ColumnChunkMetadata column, Object bound) {
        var type = column.getPrimitiveType();
        var physical = type.getPrimitiveTypeName();
        if (type.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal) {
            int precision = decimal.getPrecision();
            int scale = decimal.getScale();
            if (precision < 1 || precision > 38 || scale < 0 || scale > precision) return null;
            java.math.BigInteger unscaled;
            switch (physical) {
                case INT32 -> {
                    if (precision > 9 || !(bound instanceof Integer value)) return null;
                    unscaled = java.math.BigInteger.valueOf(value);
                }
                case INT64 -> {
                    if (precision > 18 || !(bound instanceof Long value)) return null;
                    unscaled = java.math.BigInteger.valueOf(value);
                }
                case BINARY, FIXED_LEN_BYTE_ARRAY -> {
                    if (!(bound instanceof Binary value) || value.length() == 0 || value.length() > 16) return null;
                    if (physical == org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
                            && value.length() != type.getTypeLength()) return null;
                    unscaled = new java.math.BigInteger(value.getBytes());
                }
                default -> { return null; }
            }
            java.math.BigDecimal value = new java.math.BigDecimal(unscaled, scale);
            return value.precision() <= precision ? Variant.ofDecimal(value) : null;
        }
        if (!(type.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation integer)
                || !integer.isSigned() || !List.of(8, 16, 32, 64).contains(integer.getBitWidth())) {
            // FLOAT/DOUBLE bounds omit NaNs. Without a NaN count they cannot exclude a group:
            // the candidate intentionally retains all non-finite observations.
            return null;
        }
        boolean int64 = integer.getBitWidth() == 64;
        if (int64 ? physical != org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64 || !(bound instanceof Long)
                : physical != org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32 || !(bound instanceof Integer)) return null;
        long value = ((Number) bound).longValue();
        long lower = int64 ? Long.MIN_VALUE : -(1L << (integer.getBitWidth() - 1));
        long upper = int64 ? Long.MAX_VALUE : (1L << (integer.getBitWidth() - 1)) - 1;
        return value >= lower && value <= upper && safeInteger(value) ? Variant.ofLong(value) : null;
    }

    private static boolean supportedRangeType(Type type) {
        return type instanceof io.trino.spi.type.DecimalType || type instanceof io.trino.spi.type.RealType
                || type instanceof io.trino.spi.type.DoubleType;
    }

    private static boolean safeInteger(long value)
    {
        return value >= -(1L << 53) && value <= (1L << 53);
    }

    @Override public boolean exact() { return true; }
    public long directRows() { return directRows; }
    public long fallbackRows() { return fallbackRows; }

    static boolean supported(Type type) {
        return type instanceof BooleanType || type instanceof VarcharType || type instanceof BigintType
            || type instanceof IntegerType || type instanceof SmallintType || type instanceof TinyintType;
    }
}

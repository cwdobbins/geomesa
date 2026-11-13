/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.variant;

import io.trino.spi.variant.Header;
import io.trino.spi.variant.Variant;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.types.Types;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Inclusive Iceberg file hints. Row-level predicates remain authoritative. */
public final class VariantFilePruning {
    // Iceberg extract currently accepts shorthand object paths, not bracket-quoted keys.
    // Keep unusual literal keys in the reader instead of changing their meaning by joining them.
    private static final Pattern MEMBER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private VariantFilePruning() {}

    public static Set<Integer> columns(List<VariantPredicate> predicates) {
        return predicates.stream().filter(VariantFilePruning::eligible).map(predicate -> predicate.column().getId())
                .collect(Collectors.toUnmodifiableSet());
    }

    private static boolean eligible(VariantPredicate predicate) {
        if (predicate.kind() != VariantPredicate.Kind.EQUALS || predicate.path().isEmpty()
                || predicate.path().stream().anyMatch(key -> !MEMBER.matcher(key).matches())) return false;
        Variant value = (Variant) predicate.expected().getValue();
        return value.basicType() == Header.BasicType.SHORT_STRING
                || (value.basicType() == Header.BasicType.PRIMITIVE && value.primitiveType() == Header.PrimitiveType.STRING);
    }

    public static Predicate<DataFile> predicate(Schema schema, List<VariantPredicate> predicates) {
        Expression expression = filter(schema, predicates);
        if (expression == Expressions.alwaysTrue()) return file -> true;
        var evaluator = new InclusiveMetricsEvaluator(schema, expression);
        return file -> {
            // Manifest readers return big-endian buffer views. Iceberg's extract evaluator
            // passes VARIANT bounds directly to the little-endian decoder. Duplicate the views
            // without mutating shared manifest metadata or copying their bytes.
            // Consume only path bounds. Root-level counts do not describe extracted paths.
            var metrics = new Metrics(file.recordCount(), null, null, null, null,
                    littleEndian(file.lowerBounds()), littleEndian(file.upperBounds()));
            var bounds = DataFiles.builder(PartitionSpec.unpartitioned()).withPath(file.location()).withFormat(file.format())
                    .withFileSizeInBytes(file.fileSizeInBytes()).withMetrics(metrics).build();
            return evaluator.eval(bounds);
        };
    }

    private static Map<Integer, ByteBuffer> littleEndian(Map<Integer, ByteBuffer> bounds) {
        return bounds == null ? null : bounds.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                entry -> entry.getValue().duplicate().order(ByteOrder.LITTLE_ENDIAN)));
    }

    static Expression filter(Schema schema, List<VariantPredicate> predicates) {
        Expression result = Expressions.alwaysTrue();
        for (VariantPredicate predicate : predicates) {
            if (!eligible(predicate)) continue;
            Variant value = (Variant) predicate.expected().getValue();
            var field = schema.columns().stream().filter(column -> column.fieldId() == predicate.column().getId()).findFirst();
            if (field.isEmpty() || !field.get().type().equals(Types.VariantType.get())
                    || !MEMBER.matcher(field.get().name()).matches()) continue;
            // Path bounds cover non-null values; they do not establish path completeness.
            // In particular MAY_EQUAL/range candidates cannot use this strict equality hint.
            result = Expressions.and(result, Expressions.equal(
                    Expressions.extract(field.get().name(), "$." + String.join(".", predicate.path()), "string"),
                    value.getStringUtf8()));
        }
        return result;
    }
}

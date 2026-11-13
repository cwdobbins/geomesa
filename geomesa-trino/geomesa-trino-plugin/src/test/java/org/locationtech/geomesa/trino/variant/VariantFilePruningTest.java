/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.variant;

import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.spi.expression.Constant;
import io.trino.spi.variant.Variant;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.expressions.PathUtil;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static io.trino.plugin.iceberg.ColumnIdentity.primitiveColumnIdentity;
import static io.trino.spi.type.VariantType.VARIANT;
import static org.assertj.core.api.Assertions.assertThat;

class VariantFilePruningTest {
    private static final IcebergColumnHandle DOC = IcebergColumnHandle.optional(primitiveColumnIdentity(1, "doc")).columnType(VARIANT).build();
    private static final Schema SCHEMA = new Schema(Types.NestedField.optional(1, "doc", Types.VariantType.get()));
    private static final List<String> PATH = List.of("itinerary", "departure", "name");

    @Test void fileBoundsAreInclusiveAndPathsAreCaseSensitive() {
        var predicate = predicate(VariantPredicate.Kind.EQUALS, PATH, Variant.ofString("match"));
        for (String[] bounds : List.of(new String[]{"a", "b"}, new String[]{"a", "match"}, new String[]{"match", "z"}, new String[]{"z", "z"})) {
            boolean retain = bounds[0].compareTo("match") <= 0 && bounds[1].compareTo("match") >= 0;
            assertThat(retains(predicate, PATH, Variants.of(bounds[0]), Variants.of(bounds[1]))).isEqualTo(retain);
        }
        assertThat(retains(predicate, List.of("itinerary", "departure", "Name"), Variants.of("a"), Variants.of("b"))).isTrue();
    }

    @Test void truncatedAndUnicodeStringBoundsRemainInclusive() {
        var longLiteral = predicate(VariantPredicate.Kind.EQUALS, PATH, Variant.ofString("a".repeat(100)));
        assertThat(retains(longLiteral, PATH, Variants.of("a".repeat(16)), Variants.of("a".repeat(15) + "b"))).isTrue();
        var unicode = predicate(VariantPredicate.Kind.EQUALS, PATH, Variant.ofString("😀"));
        assertThat(retains(unicode, PATH, Variants.of("\uE000"), Variants.of("😀"))).isTrue();
        assertThat(retains(unicode, PATH, Variants.of("é"), Variants.of("\uE000"))).isFalse();
    }

    @Test void missingAndIncompatibleBoundsKeepFiles() {
        var predicate = predicate(VariantPredicate.Kind.EQUALS, PATH, Variant.ofString("match"));
        assertThat(retains(predicate, PATH, null, null)).isTrue();
        assertThat(retains(predicate, PATH, Variants.of(1), Variants.of(2))).isTrue();
        assertThat(retains(predicate, PATH, null, Variants.of("z"))).isTrue();
        assertThat(retains(predicate, PATH, Variants.of("a"), null)).isTrue();
        // A known single-sided bound can still prove exclusion.
        assertThat(retains(predicate, PATH, Variants.of("z"), null)).isFalse();
        assertThat(retains(predicate, PATH, null, Variants.of("b"))).isFalse();
    }

    @Test void manifestByteOrderAndBufferPositionArePreserved() {
        var encoded = bound(PATH, Variants.of("z")).get(1);
        var buffer = ByteBuffer.allocate(encoded.remaining() + 4).putInt(123).put(encoded.duplicate());
        buffer.flip();
        buffer.position(4);
        var shared = buffer.asReadOnlyBuffer(); // manifest-style view, BIG_ENDIAN, nonzero position
        var file = DataFiles.builder(PartitionSpec.unpartitioned()).withPath("test.parquet").withFileSizeInBytes(100)
                .withMetrics(new Metrics(3L, null, null, null, null, Map.of(1, shared), Map.of(1, shared))).build();
        var matcher = VariantFilePruning.predicate(SCHEMA, List.of(predicate(VariantPredicate.Kind.EQUALS, PATH, Variant.ofString("match"))));
        assertThat(matcher.test(file)).isFalse();
        assertThat(matcher.test(file)).isFalse();
        assertThat(shared.order()).isEqualTo(java.nio.ByteOrder.BIG_ENDIAN);
        assertThat(shared.position()).isEqualTo(4);
        assertThat(shared.isReadOnly()).isTrue();
    }

    @Test void candidatesArraysNumericEqualityAndRootPathsAreNeverStrengthened() {
        for (var kind : VariantPredicate.Kind.values()) {
            if (kind == VariantPredicate.Kind.EQUALS) continue;
            assertThat(VariantFilePruning.filter(SCHEMA, List.of(predicate(kind, PATH, Variant.ofString("match")))))
                    .isSameAs(Expressions.alwaysTrue());
        }
        for (Variant value : List.of(Variant.ofInt(1), Variant.ofDouble(1), Variant.NULL_VALUE)) {
            assertThat(VariantFilePruning.filter(SCHEMA, List.of(predicate(VariantPredicate.Kind.EQUALS, PATH, value))))
                    .isSameAs(Expressions.alwaysTrue());
        }
        assertThat(VariantFilePruning.filter(SCHEMA, List.of(predicate(VariantPredicate.Kind.EQUALS, List.of(), Variant.ofString("match")))))
                .isSameAs(Expressions.alwaysTrue());
    }

    @Test void unsupportedLiteralKeysDoNotTurnIntoDifferentPaths() {
        for (String key : List.of("a.b", "*", "", "a'b", "[0]", "a\\b", "two words", "é", "1")) {
            assertThat(VariantFilePruning.filter(SCHEMA, List.of(predicate(VariantPredicate.Kind.EQUALS, List.of(key), Variant.ofString("match")))))
                    .as(key).isSameAs(Expressions.alwaysTrue());
        }
        for (String key : List.of("value", "typed_value", "metadata", "A_1")) {
            var predicate = predicate(VariantPredicate.Kind.EQUALS, List.of(key), Variant.ofString("match"));
            assertThat(retains(predicate, List.of(key), Variants.of("a"), Variants.of("b"))).isFalse();
        }
    }

    @Test void schemaIdentitySurvivesRenameButNotMissingOrChangedTypes() {
        var predicate = predicate(VariantPredicate.Kind.EQUALS, PATH, Variant.ofString("match"));
        var renamed = new Schema(Types.NestedField.optional(1, "renamed", Types.VariantType.get()));
        assertThat(VariantFilePruning.filter(renamed, List.of(predicate)).toString()).contains("renamed");
        for (var schema : List.of(new Schema(Types.NestedField.optional(2, "doc", Types.VariantType.get())),
                new Schema(Types.NestedField.optional(1, "doc", Types.StringType.get())),
                new Schema(Types.NestedField.optional(1, "a.b", Types.VariantType.get())))) {
            assertThat(VariantFilePruning.filter(schema, List.of(predicate))).isSameAs(Expressions.alwaysTrue());
        }
    }

    @Test void unsupportedConjunctDoesNotEraseSafeExactHint() {
        var exact = predicate(VariantPredicate.Kind.EQUALS, PATH, Variant.ofString("match"));
        var candidate = predicate(VariantPredicate.Kind.MAY_EQUAL, PATH, Variant.ofString("match"));
        assertThat(VariantFilePruning.filter(SCHEMA, List.of(exact, candidate)).toString())
                .isEqualTo(VariantFilePruning.filter(SCHEMA, List.of(exact)).toString());
    }

    private static VariantPredicate predicate(VariantPredicate.Kind kind, List<String> path, Variant value) {
        return new VariantPredicate(kind, DOC, path, List.of(), new Constant(value, VARIANT));
    }

    private static boolean retains(VariantPredicate predicate, List<String> path, VariantValue min, VariantValue max) {
        // Root null counts do not describe the extracted path. Unknown root counts must not
        // prevent exact pruning, and must never be used as proof for a conservative candidate.
        var metrics = new Metrics(3L, null, null, null, null, bound(path, min), bound(path, max));
        var file = DataFiles.builder(PartitionSpec.unpartitioned()).withPath("test.parquet").withFileSizeInBytes(100).withMetrics(metrics).build();
        return new InclusiveMetricsEvaluator(SCHEMA, VariantFilePruning.filter(SCHEMA, List.of(predicate))).eval(file);
    }

    private static Map<Integer, ByteBuffer> bound(List<String> path, VariantValue value) {
        if (value == null) return Map.of();
        String normalized = PathUtil.toNormalizedPath(path);
        var metadata = Variants.metadata(normalized);
        var object = Variants.object(metadata);
        object.put(normalized, value);
        return Map.of(1, Conversions.toByteBuffer(Types.VariantType.get(), org.apache.iceberg.variants.Variant.of(metadata, object)));
    }
}

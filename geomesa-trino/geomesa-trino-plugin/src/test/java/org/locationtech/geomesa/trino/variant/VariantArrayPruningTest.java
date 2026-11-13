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
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.iceberg.ColumnIdentity.primitiveColumnIdentity;
import static io.trino.spi.type.VariantType.VARIANT;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.assertj.core.api.Assertions.assertThat;

class VariantArrayPruningTest {
    private static final IcebergColumnHandle DOC = IcebergColumnHandle.optional(primitiveColumnIdentity(1, "doc")).columnType(VARIANT).build();
    private static final List<String> PREFIX = List.of("outer", "items");
    private static final List<String> SUFFIX = List.of("label", "name");

    @Test void repeatedCountsCanExceedRowsAndBoundsAreInclusive() {
        for (var kind : List.of(VariantPredicate.Kind.ANY_EQUALS, VariantPredicate.Kind.ANY_MAY_EQUAL)) {
            for (String target : List.of("a", "m", "z")) {
                var evaluator = evaluator(kind, PREFIX, SUFFIX, Variant.ofString(target));
                for (String[] bounds : List.of(new String[]{"a", "a"}, new String[]{"a", "m"}, new String[]{"m", "z"}, new String[]{"z", "z"})) {
                    boolean excluded = target.compareTo(bounds[0]) < 0 || target.compareTo(bounds[1]) > 0;
                    assertThat(evaluator.cannotMatchRowGroup("doc", 3, columns(PREFIX, SUFFIX, 3, 8, 0, bounds[0], bounds[1])))
                            .as("%s %s [%s,%s]", kind, target, bounds[0], bounds[1]).isEqualTo(excluded);
                }
            }
        }
    }

    @Test void everyParentAndRepeatedFallbackMustAccountForAllEntries() {
        var proven = columns(PREFIX, SUFFIX, 3, 8, 0, "a", "b");
        for (var kind : List.of(VariantPredicate.Kind.ANY_EQUALS, VariantPredicate.Kind.ANY_MAY_EQUAL)) {
            var evaluator = evaluator(kind, PREFIX, SUFFIX, Variant.ofString("z"));
            assertThat(evaluator.cannotMatchRowGroup("doc", 3, proven)).isTrue();
            for (int i = 0; i < proven.size() - 1; i++) {
                var old = proven.get(i);
                for (long nulls : new long[]{old.getValueCount() - 1, -1}) {
                    var copy = new ArrayList<>(proven);
                    copy.set(i, chunk(List.of(old.getPath().toArray()), old.getValueCount(), nulls, null, null));
                    assertThat(evaluator.cannotMatchRowGroup("doc", 3, copy)).as("%s", old.getPath()).isFalse();
                }
                var missing = new ArrayList<>(proven);
                missing.remove(i);
                assertThat(evaluator.cannotMatchRowGroup("doc", 3, missing)).isFalse();
            }
            var wrongRepeatedCount = new ArrayList<>(proven);
            var element = wrongRepeatedCount.get(3);
            wrongRepeatedCount.set(3, chunk(List.of(element.getPath().toArray()), 3, 3, null, null));
            assertThat(evaluator.cannotMatchRowGroup("doc", 3, wrongRepeatedCount)).isFalse();
        }
    }

    @Test void emptyMissingAndNullEntriesDistinguishExactFromCandidate() {
        var exact = evaluator(VariantPredicate.Kind.ANY_EQUALS, PREFIX, SUFFIX, Variant.ofString("z"));
        var candidate = evaluator(VariantPredicate.Kind.ANY_MAY_EQUAL, PREFIX, SUFFIX, Variant.ofString("z"));
        // One null placeholder may represent an empty array or a missing element path.
        for (long nulls : new long[]{1, -1}) {
            var stats = columns(PREFIX, SUFFIX, 3, 8, nulls, "a", "b");
            assertThat(exact.cannotMatchRowGroup("doc", 3, stats)).isTrue();
            assertThat(candidate.cannotMatchRowGroup("doc", 3, stats)).isFalse();
        }
        var empty = Variant.ofObject(Map.of(utf8Slice("outer"), Variant.ofObject(Map.of(utf8Slice("items"), Variant.ofArray(List.of())))));
        assertThat(predicate(VariantPredicate.Kind.ANY_EQUALS, PREFIX, SUFFIX, Variant.ofString("z")).test(empty)).isFalse();
        assertThat(predicate(VariantPredicate.Kind.ANY_MAY_EQUAL, PREFIX, SUFFIX, Variant.ofString("z")).test(empty)).isTrue();
    }

    @Test void rootArraysScalarElementsAndReservedKeysUseTheirLiteralPaths() {
        for (var prefix : List.of(List.<String>of(), List.of("items"), List.of("typed_value", "list"))) {
            for (var suffix : List.of(List.<String>of(), List.of("value"), SUFFIX)) {
                for (var kind : List.of(VariantPredicate.Kind.ANY_EQUALS, VariantPredicate.Kind.ANY_MAY_EQUAL)) {
                    assertThat(evaluator(kind, prefix, suffix, Variant.ofString("z"))
                            .cannotMatchRowGroup("doc", 3, columns(prefix, suffix, 3, 8, 0, "a", "b"))).isTrue();
                }
            }
        }
    }

    @Test void unknownInvalidOrUnsupportedMetadataKeepsGroups() {
        var evaluator = evaluator(VariantPredicate.Kind.ANY_MAY_EQUAL, PREFIX, SUFFIX, Variant.ofString("z"));
        assertThat(evaluator.cannotMatchRowGroup("doc", 3, List.of())).isFalse();
        assertThat(evaluator.cannotMatchRowGroup("doc", 0, columns(PREFIX, SUFFIX, 3, 8, 0, "a", "b"))).isFalse();
        assertThat(evaluator.cannotMatchRowGroup("doc", 3, columns(PREFIX, SUFFIX, 3, 2, 0, "a", "b"))).isFalse();
        assertThat(evaluator.cannotMatchRowGroup("doc", 3, columns(PREFIX, SUFFIX, 3, 8, 0, "b", "a"))).isFalse();
        assertThat(evaluator.cannotMatchRowGroup("doc", 3, columns(PREFIX, SUFFIX, 3, 8, 8, null, null))).isFalse();
        for (Variant expected : List.of(Variant.ofInt(10), Variant.ofDouble(10), Variant.NULL_VALUE)) {
            assertThat(evaluator(VariantPredicate.Kind.ANY_EQUALS, PREFIX, SUFFIX, expected)
                    .cannotMatchRowGroup("doc", 3, columns(PREFIX, SUFFIX, 3, 8, 0, "a", "b"))).isFalse();
        }
    }

    @Test void separateArrayPredicatesNeverAssumeTheSameElementMatches() {
        var name = predicate(VariantPredicate.Kind.ANY_EQUALS, List.of("items"), List.of("name"), Variant.ofString("target"));
        var code = predicate(VariantPredicate.Kind.ANY_EQUALS, List.of("items"), List.of("code"), Variant.ofString("target"));
        var document = Variant.ofObject(Map.of(utf8Slice("items"), Variant.ofArray(List.of(
                Variant.ofObject(Map.of(utf8Slice("name"), Variant.ofString("target"), utf8Slice("code"), Variant.ofString("other"))),
                Variant.ofObject(Map.of(utf8Slice("name"), Variant.ofString("other"), utf8Slice("code"), Variant.ofString("target")))))));
        assertThat(name.test(document) && code.test(document)).isTrue();
        var columns = new ArrayList<>(columns(List.of("items"), List.of("name"), 1, 2, 0, "other", "target"));
        var other = columns(List.of("items"), List.of("code"), 1, 2, 0, "other", "target");
        for (var column : other) {
            if (columns.stream().noneMatch(existing -> existing.getPath().equals(column.getPath()))) columns.add(column);
        }
        assertThat(new VariantTypedEvaluator(List.of(name, code)).cannotMatchRowGroup("doc", 1, columns)).isFalse();
    }

    private static VariantPredicate predicate(VariantPredicate.Kind kind, List<String> prefix, List<String> suffix, Variant expected) {
        return new VariantPredicate(kind, DOC, prefix, suffix, new Constant(expected, VARIANT));
    }
    private static VariantTypedEvaluator evaluator(VariantPredicate.Kind kind, List<String> prefix, List<String> suffix, Variant expected) {
        return new VariantTypedEvaluator(List.of(predicate(kind, prefix, suffix, expected)));
    }
    private static List<ColumnChunkMetadata> columns(List<String> prefix, List<String> suffix, long rows, long entries,
            long nulls, String min, String max) {
        var result = new ArrayList<ColumnChunkMetadata>();
        var node = new ArrayList<>(List.of("doc"));
        result.add(fallback(node, rows));
        for (String key : prefix) { node.addAll(List.of("typed_value", key)); result.add(fallback(node, rows)); }
        node.addAll(List.of("typed_value", "list", "element"));
        result.add(fallback(node, entries));
        for (String key : suffix) { node.addAll(List.of("typed_value", key)); result.add(fallback(node, entries)); }
        node.add("typed_value");
        result.add(chunk(node, entries, nulls, min, max));
        return result;
    }
    private static ColumnChunkMetadata fallback(List<String> node, long count) {
        var path = new ArrayList<>(node); path.add("value");
        return chunk(path, count, count, null, null);
    }
    private static ColumnChunkMetadata chunk(List<String> path, long count, long nulls, String min, String max) {
        var type = Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("value");
        var stats = new BinaryStatistics();
        stats.setNumNulls(nulls);
        if (min != null && max != null) stats.setMinMax(Binary.fromString(min), Binary.fromString(max));
        return ColumnChunkMetadata.get(ColumnPath.get(path.toArray(String[]::new)), type, UNCOMPRESSED, null, Set.of(), stats, 0, 0, count, 0, 0);
    }
}

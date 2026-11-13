/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.variant;

import io.trino.parquet.VariantPath;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.parquet.reader.VariantBatchAccess;
import io.trino.parquet.reader.VariantPathProbe;
import io.trino.parquet.reader.VariantPredicateEvaluator.Decision;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.spi.block.Block;
import io.trino.spi.expression.Constant;
import io.trino.spi.type.Type;
import io.trino.spi.variant.Variant;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.iceberg.ColumnIdentity.primitiveColumnIdentity;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VariantType.VARIANT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;

class VariantTypedEvaluatorTest {
    private static final IcebergColumnHandle DOC = IcebergColumnHandle.optional(primitiveColumnIdentity(1, "doc")).columnType(VARIANT).build();

    @Test
    void skipsActualSortedRowGroupsAndKeepsRandomAndMixedGroups(@TempDir Path tempDir)
            throws IOException
    {
        VariantTypedEvaluator evaluator = new VariantTypedEvaluator(List.of(new VariantPredicate(
                VariantPredicate.Kind.EQUALS,
                DOC,
                List.of("name"),
                List.of(),
                new Constant(Variant.ofString("m-target"), VARIANT))));
        List<String> values = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            values.add((i < 100 ? "a" : "z") + "%04d".formatted(i));
        }

        Path sortedFile = tempDir.resolve("sorted.parquet");
        writeStatsFixture(sortedFile, values, -1);
        List<BlockMetaData> sortedGroups = rowGroups(sortedFile);
        long sortedSkipped = sortedGroups.stream()
                .filter(group -> evaluator.cannotMatchRowGroup("doc", group.getRowCount(), columns(group)))
                .count();
        assertThat(sortedGroups).hasSize(2);
        assertThat(sortedSkipped).isEqualTo(2);

        List<String> randomValues = new ArrayList<>(values);
        Collections.shuffle(randomValues, new Random(19));
        Path randomFile = tempDir.resolve("random.parquet");
        writeStatsFixture(randomFile, randomValues, -1);
        List<BlockMetaData> randomGroups = rowGroups(randomFile);
        long randomSkipped = randomGroups.stream()
                .filter(group -> evaluator.cannotMatchRowGroup("doc", group.getRowCount(), columns(group)))
                .count();
        assertThat(randomGroups).hasSize(2);
        assertThat(randomSkipped).isZero();

        Path mixedFile = tempDir.resolve("mixed.parquet");
        writeStatsFixture(mixedFile, values, 150);
        List<BlockMetaData> mixedGroups = rowGroups(mixedFile);
        long mixedSkipped = mixedGroups.stream()
                .filter(group -> evaluator.cannotMatchRowGroup("doc", group.getRowCount(), columns(group)))
                .count();
        assertThat(mixedGroups).hasSize(2);
        assertThat(mixedSkipped).isEqualTo(1);
    }

    private static void writeStatsFixture(Path file, List<String> values, int mismatchIndex)
            throws IOException
    {
        MessageType schema = Types.buildMessage()
                .required(PrimitiveType.PrimitiveTypeName.INT32).named("id")
                .optionalGroup().as(LogicalTypeAnnotation.variantType((byte) 1))
                .optional(PrimitiveType.PrimitiveTypeName.BINARY).named("metadata")
                .optional(PrimitiveType.PrimitiveTypeName.BINARY).named("value")
                .optionalGroup()
                .optionalGroup()
                .optional(PrimitiveType.PrimitiveTypeName.BINARY).named("value")
                .optional(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("typed_value")
                .named("name")
                .optional(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("padding")
                .named("typed_value")
                .named("doc")
                .named("root");
        Random padding = new Random(41);
        try (var writer = ExampleParquetWriter.builder(new org.apache.parquet.io.LocalOutputFile(file))
                .withType(schema)
                .withRowGroupSize(4096)
                .withPageSize(1024)
                .build()) {
            for (int i = 0; i < values.size(); i++) {
                SimpleGroup row = new SimpleGroup(schema);
                row.append("id", i);
                Group document = row.addGroup("doc");
                document.append("metadata", Binary.fromConstantByteArray(new byte[] {0}));
                Group typed = document.addGroup("typed_value");
                Group name = typed.addGroup("name");
                if (i == mismatchIndex) {
                    document.append("value", Binary.fromConstantByteArray(new byte[] {1}));
                    name.append("value", Binary.fromConstantByteArray(new byte[] {1}));
                }
                else {
                    name.append("typed_value", Binary.fromString(values.get(i)));
                }
                byte[] bytes = new byte[768];
                padding.nextBytes(bytes);
                typed.append("padding", Binary.fromConstantByteArray(bytes));
                writer.write(row);
            }
        }
    }

    private static List<BlockMetaData> rowGroups(Path file)
            throws IOException
    {
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            return List.copyOf(reader.getFooter().getBlocks());
        }
    }

    private static List<ColumnChunkMetadata> columns(BlockMetaData block)
    {
        return block.getColumns().stream()
                .map(column -> ColumnChunkMetadata.get(
                        column.getPath(),
                        column.getPrimitiveType(),
                        column.getCodec(),
                        column.getEncodingStats(),
                        column.getEncodings(),
                        column.getStatistics(),
                        column.getFirstDataPageOffset(),
                        column.getDictionaryPageOffset(),
                        column.getValueCount(),
                        column.getTotalSize(),
                        column.getTotalUncompressedSize()))
                .toList();
    }

    @Test void skipsOnlyWhenFallbackIsProvenNullAndTypedStringRangeExcludesLiteral() {
        VariantTypedEvaluator evaluator = new VariantTypedEvaluator(List.of(equalsPredicate(Variant.ofString("middle"))));
        List<ColumnChunkMetadata> columns = List.of(
                chunk(List.of("doc", "value"), 10, null, null, true, 10),
                chunk(List.of("doc", "typed_value", "a", "value"), 10, null, null, true, 10),
                chunk(List.of("doc", "typed_value", "a", "typed_value"), 10, "alpha", "omega", false, 0));
        assertThat(evaluator.cannotMatchRowGroup("doc", 10, columns)).isFalse();

        columns = List.of(
                chunk(List.of("doc", "value"), 10, null, null, true, 10),
                chunk(List.of("doc", "typed_value", "a", "value"), 10, null, null, true, 10),
                chunk(List.of("doc", "typed_value", "a", "typed_value"), 10, "zulu", "zzzz", false, 0));
        assertThat(evaluator.cannotMatchRowGroup("doc", 10, columns)).isTrue();
    }

    @Test void unknownMixedAndPlainStatisticsKeepTheRowGroup() {
        VariantTypedEvaluator evaluator = new VariantTypedEvaluator(List.of(equalsPredicate(Variant.ofString("hit"))));
        List<ColumnChunkMetadata> provenDisjoint = List.of(
                chunk(List.of("doc", "value"), 10, null, null, true, 10),
                chunk(List.of("doc", "typed_value", "a", "value"), 10, null, null, true, 10),
                chunk(List.of("doc", "typed_value", "a", "typed_value"), 10, "x", "z", false, 0));
        assertThat(evaluator.cannotMatchRowGroup("doc", 10, provenDisjoint)).isTrue();
        assertThat(evaluator.cannotMatchRowGroup("doc", 10, List.of(
                chunk(List.of("doc", "value"), 10, null, null, false, 10),
                provenDisjoint.get(1), provenDisjoint.get(2)))).isFalse();
        assertThat(evaluator.cannotMatchRowGroup("doc", 10, List.of(
                chunk(List.of("doc", "value"), 10, null, null, true, 9),
                provenDisjoint.get(1), provenDisjoint.get(2)))).isFalse();
        assertThat(evaluator.cannotMatchRowGroup("doc", 10, List.of(provenDisjoint.get(2)))).isFalse();
        assertThat(evaluator.cannotMatchRowGroup("doc", 10, List.of(provenDisjoint.getFirst(),
                chunk(List.of("doc", "typed_value", "a", "value"), 10, null, null, true, 9),
                provenDisjoint.get(2)))).isFalse();
    }

    private static ColumnChunkMetadata chunk(List<String> path, long values, String min, String max, boolean nullsSet, long nullCount) {
        PrimitiveType primitiveType = Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType()).named("value");
        Statistics<?> statistics = Statistics.createStats(primitiveType);
        if (min != null && max != null) {
            ((BinaryStatistics) statistics).setMinMax(Binary.fromString(min), Binary.fromString(max));
        }
        statistics.setNumNulls(nullsSet ? nullCount : -1);
        return ColumnChunkMetadata.get(
                ColumnPath.get(path.toArray(String[]::new)),
                primitiveType,
                UNCOMPRESSED,
                null,
                java.util.Set.of(),
                statistics,
                0,
                0,
                values,
                0,
                0);
    }

    @Test void fractionalRangeProbesAvoidReconstructionWithoutChangingExactPredicates() {
        var probes = new ArrayList<VariantPathProbe>();
        for (int precision : List.of(9, 18, 38)) {
            var type = io.trino.spi.type.DecimalType.createDecimalType(precision, 4);
            var builder = type.createBlockBuilder(null, 1);
            if (type.isShort()) type.writeLong(builder, 12500L);
            else io.trino.spi.type.Decimals.writeBigDecimal(type, builder, new java.math.BigDecimal("1.2500"));
            probes.add(typed(type, builder.build()));
        }
        for (double value : List.of(1.25, -0.0, Double.NaN, Double.POSITIVE_INFINITY, Double.MIN_VALUE)) {
            probes.add(typed(DOUBLE, writeNativeValue(DOUBLE, value)));
        }
        var real = io.trino.spi.type.RealType.REAL;
        probes.add(typed(real, writeNativeValue(real, (long) Float.floatToRawIntBits(0.1f))));
        for (var probe : probes) {
            var document = object(probe.value());
            for (var kind : List.of(VariantPredicate.Kind.MAY_GREATER_THAN, VariantPredicate.Kind.MAY_GREATER_THAN_OR_EQUAL,
                    VariantPredicate.Kind.MAY_LESS_THAN, VariantPredicate.Kind.MAY_LESS_THAN_OR_EQUAL)) {
                for (var bound : List.of(Variant.ofDouble(1.25), Variant.ofFloat(0.1f), Variant.ofDecimal(new java.math.BigDecimal("1.2500")))) {
                    var predicate = new VariantPredicate(kind, DOC, List.of("a"), List.of(), new Constant(bound, VARIANT));
                    var references = new AtomicInteger();
                    var evaluator = new VariantTypedEvaluator(List.of(predicate));
                    assertThat(evaluator.evaluate(batch(List.of(probe), document, references), 0))
                            .isEqualTo(predicate.test(document) ? Decision.TRUE : Decision.FALSE);
                    assertThat(references).hasValue(0);
                    assertThat(evaluator.directRows()).isEqualTo(1);
                    assertThat(evaluator.evaluate(batch(List.of(status(VariantPathProbe.State.UNRESOLVED)), document, references), 0))
                            .isEqualTo(predicate.test(document) ? Decision.TRUE : Decision.FALSE);
                    assertThat(references).hasValue(1);
                }
            }
        }
    }

    @Test void integerRangeProbesAndReferenceFallbackAgree() {
        for (var kind : List.of(VariantPredicate.Kind.MAY_GREATER_THAN, VariantPredicate.Kind.MAY_GREATER_THAN_OR_EQUAL,
                VariantPredicate.Kind.MAY_LESS_THAN, VariantPredicate.Kind.MAY_LESS_THAN_OR_EQUAL)) {
            for (Type type : List.of(TINYINT, SMALLINT, INTEGER, BIGINT)) {
                var bound = typed(type, writeNativeValue(type, 10L)).value();
                var predicate = new VariantPredicate(kind, DOC, List.of("a"), List.of(), new Constant(bound, VARIANT));
                for (long value : List.of(9L, 10L, 11L)) {
                    var probe = typed(type, writeNativeValue(type, value));
                    var document = object(probe.value());
                    var references = new AtomicInteger();
                    var evaluator = new VariantTypedEvaluator(List.of(predicate));
                    assertThat(evaluator.evaluate(batch(List.of(probe), document, references), 0))
                            .isEqualTo(predicate.test(document) ? Decision.TRUE : Decision.FALSE);
                    assertThat(references).hasValue(0);
                    assertThat(evaluator.evaluate(batch(List.of(status(VariantPathProbe.State.UNRESOLVED)), document, references), 0))
                            .isEqualTo(predicate.test(document) ? Decision.TRUE : Decision.FALSE);
                    assertThat(references).hasValue(1);
                }
                for (var state : List.of(VariantPathProbe.State.SQL_NULL, VariantPathProbe.State.MISSING, VariantPathProbe.State.WRONG_SHAPE)) {
                    assertThat(new VariantTypedEvaluator(List.of(predicate)).evaluate(batch(List.of(status(state)), null, new AtomicInteger()), 0))
                            .isEqualTo(Decision.TRUE);
                }
                assertThat(new VariantTypedEvaluator(List.of(predicate)).evaluate(batch(List.of(), null, new AtomicInteger()), 0))
                        .isEqualTo(Decision.TRUE);
            }
        }
    }

    @Test void integerWidthsAndLargeExactIntegersUseSpiEquality() {
        for (Type type : List.of(TINYINT, SMALLINT, INTEGER, BIGINT)) {
            for (long value : (type == BIGINT ? List.of(7L, (1L << 53) + 1, Long.MAX_VALUE) : List.of(7L, -8L))) {
                Block block = writeNativeValue(type, value);
                for (Variant expected : List.of(Variant.ofLong(value), Variant.ofLong(value - 1), Variant.ofDouble((double) value), Variant.NULL_VALUE)) {
                    VariantPathProbe probe = typed(type, block);
                    Variant document = object(probe.value());
                    VariantPredicate predicate = equalsPredicate(expected);
                    AtomicInteger references = new AtomicInteger();
                    VariantTypedEvaluator evaluator = new VariantTypedEvaluator(List.of(predicate));
                    assertThat(evaluator.evaluate(batch(List.of(probe), document, references), 0))
                        .isEqualTo(predicate.test(document) ? Decision.TRUE : Decision.FALSE);
                    assertThat(references).hasValue(0);
                    assertThat(evaluator.directRows()).isEqualTo(1);
                    assertThat(evaluator.fallbackRows()).isZero();
                }
            }
        }
    }

    @Test void booleansAndExactStringsStayDirect() {
        for (VariantPathProbe probe : List.of(typed(BOOLEAN, writeNativeValue(BOOLEAN, true)),
                typed(VARCHAR, writeNativeValue(VARCHAR, utf8Slice("value.typed_value.é漢字"))))) {
            for (Variant expected : List.of(probe.value(), Variant.ofLong(7), Variant.ofString(utf8Slice("other")))) {
                Variant document = object(probe.value());
                VariantPredicate predicate = equalsPredicate(expected);
                AtomicInteger references = new AtomicInteger();
                assertThat(new VariantTypedEvaluator(List.of(predicate)).evaluate(batch(List.of(probe), document, references), 0))
                    .isEqualTo(predicate.test(document) ? Decision.TRUE : Decision.FALSE);
                assertThat(references).hasValue(0);
            }
        }
    }

    @Test void unresolvedAndUnprovedPrimitiveTypesUseCompleteReference() {
        for (double value : List.of(Double.NaN, Double.POSITIVE_INFINITY, -0.0, 7.0)) {
            Variant document = object(Variant.ofDouble(value));
            VariantPredicate predicate = equalsPredicate(Variant.ofDouble(value));
            for (VariantPathProbe probe : List.of(typed(DOUBLE, writeNativeValue(DOUBLE, value)), status(VariantPathProbe.State.UNRESOLVED))) {
                AtomicInteger references = new AtomicInteger();
                VariantTypedEvaluator evaluator = new VariantTypedEvaluator(List.of(predicate));
                Decision expected = predicate.test(document) ? Decision.TRUE : Decision.FALSE;
                assertThat(evaluator.evaluate(batch(List.of(probe), document, references), 0)).isEqualTo(expected);
                assertThat(references).hasValue(1);
                assertThat(evaluator.fallbackRows()).isEqualTo(1);
                assertThat(evaluator.directRows()).isZero();
                assertThat(evaluator.evaluateReference(document)).isEqualTo(expected);
            }
        }
        for (var state : List.of(VariantPathProbe.State.SQL_NULL, VariantPathProbe.State.MISSING, VariantPathProbe.State.WRONG_SHAPE)) {
            AtomicInteger references = new AtomicInteger();
            assertThat(new VariantTypedEvaluator(List.of(equalsPredicate(Variant.NULL_VALUE)))
                .evaluate(batch(List.of(status(state)), null, references), 0)).isEqualTo(Decision.FALSE);
            assertThat(references).hasValue(0);
        }
    }

    @Test void listScopesRemainIndependentAndFallbackIsAuthoritative() {
        VariantPredicate a = new VariantPredicate(VariantPredicate.Kind.ANY_EQUALS, DOC, List.of("items"), List.of("a"), new Constant(Variant.ofLong(7), VARIANT));
        VariantPredicate b = new VariantPredicate(VariantPredicate.Kind.ANY_EQUALS, DOC, List.of("items"), List.of("b"), new Constant(Variant.ofLong(8), VARIANT));
        Variant document = Variant.ofObject(Map.of(utf8Slice("items"), Variant.ofArray(List.of(
            object(Variant.ofLong(7)), Variant.ofObject(Map.of(utf8Slice("b"), Variant.ofLong(8)))))));
        AtomicInteger references = new AtomicInteger();
        VariantBatchAccess batch = new VariantBatchAccess() {
            @Override public List<VariantPathProbe> probe(VariantPath path, int position) {
                return path.equals(a.demand()) ? List.of(typed(BIGINT, writeNativeValue(BIGINT, 7L)), status(VariantPathProbe.State.MISSING))
                    : List.of(status(VariantPathProbe.State.MISSING), typed(BIGINT, writeNativeValue(BIGINT, 8L)));
            }
            @Override public Optional<Variant> reference(int position) { references.incrementAndGet(); return Optional.of(document); }
        };
        assertThat(new VariantTypedEvaluator(List.of(a, b)).evaluate(batch, 0)).isEqualTo(Decision.TRUE);
        assertThat(references).hasValue(0);
        assertThat(new VariantTypedEvaluator(List.of(a)).evaluate(batch(List.of(), document, references), 0)).isEqualTo(Decision.FALSE);
        // A typed match plus unresolved data still runs the complete reference path.
        assertThat(new VariantTypedEvaluator(List.of(a)).evaluate(batch(List.of(typed(BIGINT, writeNativeValue(BIGINT, 7L)),
            status(VariantPathProbe.State.UNRESOLVED)), document, references), 0)).isEqualTo(Decision.TRUE);
        assertThat(references).hasValue(1);
    }


    @Test void conservativePredicatesKeepMissingEmptyAndNonstringObservations() {
        for (var kind : List.of(VariantPredicate.Kind.MAY_EQUAL, VariantPredicate.Kind.ANY_MAY_EQUAL)) {
            var predicate = new VariantPredicate(kind, DOC, List.of("a"), List.of(), new Constant(Variant.ofString("001"), VARIANT));
            for (var state : List.of(VariantPathProbe.State.SQL_NULL, VariantPathProbe.State.MISSING, VariantPathProbe.State.WRONG_SHAPE)) {
                assertThat(new VariantTypedEvaluator(List.of(predicate)).evaluate(batch(List.of(status(state)), null, new AtomicInteger()), 0))
                    .isEqualTo(Decision.TRUE);
            }
            for (VariantPathProbe probe : List.of(typed(VARCHAR, writeNativeValue(VARCHAR, utf8Slice("001"))),
                    typed(VARCHAR, writeNativeValue(VARCHAR, utf8Slice("other"))), typed(BIGINT, writeNativeValue(BIGINT, 1L)))) {
                Variant value = probe.value();
                Variant document = object(kind.array() ? Variant.ofArray(List.of(value)) : value);
                AtomicInteger references = new AtomicInteger();
                var evaluator = new VariantTypedEvaluator(List.of(predicate));
                assertThat(evaluator.evaluate(batch(List.of(probe), document, references), 0))
                    .isEqualTo(predicate.test(document) ? Decision.TRUE : Decision.FALSE);
                assertThat(references).hasValue(0);
            }
            AtomicInteger references = new AtomicInteger();
            Variant document = object(kind.array() ? Variant.ofArray(List.of(Variant.ofString("no"))) : Variant.ofString("no"));
            assertThat(new VariantTypedEvaluator(List.of(predicate)).evaluate(batch(List.of(status(VariantPathProbe.State.UNRESOLVED)), document, references), 0))
                .isEqualTo(Decision.FALSE);
            assertThat(references).hasValue(1);
        }
        var predicate = new VariantPredicate(VariantPredicate.Kind.ANY_MAY_EQUAL, DOC, List.of("a"), List.of(), new Constant(Variant.ofString("x"), VARIANT));
        assertThat(new VariantTypedEvaluator(List.of(predicate)).evaluate(batch(List.of(), object(Variant.ofArray(List.of())), new AtomicInteger()), 0)).isEqualTo(Decision.TRUE);
    }

    private static VariantPredicate equalsPredicate(Variant expected) {
        return new VariantPredicate(VariantPredicate.Kind.EQUALS, DOC, List.of("a"), List.of(), new Constant(expected, VARIANT));
    }
    private static Variant object(Variant value) { return Variant.ofObject(Map.of(utf8Slice("a"), value)); }
    private static VariantPathProbe typed(Type type, Block block) {
        return new VariantPathProbe(() -> {}, VariantPathProbe.State.TYPED_VALUE, "doc", type, block, 0);
    }
    private static VariantPathProbe status(VariantPathProbe.State state) {
        return new VariantPathProbe(() -> {}, state, "doc", null, null, 0);
    }
    private static VariantBatchAccess batch(List<VariantPathProbe> probes, Variant document, AtomicInteger references) {
        return new VariantBatchAccess() {
            @Override public List<VariantPathProbe> probe(VariantPath path, int position) { return probes; }
            @Override public Optional<Variant> reference(int position) { references.incrementAndGet(); return Optional.ofNullable(document); }
        };
    }
}

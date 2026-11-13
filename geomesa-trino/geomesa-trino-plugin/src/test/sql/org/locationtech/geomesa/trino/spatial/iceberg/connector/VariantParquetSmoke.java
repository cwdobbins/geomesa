/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import io.trino.filesystem.Location;
import io.trino.filesystem.memory.MemoryFileSystem;
import io.trino.parquet.VariantPath;
import io.trino.parquet.cache.ParquetFooterCache;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.plugin.iceberg.*;
import io.trino.plugin.iceberg.delete.DeleteFile;
import io.trino.plugin.iceberg.encryption.IcebergEncryptionConfig;
import io.trino.plugin.iceberg.fileio.ForwardingFileIoFactory;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.*;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.variant.Variant;
import io.trino.testing.TestingConnectorSession;
import org.apache.iceberg.*;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.types.Types;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.locationtech.geomesa.trino.variant.VariantPredicateCompiler;
import org.locationtech.geomesa.trino.variant.VariantPushdownConfig;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Executors;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VariantType.VARIANT;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.*;
import static org.locationtech.geomesa.trino.spatial.iceberg.connector.VariantTestFixtures.*;

/** Real Parquet/Iceberg read, including unshredded fallback, security, deletes and whole/pruned outputs. */
public final class VariantParquetSmoke {
    private static final int ROWS = 320;
    private static final IcebergColumnHandle GEOM = IcebergColumnHandle.optional(ColumnIdentity.primitiveColumnIdentity(4, "geom")).columnType(io.trino.spi.type.VarbinaryType.VARBINARY).build();
    private static final IcebergColumnHandle X = IcebergColumnHandle.optional(ColumnIdentity.primitiveColumnIdentity(5, "x")).columnType(io.trino.spi.type.RealType.REAL).build();
    private static final IcebergColumnHandle Y = IcebergColumnHandle.optional(ColumnIdentity.primitiveColumnIdentity(6, "y")).columnType(io.trino.spi.type.RealType.REAL).build();
    private static final Schema SCHEMA = new Schema(Types.NestedField.optional(1, "doc", Types.VariantType.get()),
        Types.NestedField.required(2, "id", Types.LongType.get()), Types.NestedField.optional(3, "__vis__", Types.StringType.get()),
        Types.NestedField.optional(4, "geom", Types.BinaryType.get()), Types.NestedField.optional(5, "x", Types.FloatType.get()), Types.NestedField.optional(6, "y", Types.FloatType.get()));
    private VariantParquetSmoke() {}

    public static void main(String[] args) throws Exception {
        Path directory = Files.createTempDirectory("variant-parquet-smoke-");
        try (var executor = Executors.newSingleThreadExecutor()) {
            var fs = new MemoryFileSystem();
            var factory = new CustomFooterProviderFactory((identity, properties) -> fs,
                new ForwardingFileIoFactory(executor), new FileFormatDataSourceStats(), new OrcReaderConfig(),
                new ParquetReaderConfig(), TESTING_TYPE_MANAGER, ParquetFooterCache.noop(),
                (types, nulls, size) -> { throw new AssertionError("Unexpected equality delete"); }, new IcebergConfig(),
                (keys, properties) -> PlaintextEncryptionManager.instance());
            var properties = new ArrayList<io.trino.spi.session.PropertyMetadata<?>>(new IcebergSessionProperties(
                new IcebergConfig(), new IcebergEncryptionConfig(), new OrcReaderConfig(), new OrcWriterConfig(),
                new ParquetReaderConfig(), new ParquetWriterConfig()).getSessionProperties());
            properties.addAll(VariantPushdownConfig.DEFAULT.sessionProperties());
            var predicate = VariantPredicateCompiler.compile(equalsCall("d", Variant.ofLong(7), "a"), ASSIGNMENTS).orElseThrow();
            for (boolean shredded : List.of(false, true)) {
                Path file = directory.resolve(shredded ? "shredded.parquet" : "unshredded.parquet");
                write(file, shredded);
                Location location = Location.of("memory:///" + file.getFileName());
                fs.newOutputFile(location).createOrOverwrite(Files.readAllBytes(file));
                Path deletes = directory.resolve(shredded ? "shredded-deletes.parquet" : "unshredded-deletes.parquet");
                writeDeletes(deletes, location.toString());
                Location deleteLocation = Location.of("memory:///" + deletes.getFileName());
                fs.newOutputFile(deleteLocation).createOrOverwrite(Files.readAllBytes(deletes));
                var delete = new DeleteFile(FileContent.POSITION_DELETES, deleteLocation.toString(), FileFormat.PARQUET,
                    1, Files.size(deletes), List.of(), OptionalLong.of(0), OptionalLong.of(0), 2,
                    OptionalLong.empty(), Optional.empty(), Optional.empty());
                var table = new IcebergTableHandle("s", "docs", TableType.DATA, OptionalLong.empty(), SchemaParser.toJson(SCHEMA),
                    OptionalInt.of(0), Map.of(0, PartitionSpecParser.toJson(PartitionSpec.unpartitioned())), 3,
                    TupleDomain.all(), TupleDomain.all(), OptionalLong.empty(), Set.of(ID), Optional.empty(), "memory:///",
                    Map.of(), Optional.empty(), false, Optional.empty(), Set.of(), Optional.empty());
                for (boolean useDeletes : List.of(false, true)) {
                    var split = new IcebergSplit(location.toString(), 0, Files.size(file), Files.size(file), ROWS,
                        IcebergFileFormat.PARQUET, 0, List.of(), useDeletes ? List.of(delete) : List.of(),
                        SplitWeight.standard(), TupleDomain.all(), Optional.empty(), OptionalLong.of(1), OptionalLong.empty(), Optional.empty());
                    for (boolean footer : List.of(false, true)) {
                        for (int output : List.of(0, 1, 2, 3)) {
                            List<ColumnHandle> columns = switch (output) {
                                case 0 -> List.of(); // predicate-only count(*)
                                case 1 -> List.of(ID);
                                case 2 -> List.of(ID, DOC); // whole output must retain b
                                default -> List.of(ID, DOC.withVariantPaths(Set.of(VariantPath.keys("a"))));
                            };
                            for (boolean spatial : List.of(false, true)) for (boolean selective : List.of(false, true)) {
                            ConnectorTableHandle inner = spatial ? new SpatialTableHandle(table, GEOM, List.of(X, Y, X, Y), -1, -1, 7.5, 1) : table;
                            var handle = new VariantTableHandle(new VisibilityTableHandle(inner, VIS, List.of("A")), List.of(predicate));
                            int rows = 0;
                            List<Long> ids = new ArrayList<>();
                            for (String mode : List.of("reference", "typed", "native")) {
                            var session = TestingConnectorSession.builder().setPropertyMetadata(properties)
                                .setPropertyValues(Map.of(VariantPushdownConfig.FILTER_MODE, mode.equals("native") ? "typed" : mode,
                                    VariantPushdownConfig.NATIVE_ENABLED, mode.equals("native"), "parquet_shredded_variant_selective_rebuild", selective)).build();
                            var activeHandle = mode.equals("native") ? new VariantTableHandle(handle.delegate(), handle.predicates(), List.of(
                                new org.locationtech.geomesa.trino.variant.VariantNativeHint(org.locationtech.geomesa.trino.variant.VariantNativeHint.Kind.EQUALS,
                                    DOC, List.of("a"), List.of(new io.trino.spi.expression.Constant(Variant.ofLong(7), VARIANT))))) : handle;
                            rows = 0;
                            ids.clear();
                            try (var source = factory.create(true, footer, MemoryContext.NO_LIMIT).createPageSource(null, session, split,
                                    activeHandle, Optional.of(new IcebergTableCredentials(Map.of(), List.of())), columns,
                                    DynamicFilter.EMPTY, MemoryContext.NO_LIMIT)) {
                                while (!source.isFinished()) {
                                    var page = source.getNextSourcePage();
                                    if (page == null) continue;
                                    rows += page.getPositionCount();
                                    assertThat(page.getChannelCount()).isEqualTo(columns.size());
                                    for (int position = 0; position < page.getPositionCount() && output > 0; position++) {
                                        long id = BIGINT.getLong(page.getBlock(0), position);
                                        ids.add(id);
                                        if (output > 1) {
                                            Variant document = VARIANT.getObject(page.getBlock(1), position);
                                            assertThat(predicate.test(document)).isTrue();
                                            if (id % 8 == 0) {
                                                if (output == 3 && shredded) assertThat(document.getObjectField(utf8Slice("b"))).isEmpty();
                                                else assertThat(document.getObjectField(utf8Slice("b"))).isPresent();
                                            }
                                        }
                                    }
                                }
                                assertThat(source.getMetrics().getMetrics()).containsKeys("Variant checked rows", "Visibility checked rows");
                                long checked = ((io.trino.plugin.base.metrics.LongCount) source.getMetrics().getMetrics().get("Variant checked rows")).getTotal();
                                assertThat(checked).isEqualTo(java.util.stream.LongStream.range(0, ROWS)
                                    .filter(id -> id % 8 != 7 && (!spatial || id % 16 < 8) && !(useDeletes && id == 0)).count());
                                if (!mode.equals("reference")) {
                                    long direct = ((io.trino.plugin.base.metrics.LongCount) source.getMetrics().getMetrics().get("Variant typed rows")).getTotal();
                                    long fallback = ((io.trino.plugin.base.metrics.LongCount) source.getMetrics().getMetrics().get("Variant fallback rows")).getTotal();
                                    if (shredded) assertThat(direct).isPositive(); else assertThat(direct).isZero();
                                    assertThat(fallback).isPositive();
                                    if (mode.equals("native")) {
                                        long decided = ((io.trino.plugin.base.metrics.LongCount) source.getMetrics().getMetrics().get("Variant native decided rows")).getTotal();
                                        long unknown = ((io.trino.plugin.base.metrics.LongCount) source.getMetrics().getMetrics().get("Variant unknown rows")).getTotal();
                                        if (shredded) assertThat(decided).isPositive(); else assertThat(decided).isZero();
                                        assertThat(unknown).isPositive();
                                    }
                                }
                            }
                            List<Long> expected = java.util.stream.LongStream.range(0, ROWS)
                                .filter(id -> (id % 8 == 0 || id % 8 == 6) && (!spatial || id % 16 < 8) && !(useDeletes && id == 0)).boxed().toList();
                            assertThat(rows).as("shredded=%s deletes=%s footer=%s output=%s", shredded, useDeletes, footer, output).isEqualTo(expected.size());
                            if (output > 0) assertThat(ids).containsExactlyElementsOf(expected);
                            }
                            }
                        }
                    }
                }
            }
            System.out.println("VARIANT Parquet smoke passed: 384 reference/typed/native, shredded/unshredded, delete, visibility, bbox, footer, selective-rebuild and projection combinations");
        } finally {
            try (var files = Files.list(directory)) { for (Path file : files.toList()) Files.delete(file); }
            Files.delete(directory);
        }
    }

    private static void write(Path path, boolean shredded) throws Exception {
        String typed = shredded ? """
                optional group typed_value {
                    required group a { optional binary value; optional int64 typed_value; }
                    required group b { optional binary value; optional int64 typed_value; }
                }
                """ : "";
        MessageType schema = MessageTypeParser.parseMessageType("message table { optional group doc (VARIANT(1)) = 1 {"
            + " required binary metadata; optional binary value; " + typed + " } required int64 id = 2; optional binary __vis__ (STRING) = 3; optional binary geom = 4; optional float x = 5; optional float y = 6; }");
        try (var writer = ExampleParquetWriter.builder(new LocalOutputFile(path)).withType(schema).withRowGroupSize(1024).build()) {
            for (int id = 0; id < ROWS; id++) {
                Group row = new SimpleGroup(schema).append("id", (long) id).append("__vis__", id % 8 == 7 ? "B" : "A")
                    .append("x", (float) (id % 16)).append("y", 0.0f)
                    .append("geom", Binary.fromConstantByteArray(new org.locationtech.jts.io.WKBWriter().write(
                        new org.locationtech.jts.geom.GeometryFactory().createPoint(new org.locationtech.jts.geom.Coordinate(id % 16, 0)))));
                Variant value = switch (id % 8) {
                    case 0, 7 -> Variant.ofObject(Map.of(utf8Slice("a"), Variant.ofLong(7), utf8Slice("b"), Variant.ofLong(100)));
                    case 1 -> Variant.ofObject(Map.of(utf8Slice("a"), Variant.ofLong(8), utf8Slice("b"), Variant.ofLong(101)));
                    case 2 -> Variant.ofLong(7);
                    case 3 -> null;
                    case 4 -> Variant.ofObject(Map.of(utf8Slice("a"), Variant.NULL_VALUE));
                    case 5 -> Variant.EMPTY_OBJECT;
                    default -> Variant.ofObject(Map.of(utf8Slice("a"), Variant.ofLong(7), utf8Slice("fallback"), Variant.ofLong(9)));
                };
                if (value != null) {
                    Group doc = row.addGroup("doc");
                    boolean typedRow = shredded && (id % 8 == 0 || id % 8 == 1 || id % 8 == 7);
                    doc.append("metadata", Binary.fromConstantByteArray((typedRow ? Variant.NULL_VALUE : value).metadata().toSlice().getBytes()));
                    if (typedRow) {
                        Group object = doc.addGroup("typed_value");
                        object.addGroup("a").append("typed_value", id % 8 == 1 ? 8L : 7L);
                        object.addGroup("b").append("typed_value", id % 8 == 1 ? 101L : 100L);
                    } else doc.append("value", Binary.fromConstantByteArray(value.data().getBytes()));
                }
                writer.write(row);
            }
        }
    }

    private static void writeDeletes(Path path, String dataFile) throws Exception {
        MessageType schema = MessageTypeParser.parseMessageType("message deletes { required binary file_path (STRING) = 2147483546; required int64 pos = 2147483545; }");
        try (var writer = ExampleParquetWriter.builder(new LocalOutputFile(path)).withType(schema).build()) {
            writer.write(new SimpleGroup(schema).append("file_path", dataFile).append("pos", 0L));
        }
    }
}

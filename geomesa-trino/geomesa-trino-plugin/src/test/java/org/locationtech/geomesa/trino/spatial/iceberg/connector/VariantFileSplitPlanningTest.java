/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.plugin.iceberg.IcebergTableScanFilter;
import io.trino.plugin.iceberg.TableType;
import io.trino.spi.connector.*;
import io.trino.spi.expression.Constant;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.spi.variant.Variant;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.locationtech.geomesa.trino.variant.VariantPredicate;
import org.locationtech.geomesa.trino.variant.VariantPushdownConfig;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;

import static io.trino.spi.type.VariantType.VARIANT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.locationtech.geomesa.trino.spatial.iceberg.connector.VariantTestFixtures.*;

class VariantFileSplitPlanningTest {
    @Test void fileHintsPreserveVisibilityStatsProjectionAndModeGating() {
        var schema = new Schema(Types.NestedField.optional(1, "doc", Types.VariantType.get()),
                Types.NestedField.optional(2, "id", Types.LongType.get()), Types.NestedField.optional(3, "__vis__", Types.StringType.get()));
        var table = new IcebergTableHandle("s", "docs", TableType.DATA, OptionalLong.of(1), SchemaParser.toJson(schema),
                OptionalInt.empty(), Map.of(), 3, TupleDomain.all(), TupleDomain.all(), OptionalLong.empty(), Set.of(),
                Optional.empty(), "file:///test/docs", Map.of(), Optional.empty(), false, Optional.empty(), Set.of(), Optional.of(false));
        var predicate = new VariantPredicate(VariantPredicate.Kind.EQUALS, DOC, List.of("name"), List.of(), new Constant(Variant.ofString("match"), VARIANT));
        for (var mode : VariantPushdownConfig.Mode.values()) {
            Connector delegate = new Connector() {
                @Override public void shutdown() {}
                @Override public ConnectorTransactionHandle beginTransaction(IsolationLevel i, boolean r, boolean a) { return null; }
                @Override public ConnectorMetadata getMetadata(ConnectorSession s, ConnectorTransactionHandle t) { return null; }
                @Override public ConnectorSplitManager getSplitManager() {
                    return new ConnectorSplitManager() {
                        @Override public ConnectorSplitSource getSplits(ConnectorTransactionHandle t, ConnectorSession s,
                                ConnectorTableHandle handle, Set<ColumnHandle> dynamicColumns, Constraint constraint) {
                            IcebergTableHandle iceberg;
                            if (mode == VariantPushdownConfig.Mode.TYPED) {
                                assertThat(handle).isInstanceOf(IcebergTableScanFilter.class);
                                var request = (IcebergTableScanFilter) handle;
                                assertThat(request.columnIds()).containsExactly(1);
                                var lower = org.apache.iceberg.variants.Variants.metadata("$['name']");
                                var value = org.apache.iceberg.variants.Variants.object(lower);
                                value.put("$['name']", org.apache.iceberg.variants.Variants.of("z"));
                                var bound = org.apache.iceberg.types.Conversions.toByteBuffer(Types.VariantType.get(),
                                        org.apache.iceberg.variants.Variant.of(lower, value));
                                var file = org.apache.iceberg.DataFiles.builder(org.apache.iceberg.PartitionSpec.unpartitioned())
                                        .withPath("test.parquet").withFileSizeInBytes(100)
                                        .withMetrics(new org.apache.iceberg.Metrics(1L, null, null, null, null, Map.of(1, bound), Map.of(1, bound))).build();
                                assertThat(request.filter().apply(schema).test(file)).isFalse();
                                // Scan schema can differ from the handle after a rename or when
                                // selecting an older snapshot. Resolve hints by stable field ID.
                                var renamed = new Schema(Types.NestedField.optional(1, "historical", Types.VariantType.get()));
                                assertThat(request.filter().apply(renamed).test(file)).isFalse();
                                assertThat(request.filter().apply(new Schema()).test(file)).isTrue();
                                iceberg = request.table();
                            } else {
                                assertThat(handle).isInstanceOf(IcebergTableHandle.class);
                                iceberg = (IcebergTableHandle) handle;
                            }
                            assertThat(iceberg.getProjectedColumns()).contains(VIS,
                                    DOC.withVariantPaths(Set.of(io.trino.parquet.VariantPath.keys("name"))));
                            assertThat(dynamicColumns).containsExactlyInAnyOrder(ID, VIS);
                            assertThat(iceberg.getSnapshotId()).hasValue(1);
                            assertThat(constraint.getSummary().isAll()).isTrue();
                            return new FixedSplitSource(List.of());
                        }
                    };
                }
            };
            try (var source = new SpatialConnector(delegate).getSplitManager().getSplits(null, session(mode),
                    new VariantTableHandle(new VisibilityTableHandle(table, VIS, List.of("A")), List.of(predicate)), Set.of(ID), Constraint.alwaysTrue())) {
                assertThat(source.isFinished()).isTrue();
            }
        }
    }
}

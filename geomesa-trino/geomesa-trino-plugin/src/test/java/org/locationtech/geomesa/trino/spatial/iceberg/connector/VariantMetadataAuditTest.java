/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.plugin.iceberg.IcebergTableCredentials;
import io.trino.spi.connector.*;
import io.trino.spi.expression.Constant;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.variant.Variant;
import org.junit.jupiter.api.Test;
import org.locationtech.geomesa.trino.spatial.iceberg.GeoMesaColumnCatalog;
import org.locationtech.geomesa.trino.variant.VariantPredicateCompiler;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.*;
import static org.locationtech.geomesa.trino.spatial.iceberg.connector.VariantTestFixtures.*;
import static org.locationtech.geomesa.trino.variant.VariantPushdownConfig.Mode.REFERENCE;

class VariantMetadataAuditTest {
    private static final SchemaTableName NAME = new SchemaTableName("s", "docs");
    private static final ConnectorTableMetadata METADATA = new ConnectorTableMetadata(NAME,
        List.of(new ColumnMetadata("doc", DOC.getType()), new ColumnMetadata("id", ID.getType())));
    private static final IcebergTableCredentials CREDENTIALS = new IcebergTableCredentials(Map.of(), List.of());
    private enum Partitioning implements ConnectorPartitioningHandle { INSTANCE }
    private static final ConnectorTableProperties PROPERTIES = new ConnectorTableProperties(
        TupleDomain.withColumnDomains(Map.of(ID, Domain.singleValue(BIGINT, 0L))),
        Optional.of(new ConnectorTablePartitioning(Partitioning.INSTANCE, List.of(ID))),
        Optional.of(new DiscretePredicates(List.of(ID), List.of(TupleDomain.withColumnDomains(Map.of(ID, Domain.singleValue(BIGINT, 0L)))))),
        List.of(new SortingProperty<>(ID, SortOrder.ASC_NULLS_FIRST)));

    private static List<ConnectorTableHandle> handles() {
        var spatial = new SpatialTableHandle(TABLE, ID, List.of(), 0, 0, 1, 1);
        var visible = new VisibilityTableHandle(TABLE, VIS, List.of("A"));
        var both = new VisibilityTableHandle(spatial, VIS, List.of("A"));
        var predicate = VariantPredicateCompiler.compile(equalsCall("d", Variant.ofLong(7), "a"), ASSIGNMENTS).orElseThrow();
        return List.of(spatial, visible, both, new VariantTableHandle(TABLE, List.of(predicate)),
            new VariantTableHandle(spatial, List.of(predicate)), new VariantTableHandle(visible, List.of(predicate)),
            new VariantTableHandle(both, List.of(predicate)));
    }
    private static SpatialConnectorMetadata metadata(ConnectorMetadata delegate) {
        return new SpatialConnectorMetadata(delegate, new GeoMesaColumnCatalog(), false, false, null, false, Set.of(), true);
    }
    private static class Delegate implements ConnectorMetadata {
        @Override public SchemaTableName getTableName(ConnectorSession s, ConnectorTableHandle h) {
            assertThat(h).isSameAs(TABLE); return NAME;
        }
        @Override public ConnectorTableMetadata getTableMetadata(ConnectorSession s, ConnectorTableHandle h) {
            assertThat(h).isSameAs(TABLE); return METADATA;
        }
        @Override public Map<String, ColumnHandle> getColumnHandles(ConnectorSession s, ConnectorTableHandle h) {
            assertThat(h).isSameAs(TABLE); return ASSIGNMENTS;
        }
        @Override public ColumnMetadata getColumnMetadata(ConnectorSession s, ConnectorTableHandle h, ColumnHandle c) {
            assertThat(h).isSameAs(TABLE); assertThat(c).isSameAs(DOC); return METADATA.getColumns().getFirst();
        }
        @Override public ConnectorTableProperties getTableProperties(ConnectorSession s, ConnectorTableHandle h) {
            assertThat(h).isSameAs(TABLE); return PROPERTIES;
        }
        @Override public Optional<ConnectorTableCredentials> getTableCredentials(ConnectorSession s, ConnectorTableHandle h) {
            assertThat(h).isSameAs(TABLE); return Optional.of(CREDENTIALS);
        }
        @Override public Optional<Object> getInfo(ConnectorSession s, ConnectorTableHandle h) {
            assertThat(h).isSameAs(TABLE); return Optional.of("info");
        }
        @Override public TableStatistics getTableStatistics(ConnectorSession s, ConnectorTableHandle h) {
            assertThat(h).isSameAs(TABLE); return TableStatistics.empty();
        }
        @Override public void validateScan(ConnectorSession s, ConnectorTableHandle h) { assertThat(h).isSameAs(TABLE); }
        @Override public boolean allowSplittingReadIntoMultipleSubQueries(ConnectorSession s, ConnectorTableHandle h) {
            assertThat(h).isSameAs(TABLE); return true;
        }
    }
    @Test void filteredRowsMustNotBeReplacedWithPartitionValues() {
        var metadata = metadata(new Delegate());
        assertThat(metadata.getTableProperties(null, TABLE)).isSameAs(PROPERTIES);
        for (var handle : handles()) {
            var properties = metadata.getTableProperties(null, handle);
            assertThat(properties.getDiscretePredicates()).as(handle.toString()).isEmpty();
            // These properties remain valid for a subset of the original rows.
            assertThat(properties.getPredicate()).isEqualTo(PROPERTIES.getPredicate());
            assertThat(properties.getTablePartitioning()).isEqualTo(PROPERTIES.getTablePartitioning());
            assertThat(properties.getLocalProperties()).isEqualTo(PROPERTIES.getLocalProperties());
        }
    }
    @Test void readMetadataUnwrapsEveryCanonicalCombination() {
        var metadata = metadata(new Delegate());
        for (var handle : handles()) {
            assertThat(metadata.getTableName(null, handle)).isEqualTo(NAME);
            assertThat(metadata.getTableMetadata(null, handle)).isSameAs(METADATA);
            assertThat(metadata.getTableSchema(null, handle).getTable()).isEqualTo(NAME);
            assertThat(metadata.getColumnHandles(null, handle)).containsAllEntriesOf(ASSIGNMENTS);
            assertThat(metadata.getColumnMetadata(null, handle, DOC)).isSameAs(METADATA.getColumns().getFirst());
            assertThat(metadata.getTableCredentials(null, handle)).contains(CREDENTIALS);
            assertThat(metadata.getInfo(null, handle)).contains("info");
            assertThat(metadata.getTableStatistics(null, handle)).isEqualTo(TableStatistics.empty());
            metadata.validateScan(null, handle);
            assertThat(metadata.allowSplittingReadIntoMultipleSubQueries(null, handle)).isTrue();
        }
    }
    @Test void scanReplacingPushdownsCannotReachDelegateWithClaims() {
        AtomicInteger calls = new AtomicInteger();
        var metadata = metadata(new Delegate() {
            @Override public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession s, ConnectorTableHandle h, long limit) {
                assertThat(h).isSameAs(TABLE); calls.incrementAndGet(); return Optional.empty();
            }
            @Override public Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(ConnectorSession s, ConnectorTableHandle h,
                    long count, List<SortItem> sort, Map<String, ColumnHandle> bindings) {
                assertThat(h).isSameAs(TABLE); calls.incrementAndGet(); return Optional.empty();
            }
            @Override public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(ConnectorSession s, ConnectorTableHandle h,
                    List<AggregateFunction> aggregates, Map<String, ColumnHandle> bindings, List<List<ColumnHandle>> groups) {
                assertThat(h).isSameAs(TABLE); calls.incrementAndGet(); return Optional.empty();
            }
        });
        for (var handle : handles()) {
            assertThat(metadata.applyLimit(null, handle, 1)).isEmpty();
            assertThat(metadata.applyTopN(null, handle, 1, List.of(), Map.of())).isEmpty();
            assertThat(metadata.applyAggregation(null, handle, List.of(), Map.of(), List.of(List.of()))).isEmpty();
            assertThat(metadata.applySample(null, handle, SampleType.SYSTEM, 0.1)).isEmpty();
            assertThat(metadata.applyPartitioning(null, handle, Optional.empty(), List.of())).isEmpty();
            assertThat(metadata.applyTableScanRedirect(null, handle)).isEmpty();
            assertThat(metadata.resolveIndex(null, handle, Set.of(), Set.of(), TupleDomain.all())).isEmpty();
            assertThat(metadata.applyJoin(null, JoinType.INNER, handle, TABLE, Constant.TRUE, Map.of(), Map.of(), null)).isEmpty();
            assertThat(metadata.applyJoin(null, JoinType.INNER, TABLE, handle, Constant.TRUE, Map.of(), Map.of(), null)).isEmpty();
            assertThat(metadata.applyUpdate(null, handle, Map.of())).isEmpty();
            assertThat(metadata.applyDelete(null, handle)).isEmpty();
        }
        assertThat(calls).hasValue(0);
        metadata.applyLimit(null, TABLE, 1);
        metadata.applyTopN(null, TABLE, 1, List.of(), Map.of());
        metadata.applyAggregation(null, TABLE, List.of(), Map.of(), List.of(List.of()));
        assertThat(calls).hasValue(3);
    }
    @Test void incomingMaintenanceScansCannotBecomeEligibleAfterDelegateChanges() {
        for (IcebergTableHandle input : List.of(TABLE.forAnalyze(), TABLE.forOptimize(true, io.airlift.units.DataSize.ofBytes(100)))) {
            var metadata = metadata(new Delegate() {
                @Override public SchemaTableName getTableName(ConnectorSession s, ConnectorTableHandle h) { return NAME; }
                @Override public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession s, ConnectorTableHandle h, Constraint c) {
                    return Optional.of(new ConstraintApplicationResult<>(TABLE, c.getSummary(), c.getExpression(), false));
                }
            });
            var call = equalsCall("d", Variant.ofLong(7), "a");
            var result = metadata.applyFilter(session(REFERENCE), input, new Constraint(TupleDomain.all(), call, ASSIGNMENTS)).orElseThrow();
            assertThat(result.getHandle()).isSameAs(TABLE);
            assertThat(result.getRemainingExpression()).contains(call);
        }
    }
    @Test void splitEnumerationPreservesPredicateAndVisibilityDemand() {
        AtomicInteger calls = new AtomicInteger();
        var connector = new SpatialConnector(new Connector() {
            @Override public void shutdown() {}
            @Override public ConnectorSplitManager getSplitManager() { return new ConnectorSplitManager() {
                @Override public ConnectorSplitSource getSplits(ConnectorTransactionHandle t, ConnectorSession s, ConnectorTableHandle h,
                        Set<ColumnHandle> dynamic, Constraint constraint) {
                    var table = (IcebergTableHandle) h;
                    assertThat(table.getProjectedColumns()).contains(VIS);
                    assertThat(table.getProjectedColumns()).anySatisfy(column -> assertThat(column.getVariantPaths())
                        .contains(Set.of(io.trino.parquet.VariantPath.keys("a"))));
                    assertThat(dynamic).containsExactlyInAnyOrder(ID, VIS);
                    calls.incrementAndGet();
                    return new FixedSplitSource(List.of());
                }
            }; }
        }, null, null, false);
        connector.getSplitManager().getSplits(null, null, handles().getLast(), Set.of(ID), new Constraint(TupleDomain.all()));
        assertThat(calls).hasValue(1);
    }
}

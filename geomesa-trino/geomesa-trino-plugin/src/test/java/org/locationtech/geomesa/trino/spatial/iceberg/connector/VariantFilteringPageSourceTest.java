/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.*;
import io.trino.spi.variant.Variant;
import org.junit.jupiter.api.Test;
import org.locationtech.geomesa.trino.variant.VariantPredicate;
import org.locationtech.geomesa.trino.variant.VariantPredicateCompiler;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.function.ObjLongConsumer;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VariantType.VARIANT;
import static org.assertj.core.api.Assertions.*;
import static org.locationtech.geomesa.trino.spatial.iceberg.connector.VariantTestFixtures.*;

class VariantFilteringPageSourceTest {
    private static final List<VariantPredicate> PREDICATES = List.of(
        VariantPredicateCompiler.compile(equalsCall("d", Variant.ofLong(7), "a"), ASSIGNMENTS).orElseThrow());
    private static final class LazyPage implements SourcePage {
        private final boolean supportsTry;
        private int[] positions = {0, 1, 2, 3, 4};
        private final Variant[] documents = {object(7), object(8), null, Variant.ofLong(7), object(7)};
        private int idLoads;
        private int variantLoads;
        private int fallbackSelections;
        LazyPage(boolean supportsTry) { this.supportsTry = supportsTry; }
        @Override public int getPositionCount() { return positions.length; }
        @Override public int getChannelCount() { return 2; }
        @Override public Block getBlock(int channel) {
            if (channel == 0) {
                idLoads++;
                var builder = BIGINT.createFixedSizeBlockBuilder(positions.length);
                for (int position : positions) BIGINT.writeLong(builder, position);
                return builder.build();
            }
            if (channel != 1) throw new IndexOutOfBoundsException(channel);
            variantLoads++;
            var builder = VARIANT.createBlockBuilder(null, positions.length);
            for (int position : positions) {
                if (documents[position] == null) builder.appendNull(); else VARIANT.writeObject(builder, documents[position]);
            }
            return builder.build();
        }
        @Override public Page getPage() { return getColumns(new int[]{0, 1}); }
        @Override public void selectPositions(int[] selected, int offset, int size) {
            fallbackSelections++;
            select(selected, offset, size);
        }
        private void select(int[] selected, int offset, int size) {
            int[] next = new int[size];
            for (int i = 0; i < size; i++) next[i] = positions[selected[offset + i]];
            positions = next;
        }
        @Override public boolean trySelectPositions(int[] selected, int offset, int size) {
            if (!supportsTry) return false;
            select(selected, offset, size);
            return true;
        }
        @Override public long getSizeInBytes() { return 0; }
        @Override public long getRetainedSizeInBytes() { return 0; }
        @Override public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer) {}
    }
    private static Variant object(long value) { return Variant.ofObject(Map.of(utf8Slice("a"), Variant.ofLong(value))); }
    private static final class Source implements ConnectorPageSource {
        private final List<SourcePage> pages;
        private int index;
        private boolean closed;
        private final CompletableFuture<Void> blocked = new CompletableFuture<>();
        Source(SourcePage... pages) { this.pages = Arrays.asList(pages); }
        @Override public SourcePage getNextSourcePage() { return index == pages.size() ? null : pages.get(index++); }
        @Override public long getCompletedBytes() { return 123; }
        @Override public OptionalLong getCompletedPositions() { return OptionalLong.of(5); }
        @Override public long getReadTimeNanos() { return 456; }
        @Override public boolean isFinished() { return index == pages.size(); }
        @Override public long getMemoryUsage() { return 789; }
        @Override public void close() { closed = true; }
        @Override public CompletableFuture<?> isBlocked() { return blocked; }
    }
    @Test void appliesSelectionsWithAndWithoutTryAndKeepsOrdinaryOutputLazy() {
        for (boolean supportsTry : List.of(true, false)) {
            var page = new LazyPage(supportsTry);
            // Simulates earlier spatial/security selections with an offset, repeats and reversal.
            page.selectPositions(new int[]{99, 4, 0, 4, 3, 1}, 1, 5);
            var source = new VariantFilteringPageSource(new Source(page), 1, PREDICATES, List.of(1));
            var result = source.getNextSourcePage();
            assertThat(result.getPositionCount()).isEqualTo(3);
            assertThat(page.idLoads).isZero();
            assertThat(page.variantLoads).isEqualTo(1);
            assertThat(result.getChannelCount()).isEqualTo(1);
            assertThatThrownBy(() -> result.getColumns(new int[]{1})).isInstanceOf(IndexOutOfBoundsException.class);
            assertThatThrownBy(() -> result.getBlock(-1)).isInstanceOf(IndexOutOfBoundsException.class);
            Block ids = result.getBlock(0);
            assertThat(List.of(BIGINT.getLong(ids, 0), BIGINT.getLong(ids, 1), BIGINT.getLong(ids, 2))).containsExactly(4L, 0L, 4L);
            int[] selected = {99, 2, 1, 2};
            if (!result.trySelectPositions(selected, 1, 3)) result.selectPositions(selected, 1, 3);
            selected[1] = 100;
            assertThat(result.getPositionCount()).isEqualTo(3);
            assertThat(BIGINT.getLong(result.getPage().getBlock(0), 0)).isEqualTo(4);
            result.selectPositions(new int[0], 0, 0);
            assertThat(result.getPage().getPositionCount()).isZero();
        }
    }
    @Test void handlesNullPagesEmptyPagesTransitionsAndLifecycle() throws IOException {
        var empty = new LazyPage(true);
        empty.selectPositions(new int[0], 0, 0);
        var delegate = new Source(null, empty, new LazyPage(true), new LazyPage(false));
        var source = new VariantFilteringPageSource(delegate, 0, PREDICATES, List.of(1));
        assertThat(source.getNextSourcePage()).isNull();
        assertThat(source.isBlocked()).isSameAs(delegate.blocked);
        assertThat(source.getNextSourcePage().getPositionCount()).isZero();
        assertThat(empty.variantLoads).isZero();
        assertThat(source.getNextSourcePage().getPositionCount()).isEqualTo(2);
        assertThat(source.getNextSourcePage().getPositionCount()).isEqualTo(2);
        assertThat(source.isFinished()).isTrue();
        assertThat(source.getCompletedBytes()).isEqualTo(123);
        assertThat(source.getReadTimeNanos()).isEqualTo(456);
        assertThat(source.getMemoryUsage()).isEqualTo(789);
        assertThat(source.getMetrics().getMetrics().get("Variant rejected rows").toString()).contains("6");
        source.close();
        assertThat(delegate.closed).isTrue();
    }
    @Test void providerAppendsUnionDemandOnceAndUnwrapsOnlyItsOwnLayer() {
        var predicates = List.of(PREDICATES.getFirst(), VariantPredicateCompiler.compile(
            equalsCall("alias", Variant.ofLong(7), "b"), ASSIGNMENTS).orElseThrow());
        var visibility = new VisibilityTableHandle(TABLE, VIS, List.of("A"));
        ConnectorPageSourceProvider delegate = new ConnectorPageSourceProvider() {
            @Override public ConnectorPageSource createPageSource(ConnectorTransactionHandle t, ConnectorSession s,
                    ConnectorSplit split, ConnectorTableHandle handle, Optional<ConnectorTableCredentials> credentials,
                    List<ColumnHandle> columns, DynamicFilter dynamic, MemoryContext memory) {
                assertThat(handle).isSameAs(visibility);
                assertThat(columns).hasSize(2);
                assertThat(columns.getFirst()).isSameAs(DOC); // whole engine output remains whole
                assertThat(((io.trino.plugin.iceberg.IcebergColumnHandle) columns.get(1)).getVariantPaths().orElseThrow()).hasSize(2);
                return new Source();
            }
        };
        new VariantPageSourceProvider(delegate).createPageSource(null, session(org.locationtech.geomesa.trino.variant.VariantPushdownConfig.Mode.REFERENCE), null,
            new VariantTableHandle(visibility, predicates), Optional.empty(), List.of(DOC), DynamicFilter.EMPTY, null);
    }
    @Test void countStarSplitDemandRetainsPredicateAndVisibilityColumns() {
        Connector delegate = new Connector() {
            @Override public void shutdown() {}
            @Override public ConnectorTransactionHandle beginTransaction(io.trino.spi.transaction.IsolationLevel i, boolean r, boolean a) { return null; }
            @Override public ConnectorMetadata getMetadata(ConnectorSession s, ConnectorTransactionHandle t) { return null; }
            @Override public ConnectorSplitManager getSplitManager() {
                return new ConnectorSplitManager() {
                    @Override public ConnectorSplitSource getSplits(ConnectorTransactionHandle t, ConnectorSession s,
                            ConnectorTableHandle h, java.util.Set<ColumnHandle> dynamicColumns, Constraint c) {
                        var iceberg = (io.trino.plugin.iceberg.IcebergTableHandle) h;
                        assertThat(iceberg.getProjectedColumns()).contains(VIS,
                            DOC.withVariantPaths(java.util.Set.of(io.trino.parquet.VariantPath.keys("a"))));
                        assertThat(dynamicColumns).contains(VIS);
                        return new FixedSplitSource(List.of());
                    }
                };
            }
        };
        new SpatialConnector(delegate).getSplitManager().getSplits(null, null,
            new VariantTableHandle(new VisibilityTableHandle(TABLE, VIS, List.of("A")), PREDICATES),
            java.util.Set.of(), Constraint.alwaysTrue());
    }

    @Test void nativeUnknownKeepsRowsWhileExactUnknownFails() {
        var builder = io.trino.spi.type.TinyintType.TINYINT.createFixedSizeBlockBuilder(4);
        for (var decision : io.trino.parquet.reader.VariantPredicateEvaluator.Decision.values()) {
            io.trino.spi.type.TinyintType.TINYINT.writeLong(builder, decision.code());
        }
        Block decisions = builder.build();
        var nativeEvaluator = new org.locationtech.geomesa.trino.variant.VariantNativeHintEvaluator(List.of(
            new org.locationtech.geomesa.trino.variant.VariantNativeHint(org.locationtech.geomesa.trino.variant.VariantNativeHint.Kind.IS_NULL,
                DOC, List.of("a"), List.of())));
        var source = VariantFilteringPageSource.typed(new Source(SourcePage.create(new Page(decisions))), 0, List.of(0), List.of(nativeEvaluator));
        assertThat(source.getNextSourcePage().getPositionCount()).isEqualTo(2);
        var exact = VariantFilteringPageSource.typed(new Source(SourcePage.create(new Page(decisions))), 0, List.of(0),
            List.of(new org.locationtech.geomesa.trino.variant.VariantTypedEvaluator(PREDICATES)));
        assertThatThrownBy(exact::getNextSourcePage).isInstanceOf(IllegalStateException.class).hasMessageContaining("UNKNOWN");
    }
}

/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import io.trino.plugin.base.metrics.LongCount;
import io.trino.parquet.reader.VariantPredicateEvaluator;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Metrics;
import org.locationtech.geomesa.trino.variant.VariantPredicate;
import org.locationtech.geomesa.trino.variant.VariantRuntimeEvaluator;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import static io.trino.spi.type.VariantType.VARIANT;
import static io.trino.spi.type.TinyintType.TINYINT;

/** Reference oracle after delete/security/spatial filtering; no session flag may disable an existing claim. */
final class VariantFilteringPageSource implements ConnectorPageSource {
    private final ConnectorPageSource delegate;
    private final int outputChannels;
    private final List<VariantPredicate> predicates;
    private final List<Integer> channels;
    private final List<VariantRuntimeEvaluator> typedEvaluators;
    private final boolean typed;
    private long checkedRows;
    private long rejectedRows;
    private long evaluations;

    VariantFilteringPageSource(ConnectorPageSource delegate, int outputChannels,
            List<VariantPredicate> predicates, List<Integer> channels) {
        this.typed = false;
        this.typedEvaluators = List.of();
        this.delegate = delegate;
        this.outputChannels = outputChannels;
        this.predicates = List.copyOf(predicates);
        this.channels = List.copyOf(channels);
        if (predicates.size() != channels.size()) throw new IllegalArgumentException("Predicate channels differ");
    }

    static VariantFilteringPageSource typed(ConnectorPageSource delegate, int outputChannels,
            List<Integer> channels, List<VariantRuntimeEvaluator> evaluators) {
        return new VariantFilteringPageSource(delegate, outputChannels, true, channels, evaluators);
    }

    private VariantFilteringPageSource(ConnectorPageSource delegate, int outputChannels, boolean typed,
            List<Integer> channels, List<VariantRuntimeEvaluator> evaluators) {
        this.delegate = delegate;
        this.outputChannels = outputChannels;
        this.typed = typed;
        this.channels = List.copyOf(channels);
        this.typedEvaluators = List.copyOf(evaluators);
        this.predicates = List.of();
        if (channels.size() != evaluators.size()) throw new IllegalArgumentException("Predicate channels differ");
    }

    @Override public SourcePage getNextSourcePage() {
        SourcePage page = delegate.getNextSourcePage();
        if (page == null) return null;
        int initialCount = page.getPositionCount();
        checkedRows += initialCount;
        for (int predicate = 0; predicate < channels.size() && page.getPositionCount() > 0; predicate++) {
            int count = page.getPositionCount();
            Block values = page.getBlock(channels.get(predicate));
            int[] retained = new int[count];
            int kept = 0;
            for (int position = 0; position < count; position++) {
                boolean matches;
                if (typed) {
                    if (values.isNull(position)) throw new IllegalStateException("Null VARIANT decision");
                    var decision = VariantPredicateEvaluator.Decision.fromCode((byte) TINYINT.getLong(values, position));
                    if (decision == VariantPredicateEvaluator.Decision.UNKNOWN && typedEvaluators.get(predicate).exact()) {
                        throw new IllegalStateException("Exact VARIANT predicate returned UNKNOWN");
                    }
                    matches = decision == VariantPredicateEvaluator.Decision.TRUE || decision == VariantPredicateEvaluator.Decision.UNKNOWN;
                } else {
                    evaluations++;
                    matches = predicates.get(predicate).test(values.isNull(position) ? null : VARIANT.getObject(values, position));
                }
                if (matches) retained[kept++] = position;
            }
            if (kept < count && !page.trySelectPositions(retained, 0, kept)) page.selectPositions(retained, 0, kept);
        }
        rejectedRows += initialCount - page.getPositionCount();
        return page.getChannelCount() > outputChannels ? new ChannelPrefixSourcePage(page, outputChannels) : page;
    }

    @Override public Metrics getMetrics() {
        return delegate.getMetrics().mergeWith(new Metrics(Map.of(
            "Variant checked rows", new LongCount(checkedRows),
            "Variant rejected rows", new LongCount(rejectedRows),
            "Variant reference evaluations", new LongCount(evaluations),
            "Variant typed rows", new LongCount(typedEvaluators.stream().filter(VariantRuntimeEvaluator::exact).mapToLong(VariantRuntimeEvaluator::directRows).sum()),
            "Variant fallback rows", new LongCount(typedEvaluators.stream().mapToLong(VariantRuntimeEvaluator::fallbackRows).sum()),
            "Variant native decided rows", new LongCount(typedEvaluators.stream().filter(evaluator -> !evaluator.exact()).mapToLong(VariantRuntimeEvaluator::directRows).sum()),
            "Variant unknown rows", new LongCount(typedEvaluators.stream().mapToLong(VariantRuntimeEvaluator::unknownRows).sum()))));
    }
    @Override public long getCompletedBytes() { return delegate.getCompletedBytes(); }
    @Override public OptionalLong getCompletedPositions() { return delegate.getCompletedPositions(); }
    @Override public long getReadTimeNanos() { return delegate.getReadTimeNanos(); }
    @Override public boolean isFinished() { return delegate.isFinished(); }
    @Override public long getMemoryUsage() { return delegate.getMemoryUsage(); }
    @Override public void close() throws IOException { delegate.close(); }
    @Override public CompletableFuture<?> isBlocked() { return delegate.isBlocked(); }
}

/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.IcebergVariantPredicateColumn;
import io.trino.spi.connector.*;
import org.locationtech.geomesa.trino.variant.VariantPathDemand;
import org.locationtech.geomesa.trino.variant.VariantPushdownConfig;
import org.locationtech.geomesa.trino.variant.VariantTypedEvaluator;
import org.locationtech.geomesa.trino.variant.VariantRuntimeEvaluator;
import org.locationtech.geomesa.trino.variant.VariantNativeHintEvaluator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Appends physical path demands; authoritative evaluation also runs on unshredded files. */
final class VariantPageSourceProvider implements ConnectorPageSourceProvider {
    private final ConnectorPageSourceProvider delegate;

    VariantPageSourceProvider(ConnectorPageSourceProvider delegate) { this.delegate = delegate; }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session,
            ConnectorSplit split, ConnectorTableHandle table, Optional<ConnectorTableCredentials> credentials,
            List<ColumnHandle> columns, DynamicFilter dynamicFilter, MemoryContext memoryContext) {
        if (!(table instanceof VariantTableHandle variant)) {
            return delegate.createPageSource(transaction, session, split, table, credentials, columns, dynamicFilter, memoryContext);
        }
        Map<IcebergColumnHandle, IcebergColumnHandle> demands = VariantPathDemand.columns(variant.predicates());
        List<ColumnHandle> physical = new ArrayList<>(columns);
        List<Integer> channels = new ArrayList<>();
        if (session.getProperty(VariantPushdownConfig.FILTER_MODE, VariantPushdownConfig.Mode.class) == VariantPushdownConfig.Mode.TYPED) {
            List<VariantRuntimeEvaluator> evaluators = new ArrayList<>();
            demands.forEach((source, demanded) -> {
                VariantTypedEvaluator evaluator = new VariantTypedEvaluator(variant.predicates().stream()
                    .filter(predicate -> predicate.column().equals(source)).toList());
                evaluators.add(evaluator);
                channels.add(physical.size());
                physical.add(new IcebergVariantPredicateColumn(demanded, evaluator));
            });
            if (VariantPushdownConfig.nativeEnabled(session)) {
                VariantPathDemand.columns(List.of(), variant.hints()).forEach((source, demanded) -> {
                    VariantNativeHintEvaluator evaluator = new VariantNativeHintEvaluator(variant.hints().stream()
                        .filter(hint -> hint.column().equals(source)).toList());
                    evaluators.add(evaluator);
                    channels.add(physical.size());
                    physical.add(new IcebergVariantPredicateColumn(demanded, evaluator));
                });
            }
            ConnectorPageSource source = delegate.createPageSource(transaction, session, split, variant.delegate(),
                credentials, physical, dynamicFilter, memoryContext);
            return VariantFilteringPageSource.typed(source, columns.size(), channels, evaluators);
        }
        for (var predicate : variant.predicates()) {
            IcebergColumnHandle demanded = demands.get(predicate.column());
            int channel = physical.indexOf(demanded);
            if (channel < 0) {
                channel = physical.size();
                physical.add(demanded);
            }
            channels.add(channel);
        }
        ConnectorPageSource source = delegate.createPageSource(transaction, session, split, variant.delegate(),
            credentials, physical, dynamicFilter, memoryContext);
        return new VariantFilteringPageSource(source, columns.size(), variant.predicates(), channels);
    }
}

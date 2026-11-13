/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.variant;

import io.trino.parquet.VariantPath;
import io.trino.plugin.iceberg.IcebergColumnHandle;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Union only predicate demands; ordinary whole-column requests remain whole. */
public final class VariantPathDemand {
    private VariantPathDemand() {}

    public static Map<IcebergColumnHandle, IcebergColumnHandle> columns(List<VariantPredicate> predicates) {
        return columns(predicates, List.of());
    }

    public static Map<IcebergColumnHandle, IcebergColumnHandle> columns(List<VariantPredicate> predicates, List<VariantNativeHint> hints) {
        Map<IcebergColumnHandle, Set<VariantPath>> demands = new LinkedHashMap<>();
        for (VariantPredicate predicate : predicates) {
            demands.computeIfAbsent(predicate.column(), ignored -> new LinkedHashSet<>()).add(predicate.demand());
        }
        for (VariantNativeHint hint : hints) {
            demands.computeIfAbsent(hint.column(), ignored -> new LinkedHashSet<>()).add(hint.demand());
        }
        Map<IcebergColumnHandle, IcebergColumnHandle> columns = new LinkedHashMap<>();
        demands.forEach((column, paths) -> columns.put(column,
            paths.stream().anyMatch(path -> path.steps().isEmpty()) ? column : column.withVariantPaths(paths)));
        return columns;
    }
}

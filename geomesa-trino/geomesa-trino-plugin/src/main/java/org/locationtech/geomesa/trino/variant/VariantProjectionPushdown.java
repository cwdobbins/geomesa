/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.variant;

import io.airlift.slice.Slice;
import io.trino.parquet.VariantPath;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.plugin.iceberg.TableType;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.VariantType.VARIANT;

/** Restricts physical input demand, leaving every SQL operation in the engine. */
public final class VariantProjectionPushdown {
    private static final int MAX_NODES = 256;
    private static final int MAX_STEPS = 64;
    private VariantProjectionPushdown() {}

    public static Optional<ProjectionApplicationResult<ConnectorTableHandle>> apply(IcebergTableHandle table,
            List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments) {
        if (table.getTableType() != TableType.DATA || table.isRecordScannedFiles() || table.getForAnalyze().orElse(false)
                || table.getMaxScannedFileSize().isPresent()
                || assignments.values().stream().anyMatch(column -> !(column instanceof IcebergColumnHandle)
                    || ((IcebergColumnHandle) column).isMergeRowIdColumn())
                || !bounded(projections)) return Optional.empty();
        Map<Integer, Set<VariantPath>> demands = new LinkedHashMap<>();
        for (ConnectorExpression expression : projections) {
            if (!collect(expression, assignments, demands)) return Optional.empty();
        }
        Map<String, ColumnHandle> updated = new LinkedHashMap<>(assignments);
        assignments.forEach((name, handle) -> {
            IcebergColumnHandle column = (IcebergColumnHandle) handle;
            Set<VariantPath> paths = demands.get(column.getId());
            // A path-bearing assignment already represents an authorized restricted input.
            // Preserve it across later passes; never turn a variable-only pass into a whole read.
            if (eligible(column) && column.getVariantPaths().isEmpty() && paths != null
                    && paths.stream().noneMatch(path -> path.steps().isEmpty())) {
                updated.put(name, column.withVariantPaths(paths));
            }
        });
        if (updated.equals(assignments)) return Optional.empty();
        Set<IcebergColumnHandle> projected = new LinkedHashSet<>();
        List<Assignment> outputs = new ArrayList<>();
        updated.forEach((name, handle) -> {
            var column = (IcebergColumnHandle) handle;
            projected.add(column);
            outputs.add(new Assignment(name, column, column.getType()));
        });
        return Optional.of(new ProjectionApplicationResult<>(table.withProjectedColumns(projected), projections, outputs, false));
    }

    private static boolean collect(ConnectorExpression expression, Map<String, ColumnHandle> assignments,
            Map<Integer, Set<VariantPath>> demands) {
        Access access = access(expression);
        if (access != null) {
            if (access.path().steps().size() > MAX_STEPS) return false;
            demand(access.variable(), access.path(), assignments, demands);
            return true;
        }
        if (expression instanceof Call call && builtin(call)) {
            Optional<VariantPredicate> predicate = VariantPredicateCompiler.compile(call, assignments);
            if (predicate.isPresent()) {
                demand((Variable) call.getArguments().getFirst(), predicate.get().demand(), assignments, demands);
                return true;
            }
            if (call.getFunctionName().getName().equals("geomesa_variant_get") && VARIANT.equals(call.getType())
                    && call.getArguments().size() == 2 && call.getArguments().getFirst() instanceof Variable variable
                    && VARIANT.equals(variable.getType())) {
                List<String> path = VariantPredicateCompiler.path(call.getArguments().get(1));
                if (path != null) {
                    demand(variable, VariantPath.keys(path.toArray(String[]::new)), assignments, demands);
                    return true;
                }
            }
        }
        // Unknown outer operations consume their entire argument. A known subdocument
        // argument is safe to retain whole; direct variable use demands the document root.
        for (ConnectorExpression child : expression.getChildren()) {
            if (!collect(child, assignments, demands)) return false;
        }
        return true;
    }

    private static void demand(Variable variable, VariantPath path, Map<String, ColumnHandle> assignments,
            Map<Integer, Set<VariantPath>> demands) {
        if (!(assignments.get(variable.getName()) instanceof IcebergColumnHandle column)
                || !VARIANT.equals(variable.getType()) || !eligible(column)) return;
        Set<VariantPath> paths = demands.computeIfAbsent(column.getId(), ignored -> new LinkedHashSet<>());
        paths.addAll(column.getVariantPaths().orElse(Set.of(path)));
    }

    private static boolean eligible(IcebergColumnHandle column) {
        return column.isBaseColumn() && VARIANT.equals(column.getType());
    }

    private static Access access(ConnectorExpression expression) {
        List<VariantPath.Step> reversed = new ArrayList<>();
        ConnectorExpression current = expression;
        while (current instanceof Call call && builtin(call) && VARIANT.equals(call.getType())
                && call.getFunctionName().getName().equals("$operator$subscript") && call.getArguments().size() == 2
                && VARIANT.equals(call.getArguments().getFirst().getType())
                && call.getArguments().get(1) instanceof Constant key && key.getType() instanceof VarcharType
                && key.getValue() instanceof Slice encoded) {
            String name = encoded.toStringUtf8();
            if (!utf8Slice(name).equals(encoded)) return null;
            reversed.add(VariantPath.Step.key(name));
            current = call.getArguments().getFirst();
        }
        return current instanceof Variable variable && VARIANT.equals(variable.getType())
            ? new Access(variable, new VariantPath(reversed.reversed())) : null;
    }

    private static boolean builtin(Call call) {
        return call.getFunctionName().getCatalogSchema().isEmpty()
            || call.getFunctionName().getCatalogSchema().get().equals(new CatalogSchemaName("system", "builtin"));
    }

    private static boolean bounded(List<ConnectorExpression> projections) {
        if (projections.size() > MAX_NODES) return false;
        List<ConnectorExpression> pending = new ArrayList<>(projections);
        int nodes = 0;
        while (!pending.isEmpty()) {
            ConnectorExpression expression = pending.removeLast();
            if (++nodes > MAX_NODES || expression.getChildren().size() > MAX_NODES) return false;
            pending.addAll(expression.getChildren());
        }
        return true;
    }

    private record Access(Variable variable, VariantPath path) {}
}

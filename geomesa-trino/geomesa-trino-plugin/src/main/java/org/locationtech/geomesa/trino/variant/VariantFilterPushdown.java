/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.variant;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;

/** Transparent conjunct bookkeeping. Opaque engine copies are never interpreted or claimed. */
public final class VariantFilterPushdown {
    private static final FunctionName ENGINE_EXPRESSION = new FunctionName("$engine_expression");
    private VariantFilterPushdown() {}

    public static List<VariantPredicate> candidates(ConnectorExpression expression, Map<String, ColumnHandle> assignments) {
        if (!safeShape(expression)) return List.of();
        List<VariantPredicate> predicates = new ArrayList<>();
        for (ConnectorExpression conjunct : conjuncts(expression)) {
            VariantPredicateCompiler.compile(conjunct, assignments).ifPresent(predicates::add);
        }
        return predicates.stream().distinct().toList();
    }

    public static ConnectorExpression removeClaims(ConnectorExpression expression, Map<String, ColumnHandle> assignments,
            Set<VariantPredicate> enforced) {
        if (!safeShape(expression)) return expression;
        List<ConnectorExpression> original = conjuncts(expression);
        List<ConnectorExpression> remaining = new ArrayList<>();
        boolean removed = false;
        for (ConnectorExpression conjunct : original) {
            if (VariantPredicateCompiler.compile(conjunct, assignments).filter(enforced::contains).isPresent()) removed = true;
            else remaining.add(conjunct);
        }
        if (!removed) return expression;
        // The engine separately restores its untranslated residual. Echoing the whole opaque
        // copy would restore the exact call too. Keep every unclaimed transparent conjunct.
        remaining.removeIf(VariantFilterPushdown::opaque);
        remaining.removeIf(Constant.TRUE::equals);
        if (remaining.isEmpty()) return Constant.TRUE;
        return remaining.size() == 1 ? remaining.getFirst() : new Call(BOOLEAN, AND_FUNCTION_NAME, remaining);
    }

    public static boolean safeShape(ConnectorExpression expression) {
        List<ConnectorExpression> conjuncts = conjuncts(expression);
        if (conjuncts.size() > 256 || conjuncts.stream().filter(VariantFilterPushdown::opaque).count() > 1) return false;
        int nodes = 0;
        for (ConnectorExpression conjunct : conjuncts) {
            if (opaque(conjunct)) {
                Call copy = (Call) conjunct;
                if (!BOOLEAN.equals(copy.getType()) || copy.getArguments().isEmpty()
                        || !(copy.getArguments().getFirst() instanceof Constant payload)
                        || !(payload.getType() instanceof io.trino.spi.type.VarcharType)
                        || !(payload.getValue() instanceof io.airlift.slice.Slice)
                        || !copy.getArguments().subList(1, copy.getArguments().size()).stream()
                            .allMatch(io.trino.spi.expression.Variable.class::isInstance)) return false;
                continue;
            }
            int count = transparentNodes(conjunct, 256 - nodes);
            if (count < 0) return false;
            nodes += count;
        }
        return true;
    }

    private static int transparentNodes(ConnectorExpression expression, int budget) {
        if (budget <= 0 || opaque(expression)) return -1;
        int nodes = 1;
        for (ConnectorExpression child : expression.getChildren()) {
            int count = transparentNodes(child, budget - nodes);
            if (count < 0) return -1;
            nodes += count;
        }
        return nodes;
    }

    private static boolean opaque(ConnectorExpression expression) {
        return expression instanceof Call call && call.getFunctionName().equals(ENGINE_EXPRESSION);
    }

    static List<ConnectorExpression> conjuncts(ConnectorExpression expression) {
        // Bound traversal as well as the resulting program (including deeply nested ANDs).
        List<ConnectorExpression> pending = new ArrayList<>();
        List<ConnectorExpression> result = new ArrayList<>();
        pending.add(expression);
        int nodes = 0;
        while (!pending.isEmpty()) {
            if (++nodes > 256) return java.util.Collections.nCopies(257, expression);
            ConnectorExpression current = pending.removeLast();
            if (current instanceof Call call && call.getFunctionName().equals(AND_FUNCTION_NAME)) {
                if (call.getArguments().size() > 256) return java.util.Collections.nCopies(257, expression);
                // Stack in reverse order to preserve conjunct order.
                pending.addAll(call.getArguments().reversed());
            } else result.add(current);
        }
        return result;
    }
}

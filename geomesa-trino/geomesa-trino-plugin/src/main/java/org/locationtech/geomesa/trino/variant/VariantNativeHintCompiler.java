/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.variant;

import io.airlift.slice.Slice;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.spi.block.Block;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.expression.StandardFunctions.*;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VariantType.VARIANT;

/** Only mandatory native VARIANT equality/IN/null tests on constant key chains. Never parses opaque engine copies. */
public final class VariantNativeHintCompiler {
    private VariantNativeHintCompiler() {}

    public static List<VariantNativeHint> candidates(ConnectorExpression expression, Map<String, ColumnHandle> assignments) {
        if (!VariantFilterPushdown.safeShape(expression)) return List.of();
        return VariantFilterPushdown.conjuncts(expression).stream().map(term -> compile(term, assignments))
            .flatMap(Optional::stream).distinct().toList();
    }

    public static Optional<VariantNativeHint> compile(ConnectorExpression expression, Map<String, ColumnHandle> assignments) {
        if (!(expression instanceof Call call) || !call.getType().equals(BOOLEAN) || !builtin(call)) return Optional.empty();
        var args = call.getArguments();
        VariantNativeHint.Kind kind;
        ConnectorExpression input;
        List<Constant> values = List.of();
        if (call.getFunctionName().equals(EQUAL_OPERATOR_FUNCTION_NAME) && args.size() == 2) {
            kind = VariantNativeHint.Kind.EQUALS;
            Constant constant;
            if (args.get(1) instanceof Constant c) { input = args.getFirst(); constant = c; }
            else if (args.getFirst() instanceof Constant c) { input = args.get(1); constant = c; }
            else return Optional.empty();
            if (!constant.getType().equals(VARIANT)) return Optional.empty();
            values = List.of(constant);
        } else if (call.getFunctionName().equals(IN_PREDICATE_FUNCTION_NAME) && args.size() == 2) {
            kind = VariantNativeHint.Kind.IN;
            input = args.getFirst();
            values = constants(args.get(1));
            if (values == null || values.isEmpty()) return Optional.empty();
        } else if (call.getFunctionName().equals(IS_NULL_FUNCTION_NAME) && args.size() == 1) {
            kind = VariantNativeHint.Kind.IS_NULL;
            input = args.getFirst();
        } else if (call.getFunctionName().equals(NOT_FUNCTION_NAME) && args.size() == 1
                && args.getFirst() instanceof Call child && builtin(child) && child.getType().equals(BOOLEAN)
                && child.getFunctionName().equals(IS_NULL_FUNCTION_NAME) && child.getArguments().size() == 1) {
            kind = VariantNativeHint.Kind.IS_NOT_NULL;
            input = child.getArguments().getFirst();
        } else return Optional.empty();
        if (values.stream().anyMatch(value -> value.getValue() != null && !(value.getValue() instanceof io.trino.spi.variant.Variant))) return Optional.empty();
        Access access = access(input, assignments);
        if (access == null) return Optional.empty();
        return Optional.of(new VariantNativeHint(kind, access.column(), access.path(), values));
    }

    private static List<Constant> constants(ConnectorExpression expression) {
        if (!(expression.getType() instanceof ArrayType array) || !array.getElementType().equals(VARIANT)) return null;
        if (expression instanceof Call call && builtin(call) && call.getFunctionName().equals(ARRAY_CONSTRUCTOR_FUNCTION_NAME)) {
            if (call.getArguments().size() > 256 || call.getArguments().stream().anyMatch(arg -> !(arg instanceof Constant) || !arg.getType().equals(VARIANT))) return null;
            return call.getArguments().stream().map(Constant.class::cast).toList();
        }
        if (expression instanceof Constant constant && constant.getValue() instanceof Block block && block.getPositionCount() <= 256) {
            List<Constant> result = new ArrayList<>();
            for (int i = 0; i < block.getPositionCount(); i++) result.add(new Constant(block.isNull(i) ? null : VARIANT.getObject(block, i), VARIANT));
            return result;
        }
        return null;
    }

    private static Access access(ConnectorExpression expression, Map<String, ColumnHandle> assignments) {
        List<String> reversed = new ArrayList<>();
        ConnectorExpression current = expression;
        while (current instanceof Call call && builtin(call) && call.getType().equals(VARIANT)
                && call.getFunctionName().getName().equals("$operator$subscript") && call.getArguments().size() == 2
                && call.getArguments().getFirst().getType().equals(VARIANT)
                && call.getArguments().get(1) instanceof Constant key && key.getType() instanceof VarcharType && key.getValue() instanceof Slice encoded) {
            if (reversed.size() == 64) return null;
            String name = encoded.toStringUtf8();
            if (!utf8Slice(name).equals(encoded)) return null;
            reversed.add(name);
            current = call.getArguments().getFirst();
        }
        if (!(current instanceof Variable variable) || !variable.getType().equals(VARIANT)
                || !(assignments.get(variable.getName()) instanceof IcebergColumnHandle column)
                || !column.isBaseColumn() || !column.getType().equals(VARIANT)) return null;
        return new Access(column, List.copyOf(reversed.reversed()));
    }

    private static boolean builtin(Call call) {
        return call.getFunctionName().getCatalogSchema().isEmpty()
            || call.getFunctionName().getCatalogSchema().get().equals(new CatalogSchemaName("system", "builtin"));
    }
    private record Access(IcebergColumnHandle column, List<String> path) {}
}

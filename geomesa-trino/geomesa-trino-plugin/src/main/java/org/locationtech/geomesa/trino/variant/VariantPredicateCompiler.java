/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.variant;

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
import io.trino.spi.variant.Variant;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VariantType.VARIANT;

/** Recognizes only a complete mandatory call; callers decide conjunct scope. */
public final class VariantPredicateCompiler {
    private VariantPredicateCompiler() {}

    public static Optional<VariantPredicate> compile(ConnectorExpression expression, Map<String, ColumnHandle> assignments) {
        if (!(expression instanceof Call call) || !BOOLEAN.equals(call.getType())
                || (call.getFunctionName().getCatalogSchema().isPresent()
                    && !call.getFunctionName().getCatalogSchema().get().equals(new CatalogSchemaName("system", "builtin")))) {
            return Optional.empty();
        }
        VariantPredicate.Kind kind;
        if (call.getFunctionName().getName().equals("geomesa_variant_equals")) kind = VariantPredicate.Kind.EQUALS;
        else if (call.getFunctionName().getName().equals("geomesa_variant_any_equals")) kind = VariantPredicate.Kind.ANY_EQUALS;
        else if (call.getFunctionName().getName().equals("geomesa_variant_may_equal")) kind = VariantPredicate.Kind.MAY_EQUAL;
        else if (call.getFunctionName().getName().equals("geomesa_variant_any_may_equal")) kind = VariantPredicate.Kind.ANY_MAY_EQUAL;
        else if (call.getFunctionName().getName().equals("geomesa_variant_may_gt")) kind = VariantPredicate.Kind.MAY_GREATER_THAN;
        else if (call.getFunctionName().getName().equals("geomesa_variant_may_gte")) kind = VariantPredicate.Kind.MAY_GREATER_THAN_OR_EQUAL;
        else if (call.getFunctionName().getName().equals("geomesa_variant_may_lt")) kind = VariantPredicate.Kind.MAY_LESS_THAN;
        else if (call.getFunctionName().getName().equals("geomesa_variant_may_lte")) kind = VariantPredicate.Kind.MAY_LESS_THAN_OR_EQUAL;
        else if (call.getFunctionName().getName().equals("geomesa_variant_contains")) kind = VariantPredicate.Kind.ANY_EQUALS;
        else return Optional.empty();
        boolean contains = call.getFunctionName().getName().equals("geomesa_variant_contains");
        List<ConnectorExpression> args = call.getArguments();
        int arity = kind.array() && !contains ? 4 : 3;
        if (args.size() != arity || !(args.getFirst() instanceof Variable variable)
                || !VARIANT.equals(variable.getType())
                || !(assignments.get(variable.getName()) instanceof IcebergColumnHandle column)
                || !column.isBaseColumn() || !VARIANT.equals(column.getType())
                || !(args.getLast() instanceof Constant expected) || !VARIANT.equals(expected.getType())
                || !(expected.getValue() instanceof Variant)) return Optional.empty();
        List<String> path = path(args.get(1));
        List<String> elementPath = kind.array() && !contains ? path(args.get(2)) : List.of();
        if (path == null || elementPath == null
                || path.size() + elementPath.size() + (kind.array() ? 1 : 0) > 64) {
            return Optional.empty();
        }
        return Optional.of(new VariantPredicate(kind, column, path, elementPath, expected));
    }

    static List<String> path(ConnectorExpression expression) {
        if (!(expression instanceof Constant constant) || !(constant.getType() instanceof ArrayType array)
                || !(array.getElementType() instanceof VarcharType) || !(constant.getValue() instanceof Block block)
                || block.getPositionCount() > 64) return null;
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) return null;
            var encoded = VARCHAR.getSlice(block, i);
            String key = encoded.toStringUtf8();
            if (!io.airlift.slice.Slices.utf8Slice(key).equals(encoded)) return null;
            keys.add(key);
        }
        return List.copyOf(keys);
    }
}

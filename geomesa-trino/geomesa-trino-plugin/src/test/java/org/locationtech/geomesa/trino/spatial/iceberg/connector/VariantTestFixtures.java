/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import io.trino.plugin.iceberg.ColumnIdentity;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.plugin.iceberg.TableType;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.variant.Variant;
import io.trino.testing.TestingConnectorSession;
import org.locationtech.geomesa.trino.variant.VariantPushdownConfig;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VariantType.VARIANT;

final class VariantTestFixtures {
    static final IcebergColumnHandle DOC = new IcebergColumnHandle(ColumnIdentity.primitiveColumnIdentity(1, "doc"),
        VARIANT, List.of(), VARIANT, true, Optional.empty());
    static final IcebergColumnHandle ID = new IcebergColumnHandle(ColumnIdentity.primitiveColumnIdentity(2, "id"),
        BIGINT, List.of(), BIGINT, true, Optional.empty());
    static final IcebergColumnHandle VIS = new IcebergColumnHandle(ColumnIdentity.primitiveColumnIdentity(3, "__vis__"),
        VARCHAR, List.of(), VARCHAR, true, Optional.empty());
    static final Map<String, ColumnHandle> ASSIGNMENTS = Map.of("d", DOC, "alias", DOC, "v", VIS, "id", ID);
    static final IcebergTableHandle TABLE = table(OptionalLong.empty());
    private VariantTestFixtures() {}

    static IcebergTableHandle table(OptionalLong limit) {
        // IcebergMetadata.tableHandleForSnapshot marks ordinary coordinator scans explicitly false.
        return table(limit, Optional.of(false));
    }

    static IcebergTableHandle table(OptionalLong limit, Optional<Boolean> forAnalyze) {
        return new IcebergTableHandle("s", "docs", TableType.DATA,
            OptionalLong.empty(), "{}", OptionalInt.empty(), Map.of(), 3, TupleDomain.all(), TupleDomain.all(),
            limit, Set.of(), Optional.empty(), "file:///test/docs", Map.of(), Optional.empty(), false,
            Optional.empty(), Set.of(), forAnalyze);
    }

    static ConnectorSession session(VariantPushdownConfig.Mode mode) {
        return TestingConnectorSession.builder().setPropertyMetadata(VariantPushdownConfig.DEFAULT.sessionProperties())
            .setPropertyValues(Map.of(VariantPushdownConfig.FILTER_MODE, mode.name())).build();
    }

    static Call equalsCall(String variable, Variant expected, String... path) {
        return new Call(BOOLEAN, new FunctionName("geomesa_variant_equals"),
            List.of(new Variable(variable, VARIANT), path(path), new Constant(expected, VARIANT)));
    }

    static Constant path(String... keys) {
        var builder = VARCHAR.createBlockBuilder(null, keys.length);
        for (String key : keys) {
            if (key == null) builder.appendNull(); else VARCHAR.writeString(builder, key);
        }
        return new Constant(builder.build(), new ArrayType(VARCHAR));
    }
}

/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.variant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.parquet.VariantPath;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.spi.expression.Constant;
import io.trino.spi.variant.Metadata;
import io.trino.spi.variant.Variant;

import java.util.List;
import java.util.Objects;

import static io.trino.spi.type.VariantType.VARIANT;

/** A rejection hint only. Its original SQL expression must remain in the engine residual. */
public final class VariantNativeHint {
    public enum Kind { EQUALS, IN, IS_NULL, IS_NOT_NULL }
    private final Kind kind;
    private final IcebergColumnHandle column;
    private final List<String> path;
    private final List<Constant> values;

    @JsonCreator
    public VariantNativeHint(@JsonProperty("kind") Kind kind, @JsonProperty("column") IcebergColumnHandle column,
            @JsonProperty("path") List<String> path, @JsonProperty("values") List<Constant> values) {
        this.kind = Objects.requireNonNull(kind, "kind");
        if (!column.isBaseColumn() || !column.getType().equals(VARIANT)) throw new IllegalArgumentException("Expected a top-level VARIANT");
        this.column = column.getBaseColumn();
        this.path = List.copyOf(path);
        if (path.size() > 64 || values.size() > 256
                || (kind == Kind.EQUALS && values.size() != 1)
                || (kind == Kind.IN && values.isEmpty())
                || ((kind == Kind.IS_NULL || kind == Kind.IS_NOT_NULL) && !values.isEmpty())) {
            throw new IllegalArgumentException("Invalid native hint");
        }
        this.values = values.stream().map(value -> {
            if (!value.getType().equals(VARIANT) || (value.getValue() != null && !(value.getValue() instanceof Variant))) {
                throw new IllegalArgumentException("Expected a VARIANT constant");
            }
            Variant variant = (Variant) value.getValue();
            return new Constant(variant == null ? null : Variant.from(Metadata.from(variant.metadata().toSlice().copy()), variant.data().copy()), VARIANT);
        }).toList();
    }
    @JsonProperty public Kind kind() { return kind; }
    @JsonProperty public IcebergColumnHandle column() { return column; }
    @JsonProperty public List<String> path() { return path; }
    @JsonProperty public List<Constant> values() { return values; }
    public VariantPath demand() { return VariantPath.keys(path.toArray(String[]::new)); }

    @Override public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof VariantNativeHint that) || kind != that.kind || !column.equals(that.column)
                || !path.equals(that.path) || values.size() != that.values.size()) return false;
        for (int i = 0; i < values.size(); i++) {
            Variant a = (Variant) values.get(i).getValue();
            Variant b = (Variant) that.values.get(i).getValue();
            if (a == null || b == null) { if (a != b) return false; }
            else if (!a.data().equals(b.data()) || !a.metadata().equals(b.metadata())) return false;
        }
        return true;
    }
    @Override public int hashCode() {
        int hash = Objects.hash(kind, column, path);
        for (Constant constant : values) {
            Variant value = (Variant) constant.getValue();
            hash = 31 * hash + (value == null ? 0 : Objects.hash(value.data(), value.metadata()));
        }
        return hash;
    }
}

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

/** Serializable predicate program. Constants retain their VARIANT encoding, including numeric kinds. */
public final class VariantPredicate {
    public enum Kind {
        EQUALS, ANY_EQUALS, MAY_EQUAL, ANY_MAY_EQUAL,
        MAY_GREATER_THAN, MAY_GREATER_THAN_OR_EQUAL, MAY_LESS_THAN, MAY_LESS_THAN_OR_EQUAL;

        public boolean array() { return this == ANY_EQUALS || this == ANY_MAY_EQUAL; }
        public boolean conservative() {
            return this == MAY_EQUAL || this == ANY_MAY_EQUAL || this == MAY_GREATER_THAN
                || this == MAY_GREATER_THAN_OR_EQUAL || this == MAY_LESS_THAN || this == MAY_LESS_THAN_OR_EQUAL;
        }
    }

    private final Kind kind;
    private final IcebergColumnHandle column;
    private final List<String> path;
    private final List<String> elementPath;
    private final Constant expected;
    private final VariantReferenceEvaluator.KeyPath compiledPath;
    private final VariantReferenceEvaluator.KeyPath compiledElementPath;

    @JsonCreator
    public VariantPredicate(@JsonProperty("kind") Kind kind,
            @JsonProperty("column") IcebergColumnHandle column,
            @JsonProperty("path") List<String> path,
            @JsonProperty("elementPath") List<String> elementPath,
            @JsonProperty("expected") Constant expected) {
        this.kind = Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(column, "column");
        if (!column.isBaseColumn() || !VARIANT.equals(column.getType())) {
            throw new IllegalArgumentException("Predicate source must be a top-level VARIANT");
        }
        // Physical identity, independent of the current projection's path demand and comment.
        this.column = column.getBaseColumn();
        this.path = List.copyOf(path);
        this.elementPath = List.copyOf(elementPath);
        if (path.size() + elementPath.size() + (kind.array() ? 1 : 0) > 64
                || (!kind.array() && !elementPath.isEmpty())) {
            throw new IllegalArgumentException("Invalid predicate path");
        }
        if (!VARIANT.equals(expected.getType()) || !(expected.getValue() instanceof Variant value)) {
            throw new IllegalArgumentException("Expected a non-SQL-null VARIANT constant");
        }
        this.expected = new Constant(Variant.from(Metadata.from(value.metadata().toSlice().copy()), value.data().copy()), VARIANT);
        this.compiledPath = VariantReferenceEvaluator.compilePath(this.path);
        this.compiledElementPath = VariantReferenceEvaluator.compilePath(this.elementPath);
    }

    @JsonProperty public Kind kind() { return kind; }
    @JsonProperty public IcebergColumnHandle column() { return column; }
    @JsonProperty public List<String> path() { return path; }
    @JsonProperty public List<String> elementPath() { return elementPath; }
    @JsonProperty public Constant expected() { return expected; }

    public VariantPath demand() {
        VariantPath demand = VariantPath.keys(path.toArray(String[]::new));
        if (kind.array()) {
            demand = demand.append(VariantPath.Step.element());
            for (String key : elementPath) demand = demand.append(VariantPath.Step.key(key));
        }
        return demand;
    }

    public boolean test(Variant document) {
        Variant value = (Variant) expected.getValue();
        return switch (kind) {
            case EQUALS -> VariantReferenceEvaluator.equals(document, compiledPath, value);
            case ANY_EQUALS -> VariantReferenceEvaluator.anyEquals(document, compiledPath, compiledElementPath, value);
            case MAY_EQUAL -> VariantReferenceEvaluator.mayEqual(document, compiledPath, value);
            case ANY_MAY_EQUAL -> VariantReferenceEvaluator.anyMayEqual(document, compiledPath, compiledElementPath, value);
            case MAY_GREATER_THAN -> VariantReferenceEvaluator.mayCompare(document, compiledPath, value, VariantReferenceEvaluator.Comparison.GREATER_THAN);
            case MAY_GREATER_THAN_OR_EQUAL -> VariantReferenceEvaluator.mayCompare(document, compiledPath, value, VariantReferenceEvaluator.Comparison.GREATER_THAN_OR_EQUAL);
            case MAY_LESS_THAN -> VariantReferenceEvaluator.mayCompare(document, compiledPath, value, VariantReferenceEvaluator.Comparison.LESS_THAN);
            case MAY_LESS_THAN_OR_EQUAL -> VariantReferenceEvaluator.mayCompare(document, compiledPath, value, VariantReferenceEvaluator.Comparison.LESS_THAN_OR_EQUAL);
        };
    }

    @Override public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof VariantPredicate that)) return false;
        Variant left = (Variant) expected.getValue();
        Variant right = (Variant) that.expected.getValue();
        // Program identity must be reflexive even for NaN; runtime numeric equality is not.
        return kind == that.kind && column.equals(that.column) && path.equals(that.path)
            && elementPath.equals(that.elementPath) && left.data().equals(right.data())
            && left.metadata().equals(right.metadata());
    }

    @Override public int hashCode() {
        Variant value = (Variant) expected.getValue();
        return Objects.hash(kind, column, path, elementPath, value.data(), value.metadata());
    }
}

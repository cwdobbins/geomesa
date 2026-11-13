/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import org.locationtech.geomesa.trino.variant.VariantPredicate;
import org.locationtech.geomesa.trino.variant.VariantNativeHint;

import java.util.List;
import java.util.Objects;

/** Outermost scan filter; deletes, spatial and visibility filtering execute before these predicates. */
public record VariantTableHandle(ConnectorTableHandle delegate, List<VariantPredicate> predicates, List<VariantNativeHint> hints) implements ConnectorTableHandle {
    public VariantTableHandle(ConnectorTableHandle delegate, List<VariantPredicate> predicates) {
        this(delegate, predicates, List.of());
    }

    @JsonCreator
    public VariantTableHandle(@JsonProperty("delegate") ConnectorTableHandle delegate,
            @JsonProperty("predicates") List<VariantPredicate> predicates, @JsonProperty("hints") List<VariantNativeHint> hints) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        if (!(delegate instanceof IcebergTableHandle || delegate instanceof SpatialTableHandle
                || delegate instanceof VisibilityTableHandle)) throw new IllegalArgumentException("Invalid VARIANT delegate");
        this.predicates = predicates.stream().distinct().toList();
        this.hints = hints == null ? List.of() : hints.stream().distinct().toList();
        if ((this.predicates.isEmpty() && this.hints.isEmpty()) || this.predicates.size() + this.hints.size() > 256) throw new IllegalArgumentException("Invalid predicate count");
    }
    @JsonProperty public ConnectorTableHandle delegate() { return delegate; }
    @JsonProperty public List<VariantPredicate> predicates() { return predicates; }

    @JsonProperty public List<VariantNativeHint> hints() { return hints; }

    VariantTableHandle withDelegate(ConnectorTableHandle handle) {
        return new VariantTableHandle(handle, predicates, hints);
    }
}

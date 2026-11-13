/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.variant;

import io.trino.parquet.reader.VariantBatchAccess;
import io.trino.parquet.reader.VariantPathProbe;
import io.trino.spi.variant.Variant;

import java.util.List;

/** Native hints can reject only proved nonmatches. UNKNOWN never becomes an exception or a rejection. */
public final class VariantNativeHintEvaluator implements VariantRuntimeEvaluator {
    private final List<VariantNativeHint> hints;
    private long directRows;
    private long unknownRows;

    public VariantNativeHintEvaluator(List<VariantNativeHint> hints) {
        this.hints = List.copyOf(hints);
        if (hints.isEmpty() || hints.stream().anyMatch(h -> !h.column().equals(hints.getFirst().column()))) {
            throw new IllegalArgumentException("Expected hints on one physical VARIANT column");
        }
    }

    @Override public Decision evaluate(VariantBatchAccess batch, int originalPosition) {
        boolean unknown = false;
        for (VariantNativeHint hint : hints) {
            Decision decision = decide(hint, batch.probe(hint.demand(), originalPosition).getFirst());
            if (decision == Decision.FALSE || decision == Decision.SQL_NULL) {
                directRows++;
                return decision;
            }
            unknown |= decision == Decision.UNKNOWN;
        }
        if (unknown) { unknownRows++; return Decision.UNKNOWN; }
        directRows++;
        return Decision.TRUE;
    }

    private static Decision decide(VariantNativeHint hint, VariantPathProbe probe) {
        var state = probe.state();
        if (state == VariantPathProbe.State.UNRESOLVED || state == VariantPathProbe.State.WRONG_SHAPE) return Decision.UNKNOWN;
        boolean sqlNull = state == VariantPathProbe.State.SQL_NULL || state == VariantPathProbe.State.MISSING;
        if (hint.kind() == VariantNativeHint.Kind.IS_NULL) return sqlNull ? Decision.TRUE : Decision.FALSE;
        if (hint.kind() == VariantNativeHint.Kind.IS_NOT_NULL) return sqlNull ? Decision.FALSE : Decision.TRUE;
        if (sqlNull) return Decision.SQL_NULL;
        if (!VariantTypedEvaluator.supported(probe.type())) return Decision.UNKNOWN;
        Variant actual = probe.value();
        boolean hasNull = false;
        for (var constant : hint.values()) {
            Variant expected = (Variant) constant.getValue();
            if (expected == null) hasNull = true;
            else if (actual.equals(expected)) return Decision.TRUE;
        }
        return hasNull ? Decision.SQL_NULL : Decision.FALSE;
    }

    @Override public Decision evaluateReference(Variant value) { unknownRows++; return Decision.UNKNOWN; }
    @Override public boolean exact() { return false; }
    @Override public long directRows() { return directRows; }
    @Override public long fallbackRows() { return 0; }
    @Override public long unknownRows() { return unknownRows; }
}

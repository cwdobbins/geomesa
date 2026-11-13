/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.variant;

import io.trino.parquet.reader.VariantPredicateEvaluator;

/** Worker-local execution and counters for an exact predicate or a conservative native hint. */
public interface VariantRuntimeEvaluator extends VariantPredicateEvaluator {
    boolean exact();
    long directRows();
    long fallbackRows();
    default long unknownRows() { return 0; }
}

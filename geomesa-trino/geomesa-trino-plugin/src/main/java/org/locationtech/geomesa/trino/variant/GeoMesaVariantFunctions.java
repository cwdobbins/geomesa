/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.variant;

import io.trino.spi.block.Block;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.variant.Variant;

/** Total object-path functions: a missing key or a mismatched shape is never a SQL type error. */
public final class GeoMesaVariantFunctions {

    private GeoMesaVariantFunctions() {}

    @ScalarFunction("geomesa_variant_get")
    @Description("Read an exact object-key path, returning SQL NULL for missing keys or mismatched shapes")
    @SqlNullable
    @SqlType("variant")
    public static Variant get(@SqlNullable @SqlType("variant") Variant document,
                              @SqlNullable @SqlType("array(varchar)") Block path) {
        return VariantReferenceEvaluator.get(document, VariantReferenceEvaluator.compilePath(path));
    }

    @ScalarFunction("geomesa_variant_equals")
    @Description("Test variant equality at an exact object-key path; missing keys, mismatched shapes and SQL NULL arguments give false")
    @SqlType("boolean")
    public static boolean equals(@SqlNullable @SqlType("variant") Variant document,
                                 @SqlNullable @SqlType("array(varchar)") Block path,
                                 @SqlNullable @SqlType("variant") Variant expected) {
        return VariantReferenceEvaluator.equals(document, VariantReferenceEvaluator.compilePath(path), expected);
    }

    @ScalarFunction("geomesa_variant_any_equals")
    @Description("Test whether any array element has an exact object-key path equal to the expected variant")
    @SqlType("boolean")
    public static boolean anyEquals(@SqlNullable @SqlType("variant") Variant document,
                                    @SqlNullable @SqlType("array(varchar)") Block listPath,
                                    @SqlNullable @SqlType("array(varchar)") Block elementPath,
                                    @SqlNullable @SqlType("variant") Variant expected) {
        return VariantReferenceEvaluator.anyEquals(document, VariantReferenceEvaluator.compilePath(listPath),
                VariantReferenceEvaluator.compilePath(elementPath), expected);
    }
    @ScalarFunction("geomesa_variant_contains")
    @Description("Test exact VARIANT membership in the array at an object-key path")
    @SqlType("boolean")
    public static boolean contains(@SqlNullable @SqlType("variant") Variant document,
                                    @SqlNullable @SqlType("array(varchar)") Block path,
                                    @SqlNullable @SqlType("variant") Variant expected) {
        return VariantReferenceEvaluator.anyEquals(document, VariantReferenceEvaluator.compilePath(path),
            VariantReferenceEvaluator.compilePath(java.util.List.of()), expected);
    }

    @ScalarFunction("geomesa_variant_fields_equal")
    @Description("Test exact equality beneath any immediate field of an object; missing or wrong shapes give false")
    @SqlType("boolean")
    public static boolean fieldsEqual(@SqlNullable @SqlType("variant") Variant document,
                                      @SqlNullable @SqlType("array(varchar)") Block objectPath,
                                      @SqlNullable @SqlType("array(varchar)") Block valuePath,
                                      @SqlNullable @SqlType("variant") Variant expected) {
        return VariantReferenceEvaluator.fieldsEqual(document, VariantReferenceEvaluator.compilePath(objectPath),
            VariantReferenceEvaluator.compilePath(valuePath), expected);
    }

    @ScalarFunction("geomesa_variant_may_equal")
    @Description("GeoTools candidate filter: reject only a proved unequal string; retain uncertain values for the residual")
    @SqlType("boolean")
    public static boolean mayEqual(@SqlNullable @SqlType("variant") Variant document,
                                   @SqlNullable @SqlType("array(varchar)") Block path,
                                   @SqlNullable @SqlType("variant") Variant expected) {
        return VariantReferenceEvaluator.mayEqual(document, VariantReferenceEvaluator.compilePath(path), expected);
    }

    @ScalarFunction("geomesa_variant_any_may_equal")
    @Description("GeoTools array candidate filter: reject only nonempty selections of proved unequal strings")
    @SqlType("boolean")
    public static boolean anyMayEqual(@SqlNullable @SqlType("variant") Variant document,
                                      @SqlNullable @SqlType("array(varchar)") Block listPath,
                                      @SqlNullable @SqlType("array(varchar)") Block elementPath,
                                      @SqlNullable @SqlType("variant") Variant expected) {
        return VariantReferenceEvaluator.anyMayEqual(document, VariantReferenceEvaluator.compilePath(listPath),
            VariantReferenceEvaluator.compilePath(elementPath), expected);
    }

    @ScalarFunction("geomesa_variant_fields_may_equal")
    @Description("GeoTools object-field candidate filter: retain uncertain or empty selections for the residual")
    @SqlType("boolean")
    public static boolean fieldsMayEqual(@SqlNullable @SqlType("variant") Variant document,
                                         @SqlNullable @SqlType("array(varchar)") Block objectPath,
                                         @SqlNullable @SqlType("array(varchar)") Block valuePath,
                                         @SqlNullable @SqlType("variant") Variant expected) {
        return VariantReferenceEvaluator.fieldsMayEqual(document, VariantReferenceEvaluator.compilePath(objectPath),
            VariantReferenceEvaluator.compilePath(valuePath), expected);
    }

    @ScalarFunction("geomesa_variant_may_gt")
    @Description("Conservative numeric greater-than candidate; uncertain VARIANT values remain for the residual")
    @SqlType("boolean")
    public static boolean mayGreaterThan(@SqlNullable @SqlType("variant") Variant document,
                                         @SqlNullable @SqlType("array(varchar)") Block path,
                                         @SqlNullable @SqlType("variant") Variant expected) {
        return VariantReferenceEvaluator.mayCompare(document, VariantReferenceEvaluator.compilePath(path), expected,
            VariantReferenceEvaluator.Comparison.GREATER_THAN);
    }

    @ScalarFunction("geomesa_variant_may_gte")
    @Description("Conservative numeric greater-than-or-equal candidate; uncertain values remain for the residual")
    @SqlType("boolean")
    public static boolean mayGreaterThanOrEqual(@SqlNullable @SqlType("variant") Variant document,
                                                @SqlNullable @SqlType("array(varchar)") Block path,
                                                @SqlNullable @SqlType("variant") Variant expected) {
        return VariantReferenceEvaluator.mayCompare(document, VariantReferenceEvaluator.compilePath(path), expected,
            VariantReferenceEvaluator.Comparison.GREATER_THAN_OR_EQUAL);
    }

    @ScalarFunction("geomesa_variant_may_lt")
    @Description("Conservative numeric less-than candidate; uncertain VARIANT values remain for the residual")
    @SqlType("boolean")
    public static boolean mayLessThan(@SqlNullable @SqlType("variant") Variant document,
                                      @SqlNullable @SqlType("array(varchar)") Block path,
                                      @SqlNullable @SqlType("variant") Variant expected) {
        return VariantReferenceEvaluator.mayCompare(document, VariantReferenceEvaluator.compilePath(path), expected,
            VariantReferenceEvaluator.Comparison.LESS_THAN);
    }

    @ScalarFunction("geomesa_variant_may_lte")
    @Description("Conservative numeric less-than-or-equal candidate; uncertain values remain for the residual")
    @SqlType("boolean")
    public static boolean mayLessThanOrEqual(@SqlNullable @SqlType("variant") Variant document,
                                             @SqlNullable @SqlType("array(varchar)") Block path,
                                             @SqlNullable @SqlType("variant") Variant expected) {
        return VariantReferenceEvaluator.mayCompare(document, VariantReferenceEvaluator.compilePath(path), expected,
            VariantReferenceEvaluator.Comparison.LESS_THAN_OR_EQUAL);
    }

}

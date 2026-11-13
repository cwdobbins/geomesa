/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.variant;

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.variant.Variant;

import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.variant.Header.BasicType.ARRAY;
import static io.trino.spi.variant.Header.BasicType.OBJECT;

/** Complete semantics shared by SQL functions and authoritative scan predicates. */
public final class VariantReferenceEvaluator {

    public enum Comparison { GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL }

    private VariantReferenceEvaluator() {}

    /** Compiled exact object keys. Invalid/null SQL paths are represented by null, not an empty path. */
    public static final class KeyPath {
        private final Slice[] keys;

        private KeyPath(Slice[] keys) {
            this.keys = keys;
        }
    }

    public static KeyPath compilePath(List<String> keys) {
        if (keys == null) return null;
        Slice[] encoded = new Slice[keys.size()];
        for (int i = 0; i < keys.size(); i++) {
            if (keys.get(i) == null) return null;
            encoded[i] = utf8Slice(keys.get(i));
        }
        return new KeyPath(encoded);
    }

    public static KeyPath compilePath(Block keys) {
        if (keys == null) return null;
        Slice[] encoded = new Slice[keys.getPositionCount()];
        for (int i = 0; i < encoded.length; i++) {
            if (keys.isNull(i)) return null;
            encoded[i] = VARCHAR.getSlice(keys, i).copy();
        }
        return new KeyPath(encoded);
    }

    public static Variant get(Variant document, KeyPath path) {
        if (path == null) return null;
        Variant current = document;
        for (Slice key : path.keys) {
            if (current == null || current.basicType() != OBJECT) return null;
            current = current.getObjectField(key).orElse(null);
        }
        return current;
    }

    public static boolean equals(Variant document, KeyPath path, Variant expected) {
        if (expected == null) return false;
        Variant actual = get(document, path);
        return actual != null && actual.equals(expected);
    }

    public static boolean anyEquals(Variant document, KeyPath listPath, KeyPath elementPath, Variant expected) {
        if (elementPath == null || expected == null) return false;
        Variant list = get(document, listPath);
        if (list == null || list.basicType() != ARRAY) return false;
        for (int element = 0; element < list.getArrayLength(); element++) {
            if (equals(list.getArrayElement(element), elementPath, expected)) return true;
        }
        return false;
    }
    /** Conservative GeoTools candidate test: only unequal strings are proved nonmatches. */
    public static boolean stringsMayEqual(Variant actual, Variant expected) {
        return !isString(actual) || !isString(expected) || actual.equals(expected);
    }

    private static boolean isString(Variant value) {
        return value != null && (value.basicType() == io.trino.spi.variant.Header.BasicType.SHORT_STRING
            || value.primitiveType() == io.trino.spi.variant.Header.PrimitiveType.STRING);
    }

    public static boolean mayEqual(Variant document, KeyPath path, Variant expected) {
        return stringsMayEqual(get(document, path), expected);
    }

    public static boolean anyMayEqual(Variant document, KeyPath listPath, KeyPath elementPath, Variant expected) {
        Variant list = get(document, listPath);
        if (elementPath == null || list == null || list.basicType() != ARRAY || list.getArrayLength() == 0) return true;
        for (int element = 0; element < list.getArrayLength(); element++) {
            if (mayEqual(list.getArrayElement(element), elementPath, expected)) return true;
        }
        return false;
    }

    /** One object-field wildcard, with each selected value retaining an independent key-path scope. */
    public static boolean fieldsEqual(Variant document, KeyPath objectPath, KeyPath valuePath, Variant expected) {
        Variant object = get(document, objectPath);
        return valuePath != null && expected != null && object != null && object.basicType() == OBJECT
            && object.objectValues().anyMatch(value -> equals(value, valuePath, expected));
    }

    public static boolean fieldsMayEqual(Variant document, KeyPath objectPath, KeyPath valuePath, Variant expected) {
        Variant object = get(document, objectPath);
        // Jayway wildcard behavior on nonobjects is deliberately left to the GeoTools residual.
        return valuePath == null || object == null || object.basicType() != OBJECT || object.getObjectFieldCount() == 0
            || object.objectValues().anyMatch(value -> mayEqual(value, valuePath, expected));
    }

    /** Conservative numeric ranges under GeoTools coercion; unsupported and non-finite values stay candidates. */
    public static boolean mayCompare(Variant document, KeyPath path, Variant expected, Comparison comparison) {
        return mayCompareValues(get(document, path), expected, comparison);
    }

    public static boolean mayCompareValues(Variant actual, Variant expected, Comparison comparison) {
        NumericBounds left = numericBounds(actual, isFloat(expected));
        NumericBounds right = numericBounds(expected, isFloat(actual));
        if (left == null || right == null) return true;
        // Reject only if every supported coercion proves the comparison false. In particular,
        // GeoTools widens Float via its decimal text, but can also round Integer to Float.
        return switch (comparison) {
            case GREATER_THAN -> left.maximum > right.minimum;
            case GREATER_THAN_OR_EQUAL -> left.maximum >= right.minimum;
            case LESS_THAN -> left.minimum < right.maximum;
            case LESS_THAN_OR_EQUAL -> left.minimum <= right.maximum;
        };
    }

    private record NumericBounds(double minimum, double maximum) {}

    private static boolean isFloat(Variant value) {
        return value != null && value.basicType() == io.trino.spi.variant.Header.BasicType.PRIMITIVE
                && value.primitiveType() == io.trino.spi.variant.Header.PrimitiveType.FLOAT;
    }

    static boolean supportedNumber(Variant value) {
        return numericBounds(value, false) != null;
    }

    private static NumericBounds numericBounds(Variant value, boolean otherIsFloat) {
        if (value == null || value.basicType() != io.trino.spi.variant.Header.BasicType.PRIMITIVE) return null;
        double number;
        var integral = integer(value);
        if (integral.isPresent()) {
            long integer = integral.getAsLong();
            if (integer < -(1L << 53) || integer > (1L << 53)) return null;
            number = integer;
        } else {
            number = switch (value.primitiveType()) {
                case FLOAT -> value.getFloat();
                case DOUBLE -> value.getDouble();
                case DECIMAL4, DECIMAL8, DECIMAL16 -> value.getDecimal().doubleValue();
                default -> Double.NaN;
            };
        }
        if (!Double.isFinite(number)) return null;
        double min = number;
        double max = number;
        if (isFloat(value)) {
            double text = Double.parseDouble(Float.toString(value.getFloat()));
            min = Math.min(min, text);
            max = Math.max(max, text);
        }
        if (otherIsFloat && integral.isPresent()) {
            // The JSON and VARIANT parsers can choose different integer widths. Cover both
            // GeoTools' integer-to-float conversion and widening to double.
            double rounded = (float) number;
            min = Math.min(min, rounded);
            max = Math.max(max, rounded);
        }
        return new NumericBounds(min, max);
    }

    static java.util.OptionalLong integer(Variant value) {
        if (value == null || value.basicType() != io.trino.spi.variant.Header.BasicType.PRIMITIVE) {
            return java.util.OptionalLong.empty();
        }
        return switch (value.primitiveType()) {
            case INT8 -> java.util.OptionalLong.of(value.getByte());
            case INT16 -> java.util.OptionalLong.of(value.getShort());
            case INT32 -> java.util.OptionalLong.of(value.getInt());
            case INT64 -> java.util.OptionalLong.of(value.getLong());
            default -> java.util.OptionalLong.empty();
        };
    }

}

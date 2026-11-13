/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.filter.Filter;
import org.geotools.api.filter.PropertyIsGreaterThan;
import org.geotools.api.filter.PropertyIsGreaterThanOrEqualTo;
import org.geotools.api.filter.PropertyIsLessThan;
import org.geotools.api.filter.PropertyIsLessThanOrEqualTo;
import org.geotools.api.filter.PropertyIsEqualTo;
import org.geotools.api.filter.expression.Expression;
import org.geotools.api.filter.expression.Literal;
import org.geotools.api.filter.expression.PropertyName;
import org.locationtech.geomesa.utils.json.JsonPathFilterFunction;
import org.locationtech.geomesa.utils.json.JsonPathParser;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.math.BigDecimal;

/** Bounded necessary conditions for GeoTools filters, never replacements for their residual semantics. */
final class VariantFilterSql {
    private VariantFilterSql() {}

    static String prefilter(Filter filter, SimpleFeatureType schema) {
        String range = rangePrefilter(filter, schema);
        if (range != null) return range;
        if (!(filter instanceof PropertyIsEqualTo equal) || !equal.isMatchingCase()) return null;
        Expression source;
        Literal literal;
        if (equal.getExpression2() instanceof Literal value) {
            source = equal.getExpression1(); literal = value;
        } else if (equal.getExpression1() instanceof Literal value) {
            source = equal.getExpression2(); literal = value;
        } else return null;
        if (!(literal.getValue() instanceof String expected)) return null;
        String path = path(source);
        if (path == null || !path.startsWith("$.")) return null;
        var parsed = JsonPathParser.parse(path, true);
        if (parsed.isEmpty() || parsed.function().isDefined()) return null;
        var elements = JavaConverters.seqAsJavaList(parsed.elements());
        if (elements.size() < 2 || elements.size() > 65
                || !(elements.get(0) instanceof JsonPathParser.PathAttribute head) || schema == null) return null;
        var descriptor = schema.getDescriptor(head.name());
        if (descriptor == null || descriptor.getType().getBinding() != String.class
                || !"true".equals(descriptor.getUserData().get(TrinoTypeMapper.OPT_JSON))
                || descriptor.getUserData().get(TrinoTypeMapper.OPT_JSON_SCHEMA) != null) return null;
        List<String> prefix = new ArrayList<>();
        List<String> suffix = new ArrayList<>();
        String wildcard = null;
        for (var element : elements.subList(1, elements.size())) {
            if (element instanceof JsonPathParser.PathAttribute key) {
                (wildcard == null ? prefix : suffix).add(key.name());
            } else if (wildcard == null && element instanceof JsonPathParser.PathIndexWildCard$) {
                wildcard = "any";
            } else if (wildcard == null && element instanceof JsonPathParser.PathAttributeWildCard$) {
                wildcard = "fields";
            } else return null;
        }
        String function = wildcard == null ? "geomesa_variant_may_equal" : "geomesa_variant_" + wildcard + "_may_equal";
        return function + "(" + identifier(head.name()) + ", " + array(prefix)
            + (wildcard == null ? "" : ", " + array(suffix)) + ", CAST(" + literal(expected) + " AS VARIANT))";
    }

    private static String rangePrefilter(Filter filter, SimpleFeatureType schema) {
        if (!(filter instanceof PropertyIsGreaterThan || filter instanceof PropertyIsGreaterThanOrEqualTo
                || filter instanceof PropertyIsLessThan || filter instanceof PropertyIsLessThanOrEqualTo)) return null;
        var comparison = (org.geotools.api.filter.BinaryComparisonOperator) filter;
        Expression source;
        Literal literal;
        boolean reverse;
        if (comparison.getExpression2() instanceof Literal value) {
            source = comparison.getExpression1(); literal = value; reverse = false;
        } else if (comparison.getExpression1() instanceof Literal value) {
            source = comparison.getExpression2(); literal = value; reverse = true;
        } else return null;
        if (!(literal.getValue() instanceof Number number)) return null;
        String numeric = numericLiteral(number);
        if (numeric == null) return null;
        String path = path(source);
        if (path == null || !path.startsWith("$.")) return null;
        var parsed = JsonPathParser.parse(path, true);
        if (parsed.isEmpty() || parsed.function().isDefined()) return null;
        var elements = JavaConverters.seqAsJavaList(parsed.elements());
        if (elements.size() < 2 || elements.size() > 65
                || !(elements.get(0) instanceof JsonPathParser.PathAttribute head) || schema == null) return null;
        var descriptor = schema.getDescriptor(head.name());
        if (descriptor == null || descriptor.getType().getBinding() != String.class
                || !"true".equals(descriptor.getUserData().get(TrinoTypeMapper.OPT_JSON))
                || descriptor.getUserData().get(TrinoTypeMapper.OPT_JSON_SCHEMA) != null) return null;
        List<String> keys = new ArrayList<>();
        for (var element : elements.subList(1, elements.size())) {
            if (!(element instanceof JsonPathParser.PathAttribute key)) return null;
            keys.add(key.name());
        }
        String function;
        if (filter instanceof PropertyIsGreaterThan) function = reverse ? "geomesa_variant_may_lt" : "geomesa_variant_may_gt";
        else if (filter instanceof PropertyIsGreaterThanOrEqualTo) function = reverse ? "geomesa_variant_may_lte" : "geomesa_variant_may_gte";
        else if (filter instanceof PropertyIsLessThan) function = reverse ? "geomesa_variant_may_gt" : "geomesa_variant_may_lt";
        else function = reverse ? "geomesa_variant_may_gte" : "geomesa_variant_may_lte";
        return function + "(" + identifier(head.name()) + ", " + array(keys) + ", " + numeric + ")";
    }

    private static String numericLiteral(Number number) {
        if (number instanceof Double value) {
            return Double.isFinite(value) ? "CAST(CAST(" + literal(value.toString()) + " AS DOUBLE) AS VARIANT)" : null;
        }
        if (number instanceof Float value) {
            return Float.isFinite(value) ? "CAST(CAST(" + literal(value.toString()) + " AS REAL) AS VARIANT)" : null;
        }
        if (number instanceof BigDecimal value) {
            // Validate before expanding scientific notation, which can otherwise allocate huge strings.
            long scale = Math.max(0L, value.scale());
            long precision = Math.max(scale, (long) value.precision() + Math.max(0L, -(long) value.scale()));
            if (precision > 38 || scale > 38) return null;
            if (value.scale() < 0) value = value.setScale(0);
            return "CAST(CAST(" + literal(value.toPlainString()) + " AS DECIMAL(" + precision + ", " + scale + ")) AS VARIANT)";
        }
        if (!(number instanceof Byte || number instanceof Short || number instanceof Integer || number instanceof Long)) return null;
        BigDecimal decimal = new BigDecimal(number.toString());
        if (decimal.abs().compareTo(BigDecimal.valueOf(9_007_199_254_740_992L)) > 0) return null;
        String type = number instanceof Byte ? "TINYINT" : number instanceof Short ? "SMALLINT"
            : number instanceof Integer ? "INTEGER" : "BIGINT";
        return "CAST(CAST(" + decimal.toPlainString() + " AS " + type + ") AS VARIANT)";
    }

    /** Static jsonPath forms only: never evaluate an arbitrary expression while planning. */
    private static String path(Expression expression) {
        if (expression instanceof PropertyName property) return property.getPropertyName();
        if (!(expression instanceof JsonPathFilterFunction function)) return null;
        var args = function.getParameters();
        if (args.size() < 1 || args.size() > 2) return null;
        String base;
        if (args.get(0) instanceof PropertyName property) base = property.getPropertyName();
        else if (args.get(0) instanceof Literal literal && literal.getValue() instanceof String value) base = value;
        else return null;
        if (args.size() == 1) return base;
        if (!(args.get(1) instanceof Literal literal) || !(literal.getValue() instanceof String nested)) return null;
        return "$." + base + "." + nested;
    }

    private static String array(List<String> keys) {
        // Typed empty arrays survive analysis and remain constant arrays for connector compilation.
        return keys.isEmpty() ? "CAST(ARRAY[] AS ARRAY(VARCHAR))"
            : keys.stream().map(VariantFilterSql::literal).collect(Collectors.joining(", ", "ARRAY[", "]"));
    }
    private static String literal(String value) { return "'" + value.replace("'", "''") + "'"; }
    private static String identifier(String value) { return "\"" + value.replace("\"", "\"\"") + "\""; }
}

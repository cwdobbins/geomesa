/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.variant;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Opt-in exact VARIANT filtering and projection settings. */
public record VariantPushdownConfig(Mode filterMode, boolean projectionPushdownEnabled, boolean nativePrefilterEnabled) {
    public VariantPushdownConfig {
        java.util.Objects.requireNonNull(filterMode, "filterMode");
    }

    public VariantPushdownConfig(Mode filterMode, boolean projectionPushdownEnabled) {
        this(filterMode, projectionPushdownEnabled, false);
    }

    public VariantPushdownConfig(Mode filterMode) {
        this(filterMode, false);
    }

    public enum Mode { OFF, REFERENCE, TYPED }
    public static final String PREFIX = "geomesa.variant.";
    public static final String PROJECTION_ENABLED = "variant_projection_pushdown_enabled";
    public static final String NATIVE_ENABLED = "variant_native_prefilter_enabled";
    public static final String FILTER_MODE = "variant_filter_mode";
    public static final VariantPushdownConfig DEFAULT = new VariantPushdownConfig(Mode.OFF);

    public static VariantPushdownConfig from(Map<String, String> config) {
        Set<String> known = Set.of(PREFIX + "filter-mode", PREFIX + "projection-pushdown-enabled", PREFIX + "native-prefilter-enabled");
        config.keySet().stream().filter(key -> key.startsWith(PREFIX) && !known.contains(key)).findFirst()
            .ifPresent(key -> { throw new IllegalArgumentException("Unknown VARIANT property: " + key); });
        String projection = config.getOrDefault(PREFIX + "projection-pushdown-enabled", "false");
        if (!projection.equalsIgnoreCase("true") && !projection.equalsIgnoreCase("false")) {
            throw new IllegalArgumentException("geomesa.variant.projection-pushdown-enabled must be true or false");
        }
        String nativePrefilter = config.getOrDefault(PREFIX + "native-prefilter-enabled", "false");
        if (!nativePrefilter.equalsIgnoreCase("true") && !nativePrefilter.equalsIgnoreCase("false")) {
            throw new IllegalArgumentException("geomesa.variant.native-prefilter-enabled must be true or false");
        }
        try {
            return new VariantPushdownConfig(Mode.valueOf(config.getOrDefault(PREFIX + "filter-mode", "off").toUpperCase(Locale.ROOT)), Boolean.parseBoolean(projection), Boolean.parseBoolean(nativePrefilter));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("geomesa.variant.filter-mode must be off, reference or typed", e);
        }
    }

    public List<PropertyMetadata<?>> sessionProperties() {
        return List.of(PropertyMetadata.enumProperty(FILTER_MODE,
            "Enforce total GeoMesa VARIANT predicates in the connector (off, reference or typed)", Mode.class, filterMode, false),
            PropertyMetadata.booleanProperty(PROJECTION_ENABLED,
                "Read only VARIANT paths required by projected SQL expressions", projectionPushdownEnabled, false),
            PropertyMetadata.booleanProperty(NATIVE_ENABLED,
                "Conservatively prefilter native VARIANT predicates in typed mode; keep engine evaluation", nativePrefilterEnabled, false));
    }

    public static boolean projectionEnabled(ConnectorSession session) {
        return session.getProperty(PROJECTION_ENABLED, Boolean.class);
    }

    public static boolean nativeEnabled(ConnectorSession session) {
        return session.getProperty(FILTER_MODE, Mode.class) == Mode.TYPED && session.getProperty(NATIVE_ENABLED, Boolean.class);
    }

    public static boolean enabled(ConnectorSession session) {
        return session.getProperty(FILTER_MODE, Mode.class) != Mode.OFF;
    }
}

/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.parquet.TypeToMessageType;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.parquet.variant.VariantJsonParser;
import org.apache.parquet.variant.VariantValueWriter;
import org.geotools.api.data.DataStoreFinder;
import org.geotools.api.data.Query;
import org.geotools.api.data.Transaction;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.filter.text.ecql.ECQL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.locationtech.geomesa.features.ScalaSimpleFeature;
import org.locationtech.geomesa.index.conf.QueryHints;
import org.locationtech.geomesa.trino.datastore.testcontainers.GeoMesaTrinoContainer;
import org.locationtech.geomesa.trino.datastore.testcontainers.IcebergRestContainer;
import org.locationtech.geomesa.trino.datastore.testcontainers.SeaweedFsContainer;
import org.locationtech.geomesa.utils.geotools.FeatureUtils;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Network;
import org.testcontainers.trino.TrinoContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end check that the plugin reads VARIANT columns stored in the shredded layout (a
 * {@code typed_value} child beside {@code metadata} and {@code value}).
 *
 * <p>The plugin directory carries its own copies of the Iceberg connector and the Parquet library
 * (patched to read shredded variants), while the server is the stock {@code trinodb/trino} image, so
 * this test is what shows the patched classes take effect when the plugin is mounted into it.
 *
 * <p>Fixture: an Iceberg v3 table created through the GeoMesa file system data store (so it carries the
 * column specs the data store discovers), holding an unshredded data file written by GeoMesa's own
 * writer, plus a second data file written here with parquet-java's {@code VariantValueWriter} against a
 * schema that shreds {@code name}, {@code eventType} and {@code count}, appended through the Iceberg
 * API. The shredded file's footer is read back to prove the {@code typed_value} columns really hold
 * values. A reference table holds the same rows all written unshredded.
 * Additional tables exercise GeoMesa's shared writer with an explicit layout, including a mix of
 * plain and shredded files and nested arrays, against those same reference rows.
 */
class ShreddedVariantEndToEndTest {

    private static final String SCHEMA = "geomesa";
    private static final String CATALOG = "spatial_iceberg";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String SHARED_LAYOUT =
            "{\"props\":{\"name\":\"string\",\"eventType\":\"string\",\"count\":\"byte\",\"items\":[{\"name\":\"string\"}]}}";

    /** id -> literal JSON, rows of the unshredded data file. */
    private static final Map<Integer, String> UNSHREDDED_ROWS = new LinkedHashMap<>();
    /** id -> literal JSON (null is a SQL NULL variant), rows of the shredded data file. */
    private static final Map<Integer, String> SHREDDED_ROWS = new LinkedHashMap<>();

    static {
        UNSHREDDED_ROWS.put(1, "{\"name\":\"b\",\"eventType\":\"click\",\"count\":5}");
        UNSHREDDED_ROWS.put(2, "{\"name\":\"c\",\"eventType\":\"view\",\"count\":6}");
        UNSHREDDED_ROWS.put(3, "{\"other\":true}");

        SHREDDED_ROWS.put(10, "{\"name\":\"a\",\"eventType\":\"click\",\"count\":1}");
        // 'extra' is not a shredded field, so it is kept in the group's residual 'value'
        SHREDDED_ROWS.put(11, "{\"name\":\"b\",\"eventType\":\"view\",\"count\":2,\"extra\":\"x\"}");
        // eventType holds an integer where the typed column is a string: it falls back to the field's 'value'
        SHREDDED_ROWS.put(12, "{\"name\":\"b\",\"eventType\":7,\"count\":3}");
        SHREDDED_ROWS.put(13, null);
        // differs from row 11 only in case: the prefilter and the residual are case sensitive
        SHREDDED_ROWS.put(14, "{\"name\":\"B\",\"eventType\":\"Click\",\"count\":4}");
    }

    private static final Map<Integer, String> PHASE_ONE_ROWS = new LinkedHashMap<>();
    static {
        List<String> documents = List.of(
            "{\"name\":\"match\"}", "{\"name\":\"other\"}", "{\"name\":1}",
            "{\"name\":\"001\"}", "{\"items\":[{\"name\":\"match\"},{\"name\":\"other\"}]}",
            "{\"items\":[{\"name\":\"other\"}]}", "{\"items\":[{\"name\":1},{\"name\":\"other\"}]}",
            "{\"items\":[{},null,7]}", "{\"items\":[]}", "{\"items\":{\"name\":\"match\"}}",
            "{\"tags\":[\"match\",\"other\"]}", "{\"tags\":[\"other\"]}", "{\"tags\":[1,\"other\"]}",
            "{\"bag\":{\"a\":{\"name\":\"match\"},\"b\":{\"name\":\"other\"}}}",
            "{\"bag\":{\"a\":{\"name\":\"other\"}}}", "{\"bag\":{\"a\":\"other\",\"b\":\"match\"}}",
            "{\"bag\":{\"a\":\"other\"}}", "{\"name\":\"O'Brien\",\"*\":\"match\",\"odd'name\":\"match\"}",
            "{\"name\":[\"match\",\"other\"]}", "{\"name\":{\"x\":1}}", "{\"name\":null}", "{}");
        for (int id = 0; id < documents.size(); id++) PHASE_ONE_ROWS.put(id, documents.get(id));
        PHASE_ONE_ROWS.put(documents.size(), null);
    }

    private static final Map<String, Map<Integer, String>> RANGE_ROWS = rangeRows();

    private static Map<String, Map<Integer, String>> rangeRows() {
        var sorted = new LinkedHashMap<Integer, String>();
        var random = new java.util.Random(73);
        for (int i = 0; i < 5000; i++) {
            byte[] padding = new byte[256];
            random.nextBytes(padding);
            sorted.put(i, "{\"outer\":{\"score\":" + (100000 + i) + "},\"padding\":\""
                    + java.util.Base64.getEncoder().encodeToString(padding) + "\"}");
        }
        var order = new ArrayList<>(sorted.keySet());
        java.util.Collections.shuffle(order, new java.util.Random(19));
        var shuffled = new LinkedHashMap<Integer, String>();
        order.forEach(id -> shuffled.put(id, sorted.get(id)));
        var mixed = new LinkedHashMap<>(sorted);
        // Spread uncertain values across every group. Candidates must retain them for the residual.
        for (int i = 0; i < 5000; i += 1000) {
            mixed.put(i, "{\"outer\":{\"score\":\"200000\"}}");
            mixed.put(i + 1, "{\"outer\":{\"score\":null}}");
            mixed.put(i + 2, "{\"outer\":{}}");
            mixed.put(i + 3, "{\"outer\":\"wrong shape\"}");
            mixed.put(i + 4, null);
            mixed.put(i + 5, "{\"outer\":{\"score\":200000.5}}");
            mixed.put(i + 6, "{\"outer\":{\"score\":7}}");
            mixed.put(i + 7, "{\"outer\":{\"score\":9007199254740993}}");
        }
        return Map.of("rangesorted", sorted, "rangerandom", shuffled, "rangemixed", mixed);
    }

    private static final Map<String, Map<Integer, String>> FRACTIONAL_ROWS = fractionalRows();

    private static Map<String, Map<Integer, String>> fractionalRows() {
        var result = new LinkedHashMap<String, Map<Integer, String>>();
        for (String name : List.of("decimal32sorted", "decimal64sorted", "decimal128sorted", "doublesorted")) {
            int scale = name.startsWith("decimal64") ? 12 : name.startsWith("decimal128") ? 20 : 4;
            var rows = new LinkedHashMap<Integer, String>();
            for (int id = 0; id < 5000; id++) {
                String value = java.math.BigDecimal.valueOf(1000 + id, 3).setScale(scale).toPlainString();
                if (name.startsWith("double")) value += "e0"; // parser chooses DOUBLE for exponent notation
                String padding = RANGE_ROWS.get("rangesorted").get(id);
                // Keep the CTP four-number shape. Padding is an explicit mechanism-test sibling,
                // not a claim about the size or distribution of operational position_error values.
                rows.put(id, "{\"position_error\":{\"semi_major\":" + value + ",\"semi_minor\":" + value
                        + ",\"azimuth\":" + value + ",\"altitude_error\":" + value + "},\"padding\":"
                        + quoteJson(padding) + "}");
            }
            result.put(name, rows);
        }
        var order = new ArrayList<>(result.get("decimal32sorted").keySet());
        java.util.Collections.shuffle(order, new java.util.Random(37));
        var shuffled = new LinkedHashMap<Integer, String>();
        order.forEach(id -> shuffled.put(id, result.get("decimal32sorted").get(id)));
        result.put("decimalrandom", shuffled);
        var mixed = new LinkedHashMap<>(result.get("decimal32sorted"));
        for (int i = 0; i < 5000; i += 1000) {
            mixed.put(i, "{\"position_error\":{\"semi_major\":\"10.0\"}}");
            mixed.put(i + 1, "{\"position_error\":{\"semi_major\":null}}");
            mixed.put(i + 2, "{\"position_error\":{}}");
            mixed.put(i + 3, "{\"position_error\":false}");
            mixed.put(i + 4, null);
            mixed.put(i + 5, "{\"position_error\":{\"semi_major\":10.00001}}"); // different scale
            mixed.put(i + 6, "{\"position_error\":{\"semi_major\":10e0}}"); // different encoding
            mixed.put(i + 7, "{\"position_error\":{\"semi_major\":9007199254740993}}");
        }
        result.put("decimalmixed", mixed);
        return result;
    }

    private static final Map<String, Map<Integer, String>> ARRAY_ROWS = arrayRows();

    private static Map<String, Map<Integer, String>> arrayRows() {
        var sorted = new LinkedHashMap<Integer, String>();
        for (int id = 0; id < 5000; id++) {
            String status = id < 4000 ? "UNKNOWN" : "RESOLVED";
            var dimensions = new ArrayList<String>();
            for (int element = 0; element <= id % 3; element++) {
                dimensions.add("{\"dimension_id\":\"TYPE\",\"status\":{\"value\":\"" + status + "\"}}");
            }
            // The nested array path comes from CTP identity_data. Padding and row order are
            // synthetic controls for reader I/O, not operational size or distribution claims.
            sorted.put(id, "{\"identity_data\":{\"dimensions\":[" + String.join(",", dimensions)
                    + "]},\"padding\":" + quoteJson(RANGE_ROWS.get("rangesorted").get(id)) + "}");
        }
        var order = new ArrayList<>(sorted.keySet());
        java.util.Collections.shuffle(order, new java.util.Random(41));
        var shuffled = new LinkedHashMap<Integer, String>();
        order.forEach(id -> shuffled.put(id, sorted.get(id)));
        var sparse = new LinkedHashMap<>(sorted);
        var mixed = new LinkedHashMap<>(sorted);
        for (int id = 0; id < 5000; id += 1000) {
            sparse.put(id, "{\"identity_data\":{\"dimensions\":[]}}");
            sparse.put(id + 1, "{\"identity_data\":{\"dimensions\":[{}]}}");
            mixed.put(id, "{\"identity_data\":{\"dimensions\":[{\"status\":{\"value\":7}}]}}");
            mixed.put(id + 1, "{\"identity_data\":{\"dimensions\":[null]}}");
            mixed.put(id + 2, "{\"identity_data\":{\"dimensions\":[{\"status\":null}]}}");
            mixed.put(id + 3, "{\"identity_data\":{\"dimensions\":{\"status\":{\"value\":\"RESOLVED\"}}}}");
            mixed.put(id + 4, "{\"identity_data\":{\"dimensions\":null}}");
            mixed.put(id + 5, "{}");
            mixed.put(id + 6, null);
            // Different elements satisfy the two predicates; conjunction cannot require
            // same-element correlation. An unrelated fallback must retain this group.
            mixed.put(id + 7, "{\"identity_data\":{\"dimensions\":["
                    + "{\"dimension_id\":\"TYPE\",\"status\":{\"value\":\"UNKNOWN\"}},"
                    + "{\"dimension_id\":\"OTHER\",\"status\":{\"value\":\"RESOLVED\"},\"extra\":true}]}}");
        }
        var result = new LinkedHashMap<String, Map<Integer, String>>();
        result.put("arraysorted", sorted);
        result.put("arrayrandom", shuffled);
        result.put("arraysparse", sparse);
        result.put("arraymixed", mixed);
        return result;
    }

    private static final Map<String, Map<Integer, String>> FILE_ROWS = fileRows();
    private static final List<String> FILE_PATH = List.of("itinerary", "origin", "location", "code");
    private static final String FILE_LAYOUT = "{\"props\":{\"itinerary\":{\"origin\":{\"location\":{\"code\":\"string\"}}},\"padding\":\"string\"}}";

    private static Map<String, Map<Integer, String>> fileRows() {
        var sorted = new LinkedHashMap<Integer, String>();
        for (int id = 0; id < 5000; id++) {
            sorted.put(id, "{\"itinerary\":{\"origin\":{\"location\":{\"code\":\"" + (id < 4000 ? "IAD" : "SFO")
                    + "\"}}},\"padding\":" + quoteJson(RANGE_ROWS.get("rangesorted").get(id)) + "}");
        }
        var order = new ArrayList<>(sorted.keySet());
        java.util.Collections.shuffle(order, new java.util.Random(43));
        var shuffled = new LinkedHashMap<Integer, String>();
        order.forEach(id -> shuffled.put(id, sorted.get(id)));
        var sparse = new LinkedHashMap<>(sorted);
        var mixed = new LinkedHashMap<>(sorted);
        for (int id = 0; id < 5000; id += 1000) {
            sparse.put(id, "{\"itinerary\":{\"origin\":{\"location\":{}}}}");
            sparse.put(id + 1, null);
            mixed.put(id, "{\"itinerary\":{\"origin\":{\"location\":{\"code\":7}}}}");
        }
        // These matching strings are deliberately stored in unshredded/fallback-only files.
        for (int id : List.of(7, 1007)) mixed.put(id, sorted.get(4007));
        var result = new LinkedHashMap<String, Map<Integer, String>>();
        result.put("fileboundssorted", sorted);
        result.put("fileboundsrandom", shuffled);
        result.put("fileboundssparse", sparse);
        result.put("fileboundsmixed", mixed);
        result.put("fileboundsnostats", sorted);
        result.put("fileboundsrenamed", sorted);
        var deleted = new LinkedHashMap<>(sorted);
        deleted.remove(0);
        deleted.remove(4000);
        deleted.remove(4001);
        result.put("fileboundsdeleted", deleted);
        return result;
    }

    private static String quoteJson(String value) {
        try { return MAPPER.writeValueAsString(value); }
        catch (java.io.IOException e) { throw new java.io.UncheckedIOException(e); }
    }

    /** every row is dated the same day, so the table has one 'daily' partition. */
    private static final String DTG = "2017-06-05T04:03:02.000Z";
    private static final long DTG_MICROS = java.time.Instant.parse(DTG).toEpochMilli() * 1000L;
    private static final int DTG_DAY = (int) java.time.LocalDate.of(2017, 6, 5).toEpochDay();

    private static final Network network = Network.newNetwork();

    private static final class ClusterNode extends GeoMesaTrinoContainer {
        @Override protected void waitUntilContainerStarted() {
            org.testcontainers.containers.wait.strategy.Wait.forHttp("/v1/info").forPort(8080)
                    .forResponsePredicate(body -> {
                        try { return !MAPPER.readTree(body).path("starting").asBoolean(true); }
                        catch (java.io.IOException e) { return false; }
                    }).withStartupTimeout(java.time.Duration.ofMinutes(2)).waitUntilReady(this);
        }
    }

    private static final SeaweedFsContainer s3 =
            new SeaweedFsContainer("admin", "admin").withNetwork(network).withNetworkAliases("seaweed");

    private static final IcebergRestContainer iceberg =
            new IcebergRestContainer("admin", "admin").withNetwork(network).withNetworkAliases("rest-catalog");

    @SuppressWarnings("resource")
    private static final TrinoContainer trino =
            new ClusterNode()
                    .withGeoMesaPlugin()
                    .withNetwork(network)
                    .withNetworkAliases("trino")
                    .withCopyToContainer(configuration("docker/trino/phase3-coordinator.properties"), "/etc/trino/config.properties")
                    .withFileSystemBind(
                            MountableFile.forClasspathResource("docker/trino/spatial_iceberg.properties").getResolvedPath(),
                            "/etc/trino/catalog/spatial_iceberg.properties", BindMode.READ_ONLY);

    @SuppressWarnings("resource")
    private static final TrinoContainer worker = new ClusterNode().withGeoMesaPlugin().withNetwork(network)
            .withNetworkAliases("trino-worker")
            .withCopyToContainer(configuration("docker/trino/phase3-worker.properties"), "/etc/trino/config.properties")
            .withFileSystemBind(MountableFile.forClasspathResource("docker/trino/spatial_iceberg.properties").getResolvedPath(),
                    "/etc/trino/catalog/spatial_iceberg.properties", BindMode.READ_ONLY);

    private static Transferable configuration(String resource) {
        // Copy content without inheriting host file ownership, which may not map into a rootless daemon.
        try (var input = ShreddedVariantEndToEndTest.class.getClassLoader().getResourceAsStream(resource)) {
            if (input == null) throw new IllegalArgumentException("Missing configuration resource: " + resource);
            return Transferable.of(input.readAllBytes(), 0644);
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    @BeforeAll
    static void setUp() throws Exception {
        s3.start();
        iceberg.start();

        var restUri = "http://" + iceberg.getHost() + ":" + iceberg.getFirstMappedPort() + "/";
        var fsProps = String.join("\n",
                "type=rest",
                "uri=" + restUri,
                "iceberg.namespace=" + SCHEMA,
                "fs.s3.region=us-east-1",
                "fs.s3.endpoint=" + s3.getS3URL(),
                "fs.s3.access-key-id=admin",
                "fs.s3.secret-access-key=admin",
                "fs.s3.force-path-style=true");

        var shreddedSft = SimpleFeatureTypes.createType("shredtest", "num:Int,props:String:json=true,dtg:Date;geomesa.fs.scheme='daily'");
        var referenceSft = SimpleFeatureTypes.createType("shredref", "num:Int,props:String:json=true,dtg:Date;geomesa.fs.scheme='daily'");

        var fsds = DataStoreFinder.getDataStore(Map.of("fs.config.properties", fsProps));
        try {
            fsds.createSchema(shreddedSft);
            write(fsds, shreddedSft, UNSHREDDED_ROWS);
            fsds.createSchema(referenceSft);
            var shared = SimpleFeatureTypes.createType("sharedtest", "num:Int,props:String:json=true,dtg:Date;geomesa.fs.scheme='daily'");
            fsds.createSchema(shared);
            write(fsds, shared, UNSHREDDED_ROWS);
            var all = new LinkedHashMap<>(UNSHREDDED_ROWS);
            all.putAll(SHREDDED_ROWS);
            write(fsds, referenceSft, all);
            var phaseOne = SimpleFeatureTypes.createType("phaseone", "num:Int,props:String:json=true,dtg:Date;geomesa.fs.scheme='daily'");
            var phaseOneRef = SimpleFeatureTypes.createType("phaseoneref", "num:Int,props:String:json=true,dtg:Date;geomesa.fs.scheme='daily'");
            fsds.createSchema(phaseOne);
            fsds.createSchema(phaseOneRef);
            write(fsds, phaseOneRef, PHASE_ONE_ROWS);
        } finally {
            fsds.dispose();
        }

        // A separate data store snapshots the explicit layout for newly opened files only.
        var sharedWriter = DataStoreFinder.getDataStore(Map.of("fs.config.properties",
                fsProps + "\ngeomesa.parquet.variant.shredding=" + SHARED_LAYOUT));
        try {
            write(sharedWriter, sharedWriter.getSchema("sharedtest"), SHREDDED_ROWS);
            var sharedPhase = SimpleFeatureTypes.createType("sharedphase", "num:Int,props:String:json=true,dtg:Date;geomesa.fs.scheme='daily'");
            sharedWriter.createSchema(sharedPhase);
            write(sharedWriter, sharedPhase, PHASE_ONE_ROWS);
        } finally {
            sharedWriter.dispose();
        }

        var rangeWriter = DataStoreFinder.getDataStore(Map.of("fs.config.properties", fsProps
                + "\ngeomesa.parquet.variant.shredding={\"props\":{\"outer\":{\"score\":\"int\"},\"padding\":\"string\"}}"
                + "\nparquet.block.row.count.limit=1000\nparquet.page.size.row.check.min=100\nparquet.page.size.row.check.max=100"));
        try {
            for (var entry : RANGE_ROWS.entrySet()) {
                var type = SimpleFeatureTypes.createType(entry.getKey(), "num:Int,props:String:json=true,dtg:Date;geomesa.fs.scheme='daily'");
                rangeWriter.createSchema(type);
                write(rangeWriter, type, entry.getValue());
            }
        } finally {
            rangeWriter.dispose();
        }

        for (var entry : FRACTIONAL_ROWS.entrySet()) {
            String name = entry.getKey();
            String kind = name.startsWith("double") ? "double" : name.startsWith("decimal64") ? "decimal(18,12)"
                    : name.startsWith("decimal128") ? "decimal(38,20)" : "decimal(9,4)";
            String layout = "{\"props\":{\"position_error\":{\"semi_major\":\"" + kind + "\",\"semi_minor\":\"" + kind
                    + "\",\"azimuth\":\"" + kind + "\",\"altitude_error\":\"" + kind + "\"},\"padding\":\"string\"}}";
            var writer = DataStoreFinder.getDataStore(Map.of("fs.config.properties", fsProps
                    + "\ngeomesa.parquet.variant.shredding=" + layout
                    + "\nparquet.block.row.count.limit=1000\nparquet.page.size.row.check.min=100\nparquet.page.size.row.check.max=100"));
            try {
                var type = SimpleFeatureTypes.createType(name, "num:Int,props:String:json=true,dtg:Date;geomesa.fs.scheme='daily'");
                writer.createSchema(type);
                write(writer, type, entry.getValue());
            } finally { writer.dispose(); }
        }

        var arrayWriter = DataStoreFinder.getDataStore(Map.of("fs.config.properties", fsProps
                + "\ngeomesa.parquet.variant.shredding={\"props\":{\"identity_data\":{\"dimensions\":["
                + "{\"dimension_id\":\"string\",\"status\":{\"value\":\"string\"}}]},\"padding\":\"string\"}}"
                + "\nparquet.block.row.count.limit=1000\nparquet.page.size.row.check.min=100\nparquet.page.size.row.check.max=100"));
        try {
            for (var entry : ARRAY_ROWS.entrySet()) {
                var type = SimpleFeatureTypes.createType(entry.getKey(), "num:Int,props:String:json=true,dtg:Date;geomesa.fs.scheme='daily'");
                arrayWriter.createSchema(type);
                write(arrayWriter, type, entry.getValue());
            }
        } finally { arrayWriter.dispose(); }

        writeFileBoundFixtures(fsProps, restUri);

        appendShreddedFile(restUri, "shredtest", SHREDDED_ROWS);
        appendShreddedFile(restUri, "phaseone", PHASE_ONE_ROWS);

        trino.start();
        worker.start();
        // Coordinator cannot run table scans: every result below requires a decoded worker handle.
        long deadline = System.nanoTime() + java.time.Duration.ofSeconds(60).toNanos();
        while (true) {
            try (Connection connection = connect(); Statement statement = connection.createStatement();
                    ResultSet result = statement.executeQuery("SELECT count(*) FROM system.runtime.nodes WHERE state = 'active' AND NOT coordinator")) {
                result.next();
                if (result.getLong(1) == 1) break;
            }
            if (System.nanoTime() > deadline) throw new AssertionError("Worker did not register");
            Thread.sleep(250);
        }
    }

    private static void writeFileBoundFixtures(String fsProps, String restUri) throws Exception {
        for (String name : FILE_ROWS.keySet()) {
            var rows = new ArrayList<>(FILE_ROWS.get(name).entrySet());
            var schema = SimpleFeatureTypes.createType(name, "num:Int,props:String:json=true,dtg:Date;geomesa.fs.scheme='daily'");
            var creator = DataStoreFinder.getDataStore(Map.of("fs.config.properties", fsProps));
            try { creator.createSchema(schema); } finally { creator.dispose(); }
            if (name.equals("fileboundsnostats") || name.equals("fileboundsdeleted") || name.equals("fileboundsrenamed")) continue;
            for (int group = 0; group < 5; group++) {
                String layout = name.equals("fileboundsmixed") && group == 0 ? "" : "\ngeomesa.parquet.variant.shredding="
                        + (name.equals("fileboundsmixed") && group == 1 ? FILE_LAYOUT.replace("code\":\"string", "code\":\"int") : FILE_LAYOUT);
                var writer = DataStoreFinder.getDataStore(Map.of("fs.config.properties", fsProps + layout));
                try {
                    var batch = new LinkedHashMap<Integer, String>();
                    rows.subList(group * 1000, (group + 1) * 1000).forEach(row -> batch.put(row.getKey(), row.getValue()));
                    write(writer, schema, batch);
                } finally { writer.dispose(); }
            }
        }
        try (var catalog = catalog(restUri)) {
            var original = catalog.loadTable(TableIdentifier.of(SCHEMA, "fileboundssorted"));
            var files = new ArrayList<DataFile>();
            try (var tasks = original.newScan().includeColumnStats().planFiles()) {
                for (var task : tasks) files.add(task.file().copy());
            }
            assertThat(files).hasSize(5);
            for (String name : List.of("fileboundsnostats", "fileboundsdeleted", "fileboundsrenamed")) {
                var table = catalog.loadTable(TableIdentifier.of(SCHEMA, name));
                var append = table.newAppend();
                for (var file : files) {
                    append.appendFile(name.endsWith("nostats") ? DataFiles.builder(table.spec()).copy(file)
                            .withMetrics(new Metrics(file.recordCount(), null, null, null, null, null, null)).build() : file);
                }
                append.commit();
                if (name.endsWith("renamed")) table.updateSchema().renameColumn("props", "payload").commit();
                if (name.endsWith("deleted")) {
                    var delta = table.newRowDelta();
                    var writer = new org.apache.iceberg.deletes.BaseDVFileWriter(
                            org.apache.iceberg.io.OutputFileFactory.builderFor(table, 0, 0).format(FileFormat.PUFFIN).build(),
                            ignored -> org.apache.iceberg.deletes.PositionDeleteIndex.empty());
                    try (writer) {
                        for (var file : files) {
                            int first = org.apache.iceberg.types.Conversions.fromByteBuffer(org.apache.iceberg.types.Types.IntegerType.get(),
                                    file.lowerBounds().get(table.schema().findField("num").fieldId()));
                            if (first == 0 || first == 4000) writer.delete(file.location(), 0, table.spec(), file.partition());
                        }
                    }
                    for (var delete : writer.result().deleteFiles()) delta.addDeletes(delete);
                    var deleteSchema = table.schema().select("num");
                    String location = table.locationProvider().newDataLocation("equalities-" + UUID.randomUUID() + ".parquet");
                    var equalities = org.apache.iceberg.parquet.Parquet.writeDeletes(table.io().newOutputFile(location))
                            .forTable(table).withPartition(files.get(0).partition()).rowSchema(deleteSchema)
                            .equalityFieldIds(List.of(deleteSchema.findField("num").fieldId()))
                            .createWriterFunc(org.apache.iceberg.data.parquet.GenericParquetWriter::create).buildEqualityWriter();
                    try (equalities) {
                        var record = org.apache.iceberg.data.GenericRecord.create(deleteSchema);
                        record.setField("num", 4001);
                        equalities.write(record);
                    }
                    delta.addDeletes(equalities.toDeleteFile());
                    delta.commit();
                }
            }
        }
    }

    @AfterAll
    static void tearDown() {
        worker.stop();
        trino.stop();
        iceberg.stop();
        s3.stop();
    }

    private static void write(
            org.geotools.api.data.DataStore ds,
            org.geotools.api.feature.simple.SimpleFeatureType sft,
            Map<Integer, String> rows) throws Exception {
        try (var writer = ds.getFeatureWriterAppend(sft.getTypeName(), Transaction.AUTO_COMMIT)) {
            for (var row : rows.entrySet()) {
                var sf = new ScalaSimpleFeature(sft, Integer.toString(row.getKey()), null, null);
                sf.setAttribute(0, row.getKey());
                sf.setAttribute(1, row.getValue());
                sf.setAttribute(2, DTG);
                FeatureUtils.write(writer, (SimpleFeature) sf, true);
            }
        }
    }

    // ---- the shredded data file ------------------------------------------------------------------

    /** The shredded variant group: object fields name, eventType (mixed case) and count, plus residual value. */
    private static Type shreddedProps(int fieldId) {
        return Types.optionalGroup().as(LogicalTypeAnnotation.variantType((byte) 1))
                .id(fieldId)
                .required(PrimitiveTypeName.BINARY).named("metadata")
                .optional(PrimitiveTypeName.BINARY).named("value")
                .addField(Types.optionalGroup()
                        .addField(Types.requiredGroup()
                                .optional(PrimitiveTypeName.BINARY).named("value")
                                .optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType())
                                .named("typed_value")
                                .named("name"))
                        .addField(Types.requiredGroup()
                                .optional(PrimitiveTypeName.BINARY).named("value")
                                .optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType())
                                .named("typed_value")
                                .named("eventType"))
                        .addField(Types.requiredGroup()
                                .optional(PrimitiveTypeName.BINARY).named("value")
                                // small JSON integers parse as int8, and only an exact physical match is shredded
                                .optional(PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.intType(8, true))
                                .named("typed_value")
                                .named("count"))
                        .addField(Types.requiredGroup()
                                .optional(PrimitiveTypeName.BINARY).named("value")
                                .addField(Types.optionalGroup().as(LogicalTypeAnnotation.listType())
                                        .addField(Types.repeatedGroup()
                                                .addField(Types.requiredGroup()
                                                        .optional(PrimitiveTypeName.BINARY).named("value")
                                                        .addField(Types.optionalGroup()
                                                                .addField(Types.requiredGroup()
                                                                        .optional(PrimitiveTypeName.BINARY).named("value")
                                                                        .optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("typed_value")
                                                                        .named("name"))
                                                                .named("typed_value"))
                                                        .named("element"))
                                                .named("list"))
                                        .named("typed_value"))
                                .named("items"))
                        .named("typed_value"))
                .named("props");
    }

    private static class RowWriteSupport extends WriteSupport<Map.Entry<Integer, String>> {
        private final MessageType schema;
        private RecordConsumer consumer;

        RowWriteSupport(MessageType schema) {
            this.schema = schema;
        }

        @Override
        public WriteContext init(org.apache.parquet.conf.ParquetConfiguration configuration) {
            return new WriteContext(schema, Map.of());
        }

        @Override
        public WriteContext init(org.apache.hadoop.conf.Configuration configuration) {
            return new WriteContext(schema, Map.of());
        }

        @Override
        public void prepareForWrite(RecordConsumer recordConsumer) {
            this.consumer = recordConsumer;
        }

        @Override
        public void write(Map.Entry<Integer, String> row) {
            consumer.startMessage();
            var fid = schema.getFieldIndex("__fid__");
            consumer.startField("__fid__", fid);
            consumer.addBinary(Binary.fromString(Integer.toString(row.getKey())));
            consumer.endField("__fid__", fid);
            var id = schema.getFieldIndex("num");
            consumer.startField("num", id);
            consumer.addInteger(row.getKey());
            consumer.endField("num", id);
            var dtg = schema.getFieldIndex("dtg");
            consumer.startField("dtg", dtg);
            consumer.addLong(DTG_MICROS);
            consumer.endField("dtg", dtg);
            if (row.getValue() != null) {
                var props = schema.getFieldIndex("props");
                consumer.startField("props", props);
                try {
                    VariantValueWriter.write(
                            consumer, schema.getType("props").asGroupType(), VariantJsonParser.parseJson(row.getValue()));
                } catch (java.io.IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
                consumer.endField("props", props);
            }
            consumer.endMessage();
        }
    }

    private static class RowWriterBuilder extends ParquetWriter.Builder<Map.Entry<Integer, String>, RowWriterBuilder> {
        private final MessageType schema;

        RowWriterBuilder(LocalOutputFile file, MessageType schema) {
            super(file);
            this.schema = schema;
        }

        @Override
        protected RowWriterBuilder self() {
            return this;
        }

        @Override
        protected WriteSupport<Map.Entry<Integer, String>> getWriteSupport(org.apache.hadoop.conf.Configuration conf) {
            return new RowWriteSupport(schema);
        }

        @Override
        protected WriteSupport<Map.Entry<Integer, String>> getWriteSupport(
                org.apache.parquet.conf.ParquetConfiguration conf) {
            return new RowWriteSupport(schema);
        }
    }

    /** Footer counts for the shredded file: non-null values per leaf column, keyed by dotted path. */
    private static Map<String, Long> nonNullValues(Path file) throws Exception {
        var counts = new TreeMap<String, Long>();
        try (var reader = ParquetFileReader.open(new LocalInputFile(file))) {
            for (var block : reader.getFooter().getBlocks()) {
                for (ColumnChunkMetaData column : block.getColumns()) {
                    counts.merge(column.getPath().toDotString(),
                            column.getValueCount() - column.getStatistics().getNumNulls(), Long::sum);
                }
            }
        }
        return counts;
    }

    private static void appendShreddedFile(String restUri, String tableName, Map<Integer, String> rows) throws Exception {
        Catalog catalog = catalog(restUri);
        Table table = catalog.loadTable(TableIdentifier.of(SCHEMA, tableName));

        // the table's own schema (with field ids) in parquet form, with the props column replaced by the shredded one
        var base = new TypeToMessageType().convert(table.schema(), "table");
        var fields = new ArrayList<Type>();
        for (Type field : base.getFields()) {
            fields.add("props".equals(field.getName()) ? shreddedProps(field.getId().intValue()) : field);
        }
        var message = new MessageType("table", fields);

        var local = Files.createTempFile("shredded-", ".parquet");
        Files.delete(local);
        try (var writer = new RowWriterBuilder(new LocalOutputFile(local), message).build()) {
            for (var row : rows.entrySet()) {
                writer.write(row);
            }
        }

        // prove the file is shredded: the typed_value columns hold values (5 rows, one a SQL NULL variant)
        var counts = nonNullValues(local);
        if (tableName.equals("shredtest")) {
        assertThat(counts.get("props.typed_value.name.typed_value")).as("%s", counts).isEqualTo(4L);
        assertThat(counts.get("props.typed_value.eventType.typed_value")).as("%s", counts).isEqualTo(3L);
        assertThat(counts.get("props.typed_value.count.typed_value")).as("%s", counts).isEqualTo(4L);
        // the mismatching eventType (row 12) and the residual field (row 11) are held in 'value' columns
        assertThat(counts.get("props.typed_value.eventType.value")).as("%s", counts).isEqualTo(1L);
        assertThat(counts.get("props.value")).as("%s", counts).isEqualTo(1L);

        } else {
            assertThat(counts.get("props.typed_value.name.typed_value")).isPositive();
            assertThat(counts.get("props.typed_value.items.typed_value.list.element.typed_value.name.typed_value")).isPositive();
        }

        var location = table.locationProvider().newDataLocation("shredded-" + UUID.randomUUID() + ".parquet");
        try (PositionOutputStream out = table.io().newOutputFile(location).create()) {
            out.write(Files.readAllBytes(local));
        }
        InputFile input = table.io().newInputFile(location);
        // the metrics iceberg itself derives from the footer, including bounds and null counts for props
        Metrics metrics = ParquetUtil.fileMetrics(input, MetricsConfig.forTable(table));
        System.out.println("shredded data file metrics: records=" + metrics.recordCount()
                + " valueCounts=" + metrics.columnSizes() + " nullCounts=" + metrics.nullValueCounts()
                + " lower=" + (metrics.lowerBounds() == null ? null : metrics.lowerBounds().keySet())
                + " upper=" + (metrics.upperBounds() == null ? null : metrics.upperBounds().keySet()));
        var partition = new org.apache.iceberg.PartitionData(table.spec().partitionType());
        partition.set(0, DTG_DAY);
        DataFile dataFile = DataFiles.builder(table.spec())
                .withPartition(partition)
                .withFormat(FileFormat.PARQUET)
                .withPath(location)
                .withFileSizeInBytes(input.getLength())
                .withMetrics(metrics)
                .build();
        table.newAppend().appendFile(dataFile).commit();
        Files.delete(local);
        if (tableName.equals("shredtest")) appendIdentityPartitionedTable(catalog, table, message);
    }

    private static RESTCatalog catalog(String restUri) {
        return (RESTCatalog) CatalogUtil.buildIcebergCatalog("test", Map.of(
                "type", "rest",
                "uri", restUri,
                "io-impl", "org.apache.iceberg.aws.s3.S3FileIO",
                "s3.endpoint", s3.getS3URL(),
                "s3.access-key-id", "admin",
                "s3.secret-access-key", "admin",
                "s3.path-style-access", "true",
                "client.region", "us-east-1"), null);
    }

    private static void appendIdentityPartitionedTable(Catalog catalog, Table source, MessageType message) throws Exception {
        var spec = org.apache.iceberg.PartitionSpec.builderFor(source.schema()).identity("num").build();
        Table table = catalog.createTable(TableIdentifier.of(SCHEMA, "shredidentity"), source.schema(), spec,
                Map.of("format-version", "3"));
        var rows = new LinkedHashMap<>(UNSHREDDED_ROWS);
        rows.putAll(SHREDDED_ROWS);
        var append = table.newAppend();
        for (var row : rows.entrySet()) {
            var local = Files.createTempFile("shredded-identity-", ".parquet");
            Files.delete(local);
            try {
                try (var writer = new RowWriterBuilder(new LocalOutputFile(local), message).build()) { writer.write(row); }
                var location = table.locationProvider().newDataLocation(UUID.randomUUID() + ".parquet");
                try (PositionOutputStream output = table.io().newOutputFile(location).create()) { output.write(Files.readAllBytes(local)); }
                var input = table.io().newInputFile(location);
                var partition = new org.apache.iceberg.PartitionData(spec.partitionType());
                partition.set(0, row.getKey());
                append.appendFile(DataFiles.builder(spec).withPartition(partition).withFormat(FileFormat.PARQUET)
                        .withPath(location).withFileSizeInBytes(input.getLength())
                        .withMetrics(ParquetUtil.fileMetrics(input, MetricsConfig.forTable(table))).build());
            } finally { Files.deleteIfExists(local); }
        }
        append.commit();
    }

    // ---- reading ---------------------------------------------------------------------------------

    private static Connection connect() throws Exception {
        var props = new java.util.Properties();
        props.setProperty("user", "admin");
        return DriverManager.getConnection("jdbc:trino://" + trino.getHost() + ":" + trino.getFirstMappedPort(), props);
    }

    private static Map<Integer, JsonNode> readJson(String table) throws Exception {
        var out = new TreeMap<Integer, JsonNode>();
        try (var conn = connect(); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT num, CAST(props AS JSON) FROM " + CATALOG + "." + SCHEMA + "." + table)) {
            while (rs.next()) {
                var json = rs.getString(2);
                int id = rs.getInt(1);
                assertThat(out).as("duplicate row in %s", table).doesNotContainKey(id);
                out.put(id, json == null ? null : MAPPER.readTree(json));
            }
        }
        return out;
    }

    private static Map<Integer, JsonNode> expected(Map<Integer, String> rows) throws Exception {
        var out = new TreeMap<Integer, JsonNode>();
        for (var row : rows.entrySet()) {
            out.put(row.getKey(), row.getValue() == null ? null : MAPPER.readTree(row.getValue()));
        }
        return out;
    }

    @Test
    void sharedWriterProducesTypedColumnsAndFallback() throws Exception {
        try (var catalog = catalog("http://" + iceberg.getHost() + ":" + iceberg.getFirstMappedPort() + "/")) {
            for (String name : List.of("sharedtest", "sharedphase")) {
                var table = catalog.loadTable(TableIdentifier.of(SCHEMA, name));
                var counts = new TreeMap<String, Long>();
                int plainFiles = 0;
                int typedFiles = 0;
                try (var tasks = table.newScan().planFiles()) {
                    for (var task : tasks) {
                        var local = Files.createTempFile("shared-writer-", ".parquet");
                        try {
                            try (var input = table.io().newInputFile(task.file().location()).newStream()) {
                                Files.copy(input, local, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                            }
                            try (var reader = ParquetFileReader.open(new LocalInputFile(local))) {
                                var props = reader.getFooter().getFileMetaData().getSchema().getType("props").asGroupType();
                                assertThat(props.getId().intValue()).isEqualTo(table.schema().findField("props").fieldId());
                                if (props.containsField("typed_value")) {
                                    typedFiles++;
                                    nonNullValues(local).forEach((path, count) -> counts.merge(path, count, Long::sum));
                                } else {
                                    plainFiles++;
                                }
                            }
                        } finally {
                            Files.delete(local);
                        }
                    }
                }
                assertThat(typedFiles).as("%s typed files", name).isPositive();
                if (name.equals("sharedtest")) {
                    assertThat(plainFiles).as("mixed layouts in one table").isPositive();
                    assertThat(counts.get("props.typed_value.name.typed_value")).isEqualTo(4L);
                    assertThat(counts.get("props.typed_value.eventType.typed_value")).isEqualTo(3L);
                    assertThat(counts.get("props.typed_value.count.typed_value")).isEqualTo(4L);
                    assertThat(counts.get("props.typed_value.eventType.value")).isEqualTo(1L);
                    assertThat(counts.get("props.value")).isEqualTo(1L);
                    var all = new LinkedHashMap<>(UNSHREDDED_ROWS);
                    all.putAll(SHREDDED_ROWS);
                    assertThat(readJson(name)).isEqualTo(expected(all));
                } else {
                    assertThat(counts.get("props.typed_value.name.typed_value")).isPositive();
                    assertThat(counts.get("props.typed_value.items.typed_value.list.element.typed_value.name.typed_value")).isPositive();
                    assertThat(counts.get("props.typed_value.name.value")).isPositive();
                    assertThat(readJson(name)).isEqualTo(expected(PHASE_ONE_ROWS));
                }
            }
        }
    }

    @Test
    void readsShreddedVariantThroughPlugin() throws Exception {
        var all = new LinkedHashMap<>(UNSHREDDED_ROWS);
        all.putAll(SHREDDED_ROWS);
        // one query reads both layouts; every value equals the literal input, including the SQL NULL row
        assertThat(readJson("shredtest")).isEqualTo(expected(all));
        // and the same rows written unshredded read identically
        assertThat(readJson("shredref")).isEqualTo(expected(all));
    }

    @Test
    void projectionAndReferenceFilterMatchUnshreddedRows() throws Exception {
        List<String> queries = List.of(
                "SELECT num, CAST(props['name'] AS JSON) FROM %s ORDER BY num",
                "SELECT num, CAST(props['name'] AS JSON), CAST(props['eventType'] AS JSON) FROM %s ORDER BY num",
                "SELECT num, CAST(props['name'] AS JSON), CAST(props AS JSON) FROM %s ORDER BY num",
                "SELECT num, TRY(CAST(props['eventType'] AS BIGINT)) FROM %s ORDER BY num",
                "SELECT num, CASE WHEN num = 12 THEN 'integer' ELSE CAST(props['eventType'] AS VARCHAR) END FROM %s ORDER BY num",
                "SELECT num, CAST(geomesa_variant_get(props, ARRAY['name']) AS JSON) FROM %s ORDER BY num",
                "SELECT num, CAST(props['eventType'] AS JSON) FROM %s WHERE geomesa_variant_equals(props, ARRAY['name'], CAST('b' AS VARIANT)) ORDER BY num",
                "SELECT num, CAST(props['name'] AS JSON) FROM %s WHERE props = CAST(json_parse('{\"name\":\"b\",\"eventType\":\"view\",\"count\":2,\"extra\":\"x\"}') AS VARIANT) ORDER BY num");
        for (String query : queries) {
            List<List<Object>> reference = readSql(query.formatted(CATALOG + "." + SCHEMA + ".shredref"), false, false);
            for (String table : List.of("shredtest", "sharedtest")) {
                for (String mode : List.of("off", "reference", "typed")) {
                    for (boolean projection : List.of(false, true)) {
                        assertThat(readSql(query.formatted(CATALOG + "." + SCHEMA + "." + table), mode, projection, false))
                                .as("%s table=%s mode=%s projection=%s", query, table, mode, projection).isEqualTo(reference);
                    }
                }
            }
        }
    }

    @Test
    void metadataOptimizationCannotBypassVariantFilters() throws Exception {
        String table = CATALOG + "." + SCHEMA + ".shredidentity";
        for (String expected : List.of("b", "absent")) {
            String where = " FROM " + table + " WHERE geomesa_variant_equals(props, ARRAY['name'], CAST('" + expected + "' AS VARIANT))";
            for (String query : List.of("SELECT DISTINCT num" + where + " ORDER BY num", "SELECT min(num)" + where,
                    "SELECT max(num)" + where, "SELECT count(DISTINCT num)" + where)) {
                var reference = readSql(query, false, false, true);
                for (String mode : List.of("reference", "typed")) {
                    assertThat(readSql(query, mode, true, true)).as(mode + ": " + query).isEqualTo(reference);
                }
            }
        }
    }

    @Test
    void nativePrefiltersMatchUnshreddedRowsOnRemoteWorker() throws Exception {
        for (String predicate : List.of("props['name'] = CAST('b' AS VARIANT)",
                "props['name'] IN (CAST('b' AS VARIANT), CAST('c' AS VARIANT))", "props['name'] IS NULL", "props['name'] IS NOT NULL",
                "props['eventType'] = CAST(7 AS VARIANT)", "props['name'] = CAST('b' AS VARIANT) AND geomesa_variant_equals(props, ARRAY['count'], CAST(2 AS VARIANT))")) {
            for (String projection : List.of("num", "num, CAST(props AS JSON)", "num, CAST(props['name'] AS JSON)")) {
                String query = "SELECT " + projection + " FROM %s WHERE " + predicate + " ORDER BY num";
                var expected = readSql(query.formatted(CATALOG + "." + SCHEMA + ".shredref"), false, false);
                for (String table : List.of("shredtest", "sharedtest")) {
                    for (boolean project : List.of(false, true)) {
                        assertThat(readSql(query.formatted(CATALOG + "." + SCHEMA + "." + table), "typed", project, false, true))
                                .as("%s table=%s", query, table).isEqualTo(expected);
                    }
                }
            }
        }
    }

    private static List<List<Object>> readSql(String query, boolean filter, boolean projection) throws Exception {
        return readSql(query, filter, projection, false);
    }

    private static List<List<Object>> readSql(String query, boolean filter, boolean projection, boolean metadataOptimization) throws Exception {
        return readSql(query, filter ? "reference" : "off", projection, metadataOptimization);
    }

    private static List<List<Object>> readSql(String query, String mode, boolean projection, boolean metadataOptimization) throws Exception {
        return readSql(query, mode, projection, metadataOptimization, false);
    }

    private static List<List<Object>> readSql(String query, String mode, boolean projection, boolean metadataOptimization, boolean nativePrefilter) throws Exception {
        List<List<Object>> rows = new ArrayList<>();
        try (Connection connection = connect(); Statement statement = connection.createStatement()) {
            statement.execute("SET SESSION optimize_metadata_queries = " + metadataOptimization);
            statement.execute("SET SESSION " + CATALOG + ".variant_filter_mode = '" + mode + "'");
            statement.execute("SET SESSION " + CATALOG + ".variant_projection_pushdown_enabled = " + projection);
            statement.execute("SET SESSION " + CATALOG + ".variant_native_prefilter_enabled = " + nativePrefilter);
            try (ResultSet result = statement.executeQuery(query)) {
                while (result.next()) {
                    List<Object> row = new ArrayList<>();
                    for (int column = 1; column <= result.getMetaData().getColumnCount(); column++) {
                        String value = result.getString(column);
                        row.add(value != null && "json".equalsIgnoreCase(result.getMetaData().getColumnTypeName(column))
                                ? MAPPER.readTree(value) : value);
                    }
                    rows.add(row);
                }
            }
        }
        return rows;
    }

    private static TreeSet<String> prefilterIds(String table, boolean prefilter) throws Exception {
        var params = Map.<String, Object>of(
                TrinoDataStoreFactory.HOST.key, trino.getHost(),
                TrinoDataStoreFactory.PORT.key, trino.getFirstMappedPort(),
                TrinoDataStoreFactory.SCHEMA.key, SCHEMA);
        var ds = DataStoreFinder.getDataStore(params);
        try {
            var query = new Query(table, ECQL.toFilter("\"$.props.name\" = 'b'"), new String[]{"num"});
            if (prefilter) {
                query.getHints().put(QueryHints.TRINO_VARIANT_PREFILTER(), Boolean.TRUE);
            }
            var ids = new TreeSet<String>();
            try (var reader = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)) {
                while (reader.hasNext()) {
                    ids.add(reader.next().getID());
                }
            }
            return ids;
        } finally {
            ds.dispose();
        }
    }

    @Test
    void variantPrefilterWorksOnShreddedColumn() throws Exception {
        // reference: the same rows, all unshredded
        var reference = prefilterIds("shredref", true);
        assertThat(reference).containsExactlyInAnyOrder("1", "11", "12");
        // case sensitive: row 14 has "B"
        var shredded = prefilterIds("shredtest", true);
        assertThat(shredded).isEqualTo(reference);
        assertThat(prefilterIds("sharedtest", true)).isEqualTo(reference);
    }
    @Test
    void phaseOneCandidatesPreserveGeoToolsResultsOnShreddedAndUnshreddedFiles() throws Exception {
        var schema = SimpleFeatureTypes.createType("oracle", "num:Int,props:String:json=true,dtg:Date");
        var features = new LinkedHashMap<Integer, SimpleFeature>();
        for (var row : PHASE_ONE_ROWS.entrySet()) {
            var feature = new ScalaSimpleFeature(schema, row.getKey().toString(), null, null);
            feature.setAttribute("num", row.getKey());
            feature.setAttribute("props", row.getValue());
            features.put(row.getKey(), feature);
        }
        var ff = org.geotools.factory.CommonFactoryFinder.getFilterFactory();
        var filters = new ArrayList<org.geotools.api.filter.Filter>();
        for (String path : List.of("$.props.name", "$.props.items[*].name", "$.props.tags[*]", "$.props.bag.*.name", "$.props.bag.*", "$.props['*']")) {
            for (String literal : List.of("match", "001", "O'Brien")) filters.add(ff.equal(ff.property(path), ff.literal(literal), true));
        }
        filters.add(ECQL.toFilter("jsonPath('$.props.name') = 'match'"));
        filters.add(ECQL.toFilter("jsonPath(props, 'items[*].name') = 'match'"));
        for (var action : org.geotools.api.filter.MultiValuedFilter.MatchAction.values()) {
            filters.add(ff.equal(ff.property("$.props.tags[*]"), ff.literal("match"), true, action));
        }
        for (var filter : filters) {
            var expected = features.entrySet().stream().filter(row -> filter.evaluate(row.getValue())).map(Map.Entry::getKey).toList();
            var split = TrinoFeatureSource.splitFilter(filter, schema, true, TrinoFeatureSource.ClientSideFiltering.PARTIAL);
            assertThat(split.pushableSql()).isNotNull();
            assertThat(split.residualAttributes()).containsExactly("props");
            for (String table : List.of("phaseone", "phaseoneref", "sharedphase")) {
                for (String mode : List.of("off", "reference", "typed")) {
                    var candidates = readSql("SELECT num FROM " + CATALOG + "." + SCHEMA + "." + table + " WHERE " + split.pushableSql(), mode, true, false)
                        .stream().map(row -> Integer.parseInt((String) row.get(0))).toList();
                    assertThat(candidates).as("%s %s %s", table, mode, filter).containsAll(expected);
                    assertThat(candidates.stream().filter(id -> split.residual().evaluate(features.get(id))).toList())
                        .containsExactlyInAnyOrderElementsOf(expected);
                }
            }
        }
        for (var entry : Map.of("$.props.name", 1, "$.props.items[*].name", 5, "$.props.tags[*]", 11, "$.props.bag.*.name", 14, "$.props.bag.*", 16).entrySet()) {
            var filter = ff.equal(ff.property(entry.getKey()), ff.literal("match"), true);
            var sql = TrinoFeatureSource.splitFilter(filter, schema, true, TrinoFeatureSource.ClientSideFiltering.PARTIAL).pushableSql();
            for (String table : List.of("phaseone", "sharedphase")) {
                assertThat(readSql("SELECT num FROM " + CATALOG + "." + SCHEMA + "." + table + " WHERE " + sql, "typed", true, false))
                    .as("%s %s", table, filter).doesNotContain(List.of(entry.getValue().toString()));
            }
        }
    }

    @Test
    void fileBoundsExcludeWholeFilesAndPreserveFallbackDeletesAndCandidates() throws Exception {
        Path output = Path.of("target", "variant-file-validation");
        Files.createDirectories(output);
        try (var csv = Files.newBufferedWriter(output.resolve("queries.csv"));
                var catalog = catalog("http://" + iceberg.getHost() + ":" + iceberg.getFirstMappedPort() + "/")) {
            csv.write("table,function,mode,iteration,rows,eligible_files,physical_bytes,total_splits,cpu_ms,wall_ns\n");
            for (var fixture : FILE_ROWS.entrySet()) {
                String name = fixture.getKey();
                String column = name.endsWith("renamed") ? "payload" : "props";
                var table = catalog.loadTable(TableIdentifier.of(SCHEMA, name));
                var fileRecords = new ArrayList<String>();
                int total = 0;
                try (var tasks = table.newScan().includeColumnStats().planFiles()) {
                    for (var task : tasks) {
                        total++;
                        var file = task.file();
                        int field = table.schema().findField(column).fieldId();
                        var lower = file.lowerBounds() == null ? null : file.lowerBounds().get(field);
                        var upper = file.upperBounds() == null ? null : file.upperBounds().get(field);
                        fileRecords.add("rows=" + file.recordCount() + " bytes=" + file.fileSizeInBytes()
                                + " lower=" + (lower == null ? "absent" : org.apache.iceberg.variants.Variant.from(lower.duplicate().order(java.nio.ByteOrder.LITTLE_ENDIAN)))
                                + " upper=" + (upper == null ? "absent" : org.apache.iceberg.variants.Variant.from(upper.duplicate().order(java.nio.ByteOrder.LITTLE_ENDIAN)))
                                + " deletes=" + task.deletes().size());
                    }
                }
                assertThat(total).isEqualTo(5);
                Files.write(output.resolve(name + "-manifest-bounds.txt"), fileRecords);
                for (String function : List.of("geomesa_variant_equals", "geomesa_variant_may_equal")) {
                    boolean exact = function.endsWith("equals");
                    var expected = new ArrayList<Integer>();
                    for (var row : fixture.getValue().entrySet()) {
                        var value = row.getValue() == null ? null : MAPPER.readTree(row.getValue()).path("itinerary").path("origin").path("location").path("code");
                        if ((value != null && value.isTextual() && value.textValue().equals("SFO"))
                                || (!exact && (value == null || !value.isTextual()))) expected.add(row.getKey());
                    }
                    expected.sort(Integer::compareTo);
                    var filter = exact ? org.apache.iceberg.expressions.Expressions.equal(
                            org.apache.iceberg.expressions.Expressions.extract(column, "$." + String.join(".", FILE_PATH), "string"), "SFO")
                            : org.apache.iceberg.expressions.Expressions.alwaysTrue();
                    int planned = 0;
                    var evaluator = new org.apache.iceberg.expressions.InclusiveMetricsEvaluator(table.schema(), filter);
                    try (var tasks = table.newScan().includeColumnStats().planFiles()) {
                        for (var task : tasks) {
                            var file = task.file();
                            var lower = new java.util.HashMap<Integer, java.nio.ByteBuffer>();
                            var upper = new java.util.HashMap<Integer, java.nio.ByteBuffer>();
                            if (file.lowerBounds() != null) file.lowerBounds().forEach((id, buffer) -> lower.put(id, buffer.duplicate().order(java.nio.ByteOrder.LITTLE_ENDIAN)));
                            if (file.upperBounds() != null) file.upperBounds().forEach((id, buffer) -> upper.put(id, buffer.duplicate().order(java.nio.ByteOrder.LITTLE_ENDIAN)));
                            var metrics = new Metrics(file.recordCount(), file.columnSizes(), file.valueCounts(), file.nullValueCounts(), file.nanValueCounts(), lower, upper);
                            if (evaluator.eval(DataFiles.builder(table.spec()).copy(file).withMetrics(metrics).build())) planned++;
                        }
                    }
                    boolean prunable = exact && (name.endsWith("sorted") || name.endsWith("sparse") || name.endsWith("deleted") || name.endsWith("renamed"));
                    assertThat(planned).as("%s %s", name, function).isEqualTo(prunable ? 1 : 5);
                    String sql = "SELECT num FROM " + CATALOG + "." + SCHEMA + "." + name + " WHERE " + function
                            + "(" + column + ", ARRAY['" + String.join("','", FILE_PATH) + "'], CAST('SFO' AS VARIANT))";
                    long referenceBytes = -1;
                    int referenceSplits = -1;
                    for (String mode : List.of("off", "reference", "typed")) {
                        try (var connection = connect(); var statement = connection.createStatement()) {
                            statement.execute("SET SESSION " + CATALOG + ".variant_filter_mode = '" + mode + "'");
                            statement.execute("SET SESSION " + CATALOG + ".variant_projection_pushdown_enabled = true");
                            // Leave the default eager small-file threshold; disable row-group statistics.
                            statement.execute("SET SESSION " + CATALOG + ".parquet_ignore_statistics = true");
                            if (!mode.equals("off")) {
                                try (var plan = statement.executeQuery("EXPLAIN (TYPE DISTRIBUTED) " + sql)) {
                                    assertThat(plan.next()).isTrue();
                                    String text = plan.getString(1);
                                    assertThat(text).contains("VariantTableHandle[").doesNotContain(function);
                                    Files.writeString(output.resolve(name + "-" + function + "-" + mode + "-plan.txt"), text);
                                }
                            }
                            for (int iteration = -1; iteration < 2; iteration++) {
                                var ids = new ArrayList<Integer>();
                                long start = System.nanoTime();
                                try (var result = statement.executeQuery(sql)) {
                                    while (result.next()) ids.add(result.getInt(1));
                                    long elapsed = System.nanoTime() - start;
                                    ids.sort(Integer::compareTo);
                                    assertThat(ids).as("%s %s %s", name, function, mode).isEqualTo(expected);
                                    var stats = result.unwrap(io.trino.jdbc.TrinoResultSet.class).getStats();
                                    if (mode.equals("reference")) { referenceBytes = stats.getPhysicalInputBytes(); referenceSplits = stats.getTotalSplits(); }
                                    if (mode.equals("typed")) {
                                        if (prunable) {
                                            assertThat(stats.getPhysicalInputBytes()).isLessThan(referenceBytes);
                                            assertThat(stats.getTotalSplits()).isLessThan(referenceSplits);
                                        } else assertThat(stats.getPhysicalInputBytes()).isEqualTo(referenceBytes);
                                    }
                                    csv.write(String.join(",", name, function, mode, Integer.toString(iteration), Integer.toString(ids.size()),
                                            Integer.toString(mode.equals("typed") ? planned : total), Long.toString(stats.getPhysicalInputBytes()),
                                            Integer.toString(stats.getTotalSplits()), Long.toString(stats.getCpuTimeMillis()), Long.toString(elapsed)) + "\n");
                                    csv.flush();
                                }
                            }
                            if (mode.equals("typed") && exact) {
                                try (var plan = statement.executeQuery("EXPLAIN ANALYZE VERBOSE " + sql)) {
                                    assertThat(plan.next()).isTrue();
                                    String text = plan.getString(1);
                                    assertThat(text).contains("fileFilterSkippedFiles");
                                    Files.writeString(output.resolve(name + "-file-metrics-plan.txt"), text);
                                }
                            }
                        }
                    }
                }
                // No matches: valid path bounds must also support an empty planned scan.
                String absent = "SELECT count(*) FROM " + CATALOG + "." + SCHEMA + "." + name
                        + " WHERE geomesa_variant_equals(" + column + ", ARRAY['" + String.join("','", FILE_PATH) + "'], CAST('ZZZ' AS VARIANT))";
                for (String mode : List.of("off", "reference", "typed")) assertThat(readSql(absent, mode, true, true)).containsExactly(List.of("0"));
            }
        }
    }

    @Test
    void arrayCandidatesPreserveEveryMatchActionAndIndependentElements() throws Exception {
        var schema = SimpleFeatureTypes.createType("oracle", "num:Int,props:String:json=true,dtg:Date");
        var ff = org.geotools.factory.CommonFactoryFinder.getFilterFactory();
        for (var fixture : ARRAY_ROWS.entrySet()) {
            var features = new LinkedHashMap<Integer, SimpleFeature>();
            for (var row : fixture.getValue().entrySet()) {
                var feature = new ScalaSimpleFeature(schema, row.getKey().toString(), null, null);
                feature.setAttribute("num", row.getKey());
                feature.setAttribute("props", row.getValue());
                features.put(row.getKey(), feature);
            }
            for (var action : org.geotools.api.filter.MultiValuedFilter.MatchAction.values()) {
                var filter = ff.equal(ff.property("$.props.identity_data.dimensions[*].status.value"), ff.literal("RESOLVED"), true, action);
                var split = TrinoFeatureSource.splitFilter(filter, schema, true, TrinoFeatureSource.ClientSideFiltering.PARTIAL);
                assertThat(split.pushableSql()).startsWith("geomesa_variant_any_may_equal");
                assertThat(split.residualAttributes()).containsExactly("props");
                var expected = features.entrySet().stream().filter(row -> filter.evaluate(row.getValue())).map(Map.Entry::getKey).toList();
                List<Integer> baseline = null;
                for (String mode : List.of("off", "reference", "typed")) {
                    var candidates = readSql("SELECT num FROM " + CATALOG + "." + SCHEMA + "." + fixture.getKey()
                            + " WHERE " + split.pushableSql(), mode, true, false).stream()
                            .map(row -> Integer.parseInt((String) row.get(0))).sorted().toList();
                    if (baseline == null) baseline = candidates;
                    assertThat(candidates).as("%s %s %s", fixture.getKey(), action, mode).isEqualTo(baseline).containsAll(expected);
                    assertThat(candidates.stream().filter(id -> split.residual().evaluate(features.get(id))).toList())
                            .containsExactlyInAnyOrderElementsOf(expected);
                }
            }
        }
        String conjunction = "SELECT num FROM " + CATALOG + "." + SCHEMA + ".arraymixed WHERE "
                + "geomesa_variant_any_equals(props, ARRAY['identity_data','dimensions'], ARRAY['status','value'], CAST('RESOLVED' AS VARIANT)) AND "
                + "geomesa_variant_any_equals(props, ARRAY['identity_data','dimensions'], ARRAY['dimension_id'], CAST('TYPE' AS VARIANT))";
        var baseline = readSql(conjunction, "off", true, false);
        for (String mode : List.of("reference", "typed")) {
            assertThat(readSql(conjunction, mode, true, false)).containsExactlyInAnyOrderElementsOf(baseline);
        }
        for (int id = 7; id < 5000; id += 1000) assertThat(baseline).contains(List.of(Integer.toString(id)));
    }

    @Test
    void integerRangeCandidatesPreserveGeoToolsResultsAndRemoteClaims() throws Exception {
        assertRangeCandidates(RANGE_ROWS, List.of("outer", "score"), "104000");
    }

    @Test
    void fractionalCandidatesPreserveGeoToolsResultsAndRemoteClaims() throws Exception {
        assertRangeCandidates(FRACTIONAL_ROWS, List.of("position_error", "semi_major"), "5.0");
    }

    private static void assertRangeCandidates(Map<String, Map<Integer, String>> fixtures, List<String> path, String bound) throws Exception {
        var schema = SimpleFeatureTypes.createType("oracle", "num:Int,props:String:json=true,dtg:Date");
        for (String table : fixtures.keySet()) {
            var features = new LinkedHashMap<Integer, SimpleFeature>();
            for (var row : fixtures.get(table).entrySet()) {
                var feature = new ScalaSimpleFeature(schema, row.getKey().toString(), null, null);
                feature.setAttribute("num", row.getKey());
                feature.setAttribute("props", row.getValue());
                features.put(row.getKey(), feature);
            }
            for (String operator : List.of(">", ">=", "<", "<=")) {
                var filters = new ArrayList<org.geotools.api.filter.Filter>();
                String property = "$.props." + String.join(".", path);
                filters.add(ECQL.toFilter("\"" + property + "\" " + operator + " " + bound));
                if (fixtures == FRACTIONAL_ROWS) {
                    var ff = org.geotools.factory.CommonFactoryFinder.getFilterFactory();
                    for (Number literal : List.of(5.001f, new java.math.BigDecimal("5.00000000000000000001"))) {
                        var left = ff.property(property);
                        var right = ff.literal(literal);
                        filters.add(switch (operator) {
                            case ">" -> ff.greater(left, right);
                            case ">=" -> ff.greaterOrEqual(left, right);
                            case "<" -> ff.less(left, right);
                            default -> ff.lessOrEqual(left, right);
                        });
                    }
                }
                for (var filter : filters) {
                var split = TrinoFeatureSource.splitFilter(filter, schema, true, TrinoFeatureSource.ClientSideFiltering.PARTIAL);
                assertThat(split.pushableSql()).startsWith("geomesa_variant_may_");
                assertThat(split.residualAttributes()).containsExactly("props");
                var expected = features.entrySet().stream().filter(row -> filter.evaluate(row.getValue())).map(Map.Entry::getKey).toList();
                List<Integer> baseline = null;
                for (String mode : List.of("off", "reference", "typed")) {
                    String sql = "SELECT num FROM " + CATALOG + "." + SCHEMA + "." + table + " WHERE " + split.pushableSql();
                    var candidates = readSql(sql, mode, true, false).stream().map(row -> Integer.parseInt((String) row.get(0))).sorted().toList();
                    if (baseline == null) baseline = candidates;
                    assertThat(candidates).as("candidate parity %s %s %s", table, operator, mode).isEqualTo(baseline);
                    assertThat(candidates).containsAll(expected);
                    assertThat(candidates.stream().filter(id -> split.residual().evaluate(features.get(id))).toList())
                            .containsExactlyInAnyOrderElementsOf(expected);
                }
                }
            }
        }
    }

    @Test
    void integerRangeStatisticsSaveBytesOnlyForDisjointCompleteGroups() throws Exception {
        rangeStatistics("variant-numeric-validation", List.of(
                new RangeCase("rangesorted", List.of("outer", "score"), "CAST(104000 AS BIGINT)", true, true),
                new RangeCase("rangerandom", List.of("outer", "score"), "CAST(104000 AS BIGINT)", false, false),
                new RangeCase("rangemixed", List.of("outer", "score"), "CAST(104000 AS BIGINT)", false, false)));
    }

    @Test
    void fractionalRangeStatisticsRespectEncodingOrderingAndFallback() throws Exception {
        var cases = new ArrayList<RangeCase>();
        for (String name : FRACTIONAL_ROWS.keySet()) {
            boolean sorted = name.endsWith("sorted");
            cases.add(new RangeCase(name, List.of("position_error", "semi_major"), "CAST('5.0' AS DOUBLE)",
                    sorted, sorted && !name.startsWith("double")));
        }
        rangeStatistics("variant-fractional-validation", cases);
    }

    @Test
    void arrayStatisticsRespectRepeatedCountsNullsOrderingAndFallback() throws Exception {
        var cases = new ArrayList<RangeCase>();
        for (String name : ARRAY_ROWS.keySet()) {
            for (String function : List.of("geomesa_variant_any_equals", "geomesa_variant_any_may_equal")) {
                boolean exact = function.endsWith("equals");
                cases.add(new RangeCase(name, List.of("identity_data", "dimensions"), "'RESOLVED'",
                        name.equals("arraysorted") || name.equals("arrayrandom"),
                        name.equals("arraysorted") || (exact && name.equals("arraysparse")),
                        List.of("status", "value"), function));
            }
        }
        rangeStatistics("variant-array-validation", cases);
    }

    private record RangeCase(String table, List<String> path, String literal, boolean sorted, boolean prunable,
            List<String> elementPath, String function) {
        RangeCase(String table, List<String> path, String literal, boolean sorted, boolean prunable) {
            this(table, path, literal, sorted, prunable, null, "geomesa_variant_may_gte");
        }
    }

    private static void rangeStatistics(String outputName, List<RangeCase> cases) throws Exception {
        Path output = Path.of("target", outputName);
        Files.createDirectories(output);
        try (var csv = Files.newBufferedWriter(output.resolve("queries.csv"));
                var catalog = catalog("http://" + iceberg.getHost() + ":" + iceberg.getFirstMappedPort() + "/")) {
            csv.write("table,function,mode,ignore_statistics,iteration,rows,physical_bytes,cpu_ms,wall_ns\n");
            for (RangeCase fixture : cases) {
                String tableName = fixture.table();
                var physicalPath = new ArrayList<String>(List.of("props"));
                for (String key : fixture.path()) { physicalPath.add("typed_value"); physicalPath.add(key); }
                if (fixture.elementPath() != null) {
                    physicalPath.addAll(List.of("typed_value", "list", "element"));
                    for (String key : fixture.elementPath()) { physicalPath.add("typed_value"); physicalPath.add(key); }
                }
                physicalPath.add("typed_value");
                String outputPrefix = tableName + "-" + fixture.function();
                var table = catalog.loadTable(TableIdentifier.of(SCHEMA, tableName));
                int groups = 0;
                try (var tasks = table.newScan().planFiles()) {
                    for (var task : tasks) {
                        Path local = Files.createTempFile("variant-footer-", ".parquet");
                        try {
                            try (var input = table.io().newInputFile(task.file().location()).newStream()) {
                                Files.copy(input, local, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                            }
                            try (var reader = ParquetFileReader.open(new LocalInputFile(local))) {
                                var footer = reader.getFooter();
                                assertThat(footer.getFileMetaData().getSchema().containsPath(physicalPath.toArray(String[]::new))).isTrue();
                                groups += footer.getBlocks().size();
                                Files.writeString(output.resolve(outputPrefix + "-footer.txt"), footer.toString());
                                if (fixture.elementPath() != null) {
                                    for (var block : footer.getBlocks()) {
                                        var leaf = block.getColumns().stream().filter(column ->
                                                List.of(column.getPath().toArray()).equals(physicalPath)).findFirst().orElseThrow();
                                        assertThat(leaf.getValueCount()).isGreaterThan(block.getRowCount());
                                    }
                                }
                            }
                            assertThat(nonNullValues(local).get(String.join(".", physicalPath))).isPositive();
                        } finally { Files.delete(local); }
                    }
                }
                assertThat(groups).as("actual shared-writer groups for %s", tableName).isEqualTo(5);
                String sql = "SELECT num FROM " + CATALOG + "." + SCHEMA + "." + tableName
                        + " WHERE " + fixture.function() + "(props, ARRAY['" + String.join("', '", fixture.path())
                        + "'], " + (fixture.elementPath() == null ? "" : "ARRAY['" + String.join("', '", fixture.elementPath()) + "'], ")
                        + "CAST(" + fixture.literal() + " AS VARIANT))";
                List<Integer> expected = null;
                long ignoredBytes = -1;
                long typedBytes = -1;
                long eagerReferenceBytes = -1;
                long eagerTypedBytes = -1;
                var modes = fixture.prunable()
                        ? List.of("reference", "typed", "typed-ignore", "reference-small-file", "typed-small-file")
                        : List.of("reference", "typed", "typed-ignore");
                for (String mode : modes) {
                    boolean ignore = mode.equals("typed-ignore");
                    boolean eager = mode.endsWith("-small-file");
                    try (var connection = connect(); var statement = connection.createStatement()) {
                        statement.execute("SET SESSION " + CATALOG + ".variant_filter_mode = '" + (mode.startsWith("reference") ? "reference" : "typed") + "'");
                        statement.execute("SET SESSION " + CATALOG + ".variant_projection_pushdown_enabled = true");
                        statement.execute("SET SESSION " + CATALOG + ".parquet_ignore_statistics = " + ignore);
                        // Isolate row-group reads from the eager small-file optimization.
                        if (!eager) statement.execute("SET SESSION " + CATALOG + ".parquet_small_file_threshold = '0B'");
                        try (var plan = statement.executeQuery("EXPLAIN (TYPE DISTRIBUTED) " + sql)) {
                            assertThat(plan.next()).isTrue();
                            String text = plan.getString(1);
                            assertThat(text).contains("VariantTableHandle[").doesNotContain(fixture.function());
                            Files.writeString(output.resolve(outputPrefix + "-" + mode + "-plan.txt"), text);
                        }
                        for (int iteration = -1; iteration < 2; iteration++) {
                            var ids = new ArrayList<Integer>();
                            long start = System.nanoTime();
                            try (var result = statement.executeQuery(sql)) {
                                while (result.next()) ids.add(result.getInt(1));
                                long elapsed = System.nanoTime() - start;
                                ids.sort(Integer::compareTo);
                                if (expected == null) expected = List.copyOf(ids);
                                assertThat(ids).as("%s %s", tableName, mode).isEqualTo(expected);
                                var stats = result.unwrap(io.trino.jdbc.TrinoResultSet.class).getStats();
                                assertThat(stats.getState()).isEqualTo("FINISHED");
                                long bytes = stats.getPhysicalInputBytes();
                                if (mode.equals("reference-small-file")) eagerReferenceBytes = bytes;
                                else if (mode.equals("typed-small-file")) eagerTypedBytes = bytes;
                                else if (ignore) ignoredBytes = bytes;
                                else if (mode.equals("typed")) typedBytes = bytes;
                                csv.write(String.join(",", tableName, fixture.function(), mode, Boolean.toString(ignore), Integer.toString(iteration),
                                        Integer.toString(ids.size()), Long.toString(bytes), Long.toString(stats.getCpuTimeMillis()), Long.toString(elapsed)) + "\n");
                                csv.flush();
                            }
                        }
                    }
                }
                if (fixture.sorted()) assertThat(expected).hasSize(1000);
                if (fixture.prunable()) {
                    assertThat(typedBytes).isLessThan(ignoredBytes);
                    assertThat(eagerTypedBytes).as("eager small-file reads consume the same bytes despite row-group pruning").isEqualTo(eagerReferenceBytes);
                    assertThat(eagerTypedBytes).isGreaterThan(typedBytes);
                } else assertThat(typedBytes).isEqualTo(ignoredBytes);
            }
        }
    }

    @Test
    void rangeAndArrayDatastorePreserveHiddenResidualCountAndPaging() throws Exception {
        var ds = DataStoreFinder.getDataStore(Map.<String, Object>of(TrinoDataStoreFactory.HOST.key, trino.getHost(),
                TrinoDataStoreFactory.PORT.key, trino.getFirstMappedPort(), TrinoDataStoreFactory.SCHEMA.key, SCHEMA));
        try {
            TrinoFeatureSource.CLIENT_SIDE_FILTERING.threadLocalValue().set(TrinoFeatureSource.ClientSideFiltering.ALL.value);
            for (var example : Map.of("rangemixed", "\"$.props.outer.score\" >= 104000 AND \"$.props.outer.score\" < 104010",
                    "decimalmixed", "\"$.props.position_error.semi_major\" >= 5.0 AND \"$.props.position_error.semi_major\" < 5.01",
                    "arraysparse", "\"$.props.identity_data.dimensions[*].status.value\" = 'RESOLVED' AND num >= 4008 AND num < 4010").entrySet()) {
            var filter = ECQL.toFilter(example.getValue());
            List<String> baseline = null;
            for (boolean enabled : List.of(false, true)) {
                var query = new Query(example.getKey(), filter, new String[]{"num"});
                query.setSortBy(org.geotools.api.filter.sort.SortBy.NATURAL_ORDER);
                query.getHints().put(QueryHints.TRINO_VARIANT_PREFILTER(), enabled);
                assertThat(ds.getFeatureSource(example.getKey()).getCount(query)).isEqualTo(-1);
                var ids = new ArrayList<String>();
                try (var reader = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)) {
                    while (reader.hasNext()) {
                        var feature = reader.next();
                        assertThat(feature.getAttributeCount()).isEqualTo(1);
                        ids.add(feature.getID());
                    }
                }
                if (baseline == null) baseline = ids;
                assertThat(ids).isEqualTo(baseline);
                assertThat(ids).containsExactly("4008", "4009");
                query.setStartIndex(1);
                query.setMaxFeatures(1);
                try (var reader = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)) {
                    assertThat(reader.hasNext()).isTrue();
                    assertThat(reader.next().getID()).isEqualTo(baseline.get(1));
                    assertThat(reader.hasNext()).isFalse();
                }
            }
            }
        } finally {
            TrinoFeatureSource.CLIENT_SIDE_FILTERING.threadLocalValue().remove();
            ds.dispose();
        }
    }

    @Test
    void phaseOneDatastorePreservesProjectionCountAndPaging() throws Exception {
        var ds = DataStoreFinder.getDataStore(Map.<String, Object>of(TrinoDataStoreFactory.HOST.key, trino.getHost(),
            TrinoDataStoreFactory.PORT.key, trino.getFirstMappedPort(), TrinoDataStoreFactory.SCHEMA.key, SCHEMA));
        try {
            TrinoFeatureSource.CLIENT_SIDE_FILTERING.threadLocalValue().set(TrinoFeatureSource.ClientSideFiltering.ALL.value);
            for (String cql : List.of("\"$.props.items[*].name\" = 'match'", "\"$.props.tags[*]\" = 'match'",
                    "\"$.props.bag.*.name\" = 'match'", "jsonPath(props, 'items[*].name') = 'match'",
                    "\"$.props.name\" = '001'", "num >= 0 AND \"$.props.name\" = 'match'",
                    "NOT (\"$.props.items[*].name\" = 'match')", "\"$.props.name\" = 'match' OR num = 1")) {
                var filter = ECQL.toFilter(cql);
                List<String> baseline = null;
                for (String table : List.of("phaseoneref", "phaseone", "sharedphase")) {
                    var source = ds.getFeatureSource(table);
                    for (boolean enabled : List.of(false, true)) {
                        var query = new Query(table, filter, new String[]{"num"});
                        query.setSortBy(org.geotools.api.filter.sort.SortBy.NATURAL_ORDER);
                        query.getHints().put(QueryHints.TRINO_VARIANT_PREFILTER(), enabled);
                        assertThat(source.getCount(query)).isEqualTo(-1);
                        List<String> ids = new ArrayList<>();
                        try (var reader = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)) {
                            while (reader.hasNext()) {
                                var feature = reader.next();
                                assertThat(feature.getAttributeCount()).isEqualTo(1);
                                ids.add(feature.getID());
                            }
                        }
                        if (baseline == null) baseline = ids;
                        assertThat(ids).as("%s %s %s", table, enabled, cql).isEqualTo(baseline);
                        query.setStartIndex(1);
                        query.setMaxFeatures(1);
                        List<String> page = new ArrayList<>();
                        try (var reader = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)) {
                            while (reader.hasNext()) page.add(reader.next().getID());
                        }
                        assertThat(page).isEqualTo(baseline.stream().skip(1).limit(1).toList());
                    }
                }
            }
        } finally {
            TrinoFeatureSource.CLIENT_SIDE_FILTERING.threadLocalValue().remove();
            ds.dispose();
        }
    }

}

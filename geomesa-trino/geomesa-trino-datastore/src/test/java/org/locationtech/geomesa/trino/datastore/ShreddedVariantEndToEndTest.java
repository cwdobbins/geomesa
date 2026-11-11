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
import org.testcontainers.utility.MountableFile;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
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
 */
class ShreddedVariantEndToEndTest {

    private static final String SCHEMA = "geomesa";
    private static final String CATALOG = "spatial_iceberg";
    private static final ObjectMapper MAPPER = new ObjectMapper();

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

    /** every row is dated the same day, so the table has one 'daily' partition. */
    private static final String DTG = "2017-06-05T04:03:02.000Z";
    private static final long DTG_MICROS = java.time.Instant.parse(DTG).toEpochMilli() * 1000L;
    private static final int DTG_DAY = (int) java.time.LocalDate.of(2017, 6, 5).toEpochDay();

    private static final Network network = Network.newNetwork();

    private static final SeaweedFsContainer s3 =
            new SeaweedFsContainer("admin", "admin").withNetwork(network).withNetworkAliases("seaweed");

    private static final IcebergRestContainer iceberg =
            new IcebergRestContainer("admin", "admin").withNetwork(network).withNetworkAliases("rest-catalog");

    @SuppressWarnings("resource")
    private static final TrinoContainer trino =
            new GeoMesaTrinoContainer()
                    .withGeoMesaPlugin()
                    .withNetwork(network)
                    .withNetworkAliases("trino")
                    .withFileSystemBind(
                            MountableFile.forClasspathResource("docker/trino/spatial_iceberg.properties").getResolvedPath(),
                            "/etc/trino/catalog/spatial_iceberg.properties", BindMode.READ_ONLY);

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
            var all = new LinkedHashMap<>(UNSHREDDED_ROWS);
            all.putAll(SHREDDED_ROWS);
            write(fsds, referenceSft, all);
        } finally {
            fsds.dispose();
        }

        appendShreddedFile(restUri);

        trino.start();
    }

    @AfterAll
    static void tearDown() {
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

    private static void appendShreddedFile(String restUri) throws Exception {
        Catalog catalog = CatalogUtil.buildIcebergCatalog("test", Map.of(
                "type", "rest",
                "uri", restUri,
                "io-impl", "org.apache.iceberg.aws.s3.S3FileIO",
                "s3.endpoint", s3.getS3URL(),
                "s3.access-key-id", "admin",
                "s3.secret-access-key", "admin",
                "s3.path-style-access", "true",
                "client.region", "us-east-1"), null);
        Table table = catalog.loadTable(TableIdentifier.of(SCHEMA, "shredtest"));

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
            for (var row : SHREDDED_ROWS.entrySet()) {
                writer.write(row);
            }
        }

        // prove the file is shredded: the typed_value columns hold values (5 rows, one a SQL NULL variant)
        var counts = nonNullValues(local);
        assertThat(counts.get("props.typed_value.name.typed_value")).as("%s", counts).isEqualTo(4L);
        assertThat(counts.get("props.typed_value.eventType.typed_value")).as("%s", counts).isEqualTo(3L);
        assertThat(counts.get("props.typed_value.count.typed_value")).as("%s", counts).isEqualTo(4L);
        // the mismatching eventType (row 12) and the residual field (row 11) are held in 'value' columns
        assertThat(counts.get("props.typed_value.eventType.value")).as("%s", counts).isEqualTo(1L);
        assertThat(counts.get("props.value")).as("%s", counts).isEqualTo(1L);

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
                out.put(rs.getInt(1), json == null ? null : MAPPER.readTree(json));
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
    void readsShreddedVariantThroughPlugin() throws Exception {
        var all = new LinkedHashMap<>(UNSHREDDED_ROWS);
        all.putAll(SHREDDED_ROWS);
        // one query reads both layouts; every value equals the literal input, including the SQL NULL row
        assertThat(readJson("shredtest")).isEqualTo(expected(all));
        // and the same rows written unshredded read identically
        assertThat(readJson("shredref")).isEqualTo(expected(all));
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
    }
}

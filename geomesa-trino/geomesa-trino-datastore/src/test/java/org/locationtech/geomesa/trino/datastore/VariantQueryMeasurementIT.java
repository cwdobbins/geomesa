/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.LocalInputFile;
import org.geotools.api.data.DataStoreFinder;
import org.junit.jupiter.api.Test;
import org.locationtech.geomesa.fs.data.FileSystemDataStore;
import org.locationtech.geomesa.fs.storage.core.parquet.io.ParquetFileMetrics;
import org.locationtech.geomesa.trino.datastore.testcontainers.GeoMesaTrinoContainer;
import org.locationtech.geomesa.trino.datastore.testcontainers.IcebergRestContainer;
import org.locationtech.geomesa.trino.datastore.testcontainers.SeaweedFsContainer;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.trino.TrinoContainer;
import io.trino.jdbc.QueryStats;
import io.trino.jdbc.TrinoResultSet;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Explicit, source-checked distributed measurements; excluded from the normal unit suite. */
class VariantQueryMeasurementIT {
    private static final ObjectMapper JSON = new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private static final String CATALOG = "spatial_iceberg";
    private static final String SCHEMA = "geomesa";
    private static final Set<String> SHAPES = Set.of(System.getProperty(
            "geomesa.variant.measurement-shapes", "itinerary,position_error").split(","));
    private static final Set<String> FILE_MODES = Set.of("native", "plain", "bounded", "default");

    private record Dataset(String name, String shape, String order, JsonNode definition, Map<Long, JsonNode> source,
                           List<String> path, List<String> elementPath, JsonNode literal, Set<Long> matches) {}
    private record Job(Dataset dataset, String storage, String mode, String workload, String table, String sql) {}
    private record Row(long id, String value) {}

    private static final class Node extends GeoMesaTrinoContainer {
        @Override protected void waitUntilContainerStarted() {
            Wait.forHttp("/v1/info").forPort(8080).forResponsePredicate(body -> {
                try { return !JSON.readTree(body).path("starting").asBoolean(true); }
                catch (java.io.IOException e) { return false; }
            }).withStartupTimeout(Duration.ofMinutes(2)).waitUntilReady(this);
        }
    }

    private final class Cluster implements AutoCloseable {
        final Network network = Network.newNetwork();
        final SeaweedFsContainer s3 = new SeaweedFsContainer("admin", "admin").withNetwork(network).withNetworkAliases("seaweed");
        final IcebergRestContainer iceberg = new IcebergRestContainer("admin", "admin").withNetwork(network).withNetworkAliases("rest-catalog");
        final TrinoContainer coordinator;
        final TrinoContainer worker;

        Cluster() throws Exception {
            coordinator = node("phase3-coordinator.properties", "trino");
            worker = node("phase3-worker.properties", "trino-worker");
        }

        private TrinoContainer node(String file, String alias) throws Exception {
            byte[] config;
            byte[] catalog;
            try (var in = getClass().getResourceAsStream("/docker/trino/" + file)) { config = in.readAllBytes(); }
            try (var in = getClass().getResourceAsStream("/docker/trino/spatial_iceberg.properties")) { catalog = in.readAllBytes(); }
            return new Node().withGeoMesaPlugin().withNetwork(network).withNetworkAliases(alias)
                    .withCopyToContainer(Transferable.of(config, 0644), "/etc/trino/config.properties")
                    .withCopyToContainer(Transferable.of(catalog, 0644), "/etc/trino/catalog/spatial_iceberg.properties");
        }

        void start(List<Dataset> datasets, Path fixtures) throws Exception {
            s3.start(); iceberg.start();
            String config = String.join("\n", "type=rest",
                    "uri=http://" + iceberg.getHost() + ":" + iceberg.getFirstMappedPort() + "/", "iceberg.namespace=" + SCHEMA,
                    "fs.s3.endpoint=" + s3.getS3URL(), "fs.s3.region=us-east-1", "fs.s3.access-key-id=admin",
                    "fs.s3.secret-access-key=admin", "fs.s3.force-path-style=true");
            var fs = (FileSystemDataStore) DataStoreFinder.getDataStore(Map.of("fs.config.properties", config));
            try {
                for (Dataset dataset : datasets) {
                    Set<String> found = new HashSet<>();
                    for (JsonNode file : dataset.definition().get("files")) {
                        String mode = file.get("mode").asText();
                        if (!FILE_MODES.contains(mode)) continue;
                        assertThat(found.add(mode)).isTrue();
                        String name = tableName(dataset, mode);
                        var sft = SimpleFeatureTypes.createType(name, Files.readString(fixtures.resolve(dataset.name()).resolve(mode + "-schema.txt")));
                        sft.getUserData().put("geomesa.fs.scheme", "hourly");
                        // The externally produced measurement schema intentionally names its oracle column id.
                        sft.getUserData().put("override.reserved.words", Boolean.TRUE);
                        fs.createSchema(sft);
                        var table = fs.storage(name).table();
                        var declared = SchemaParser.fromJson(Files.readString(fixtures.resolve(file.get("iceberg_schema").asText())));
                        assertThat(table.schema().asStruct()).isEqualTo(declared.asStruct());
                        assertThat(table.spec().fields()).hasSize(1);
                        assertThat(table.spec().fields().get(0).transform().toString()).isEqualTo("hour");
                        Path path = fixtures.resolve(file.get("file").asText());
                        assertThat(Files.size(path)).isEqualTo(file.get("bytes").longValue());
                        var partition = new PartitionData(table.spec().partitionType());
                        try (var reader = ParquetFileReader.open(new LocalInputFile(path))) {
                            var footer = reader.getFooter();
                            assertThat(ParquetSchemaUtil.convert(footer.getFileMetaData().getSchema()).asStruct())
                                    .as("Physical field IDs and types for %s/%s", dataset.name(), mode).isEqualTo(declared.asStruct());
                            assertThat(footer.getBlocks().stream().mapToLong(b -> b.getRowCount()).sum()).isEqualTo(dataset.source().size());
                            for (var field : footer.getFileMetaData().getSchema().getFields()) {
                                assertThat(field.getId().intValue()).isEqualTo(declared.findField(field.getName()).fieldId());
                            }
                            JsonNode key = JSON.readTree(footer.getFileMetaData().getKeyValueMetaData().get("geomesa.fs.partition"));
                            assertThat(key).hasSize(1);
                            assertThat(key.get(0).get("name").asText()).isEqualTo("hours:attribute=dtg");
                            partition.set(0, Integer.parseInt(key.get(0).get("value").asText()));
                        }
                        String destination = table.locationProvider().newDataLocation("measurement-" + UUID.randomUUID() + ".parquet");
                        try (var out = table.io().newOutputFile(destination).create()) { Files.copy(path, out); }
                        var input = table.io().newInputFile(destination);
                        var metrics = ParquetFileMetrics.fileMetrics(input, MetricsConfig.forTable(table));
                        assertThat(metrics.recordCount()).isEqualTo(dataset.source().size());
                        table.newAppend().appendFile(DataFiles.builder(table.spec()).withPath(destination).withFormat(FileFormat.PARQUET)
                                .withPartition(partition).withFileSizeInBytes(input.getLength()).withMetrics(metrics).build()).commit();
                    }
                    assertThat(found).containsExactlyInAnyOrderElementsOf(fileModes(dataset));
                }
            } finally { fs.dispose(); }
            coordinator.start(); worker.start();
            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            while (true) {
                try (var connection = connection(); var statement = connection.createStatement();
                        var result = statement.executeQuery("SELECT count(*) FROM system.runtime.nodes WHERE state='active' AND NOT coordinator")) {
                    assertThat(result.next()).isTrue();
                    if (result.getLong(1) == 1) break;
                }
                assertThat(System.nanoTime()).as("Worker registration timeout").isLessThan(deadline);
                Thread.sleep(250);
            }
        }

        java.sql.Connection connection() throws Exception {
            var props = new Properties(); props.setProperty("user", "admin");
            return DriverManager.getConnection("jdbc:trino://" + coordinator.getHost() + ":" + coordinator.getFirstMappedPort(), props);
        }

        @Override public void close() {
            try { worker.stop(); } finally {
                try { coordinator.stop(); } finally {
                    try { iceberg.stop(); } finally { try { s3.stop(); } finally { network.close(); } }
                }
            }
        }
    }

    @Test
    void measureWholeProjectionAndSelectiveQueriesOnFreshStockClusters() throws Exception {
        Path fixtures = requiredDirectory("geomesa.variant.measurement-directory");
        String outputProperty = System.getProperty("geomesa.variant.measurement-output");
        assertThat(outputProperty).as("Set geomesa.variant.measurement-output").isNotBlank();
        Path output = Path.of(outputProperty);
        Files.createDirectories(output);
        assertThat(Files.exists(output.resolve("queries.csv"))).as("Use a fresh result directory").isFalse();
        int forks = Integer.getInteger("geomesa.variant.measurement-clusters", 2);
        int warmups = Integer.getInteger("geomesa.variant.measurement-warmups", 2);
        int repeats = Integer.getInteger("geomesa.variant.measurement-repeats", 5);
        assertThat(forks).isBetween(1, 2);
        assertThat(warmups).isGreaterThanOrEqualTo(0);
        assertThat(repeats).isGreaterThanOrEqualTo(1);
        String pluginProperty = System.getProperty("trino.plugin.path.2.12", System.getProperty("trino.plugin.path.2.13"));
        assertThat(pluginProperty).as("Set trino.plugin.path for the configured Scala version").isNotBlank();
        Path plugin = Path.of(pluginProperty);
        String pluginHash = hash(plugin);
        Path manifestPath = fixtures.resolve("manifest.json");
        JsonNode manifest = JSON.readTree(Files.readString(manifestPath));
        List<Dataset> datasets = datasets(fixtures, manifest);
        assertThat(datasets).isNotEmpty();
        Map<String, String> inputs = new TreeMap<>();
        inputs.put("manifest.json", hash(manifestPath));
        for (Dataset dataset : datasets) {
            inputs.put(dataset.name() + "/expected.jsonl", hash(fixtures.resolve(dataset.name()).resolve("expected.jsonl")));
            for (JsonNode file : dataset.definition().get("files")) {
                if (FILE_MODES.contains(file.get("mode").asText())) {
                    String path = file.get("file").asText(); inputs.put(path, hash(fixtures.resolve(path)));
                    String schema = file.get("iceberg_schema").asText(); inputs.put(schema, hash(fixtures.resolve(schema)));
                    String sft = dataset.name() + "/" + file.get("mode").asText() + "-schema.txt";
                    inputs.put(sft, hash(fixtures.resolve(sft)));
                }
            }
        }
        var provenance = new LinkedHashMap<String, Object>();
        provenance.put("plugin_sha256", pluginHash); provenance.put("inputs_sha256", inputs);
        provenance.put("warmups", warmups); provenance.put("repeats", repeats); provenance.put("fresh_clusters", forks);
        provenance.put("shapes", SHAPES.stream().sorted().toList());
        provenance.put("allow_degenerate_predicates", Boolean.getBoolean("geomesa.variant.measurement-allow-degenerate-predicates"));
        provenance.put("predicate_match_counts", datasets.stream().collect(java.util.stream.Collectors.toMap(
                Dataset::name, dataset -> Map.of("rows", dataset.source().size(), "matches", dataset.matches().size()))));
        provenance.put("server_image", GeoMesaTrinoContainer.IMAGE.asCanonicalNameString());
        provenance.put("timing_scope", "JDBC execute plus complete result consumption; oracle checking and session setup excluded. Server metrics from final Trino JDBC QueryStats.");
        provenance.put("order", "All case/storage/mode/workload jobs shuffled once per iteration with fixed seed; both orders use the same original source IDs.");
        provenance.put("plan_preflight", "Every query configuration must claim its intended connector path or predicate before warmups; whole reads and off-mode residual filters are also checked. Plans are archived per fork.");
        JSON.writerWithDefaultPrettyPrinter().writeValue(output.resolve("provenance.json").toFile(), provenance);
        List<Job> jobs = jobs(datasets);
        List<Map<String, Object>> environments = new ArrayList<>();
        try (BufferedWriter csv = Files.newBufferedWriter(output.resolve("queries.csv"))) {
            csv.write("fork,case,storage,mode,workload,iteration,measured,rows,jdbc_wall_nanos,server_cpu_ms,server_elapsed_ms,processed_rows,processed_bytes,physical_input_bytes,peak_memory_bytes,query_id\n");
            for (int fork = 0; fork < forks; fork++) {
                assertThat(hash(plugin)).isEqualTo(pluginHash);
                try (var cluster = new Cluster()) {
                    cluster.start(datasets, fixtures);
                    verifyPlans(cluster, jobs, output, fork);
                    var environment = new LinkedHashMap<String, Object>();
                    environment.put("fork", fork);
                    environment.put("coordinator", containerEnvironment(cluster.coordinator));
                    environment.put("worker", containerEnvironment(cluster.worker));
                    environment.put("client_before", clientEnvironment());
                    environments.add(environment);
                    JSON.writerWithDefaultPrettyPrinter().writeValue(output.resolve("environment.json").toFile(), environments);
                    for (int iteration = -warmups; iteration < repeats; iteration++) {
                        List<Job> order = new ArrayList<>(jobs);
                        Collections.shuffle(order, new Random(20261112L + fork * 1000L + iteration));
                        for (Job job : order) measure(cluster, csv, fork, iteration, job);
                    }
                    environment.put("client_after", clientEnvironment());
                    JSON.writerWithDefaultPrettyPrinter().writeValue(output.resolve("environment.json").toFile(), environments);
                }
            }
        }
        assertThat(hash(plugin)).isEqualTo(pluginHash);
        for (var input : inputs.entrySet()) assertThat(hash(fixtures.resolve(input.getKey()))).isEqualTo(input.getValue());
        Files.writeString(output.resolve("SUCCESS"), "All source oracles and immutable input checks passed; see provenance.json for cluster and repetition counts.\n");
    }

    private static List<Dataset> datasets(Path root, JsonNode manifest) throws Exception {
        List<Dataset> datasets = new ArrayList<>();
        for (JsonNode entry : manifest.get("cases")) {
            String shape = entry.get("shape").asText();
            if (!SHAPES.contains(shape)) continue;
            String name = entry.get("case").asText();
            List<String> path = new ArrayList<>(); entry.get("predicate").get("path").forEach(key -> path.add(key.asText()));
            List<String> elementPath = new ArrayList<>();
            entry.get("predicate").path("element_path").forEach(key -> elementPath.add(key.asText()));
            JsonNode literal = entry.get("predicate").get("value");
            Map<Long, JsonNode> rows = new TreeMap<>();
            Set<Long> matches = new HashSet<>();
            try (var lines = Files.lines(root.resolve(name).resolve("expected.jsonl"))) {
                for (String line : (Iterable<String>) lines::iterator) {
                    JsonNode row = JSON.readTree(line);
                    long id = row.get("id").longValue();
                    assertThat(rows).doesNotContainKey(id);
                    JsonNode doc = JSON.readTree(row.get("doc").textValue());
                    rows.put(id, doc);
                    JsonNode selected = at(doc, path);
                    if (elementPath.isEmpty()) {
                        if (sameScalar(selected, literal)) matches.add(id);
                    } else if (selected != null && selected.isArray()) {
                        for (JsonNode element : selected) {
                            if (sameScalar(at(element, elementPath), literal)) { matches.add(id); break; }
                        }
                    }
                }
            }
            assertThat(rows).hasSize(entry.get("source_rows").intValue());
            assertThat(rows.keySet().stream().mapToLong(Long::longValue).sum()).isEqualTo(entry.get("all_id_sum").longValue());
            assertThat(matches).hasSize(entry.get("matching_count").intValue());
            assertThat(matches.stream().mapToLong(Long::longValue).sum()).isEqualTo(entry.get("matching_id_sum").longValue());
            // Representative sources may have constant fields. Keep their actual distribution;
            // explicit opt-in records that these queries do not demonstrate selective pruning.
            if (!Boolean.getBoolean("geomesa.variant.measurement-allow-degenerate-predicates")) {
                assertThat(matches).isNotEmpty().hasSizeLessThan(rows.size());
            }
            datasets.add(new Dataset(name, shape, entry.get("order").asText(), entry, rows, path, elementPath, literal, matches));
        }
        return datasets;
    }

    private static List<Job> jobs(List<Dataset> datasets) {
        List<Job> jobs = new ArrayList<>();
        for (Dataset dataset : datasets) {
            for (String storage : fileModes(dataset)) {
                String table = CATALOG + "." + SCHEMA + "." + tableName(dataset, storage);
                boolean nativeRow = storage.equals("native");
                String path = nativeRow ? "doc." + dataset.path().stream().map(VariantQueryMeasurementIT::identifier).collect(java.util.stream.Collectors.joining(".")) :
                        "geomesa_variant_get(doc, ARRAY[" + dataset.path().stream().map(VariantQueryMeasurementIT::literal).collect(java.util.stream.Collectors.joining(",")) + "])";
                String value = dataset.literal().isTextual() ? literal(dataset.literal().asText()) : dataset.literal().toString();
                String predicate = nativeRow ? path + " = " + value : "geomesa_variant_equals(doc, ARRAY[" +
                        dataset.path().stream().map(VariantQueryMeasurementIT::literal).collect(java.util.stream.Collectors.joining(",")) + "], CAST(" + value + " AS VARIANT))";
                if (!dataset.elementPath().isEmpty()) {
                    String element = dataset.elementPath().stream().map(VariantQueryMeasurementIT::identifier)
                            .collect(java.util.stream.Collectors.joining("."));
                    predicate = nativeRow ? "any_match(" + path + ", element -> element." + element + " = " + value + ")" :
                            "geomesa_variant_any_equals(doc, ARRAY[" + dataset.path().stream().map(VariantQueryMeasurementIT::literal)
                                    .collect(java.util.stream.Collectors.joining(",")) + "], ARRAY[" +
                                    dataset.elementPath().stream().map(VariantQueryMeasurementIT::literal)
                                            .collect(java.util.stream.Collectors.joining(",")) + "], CAST(" + value + " AS VARIANT))";
                }
                for (String mode : nativeRow ? List.of("off") : List.of("off", "reference", "typed")) {
                    jobs.add(new Job(dataset, storage, mode, "whole", table, "SELECT id, CAST(doc AS JSON) FROM " + table));
                    jobs.add(new Job(dataset, storage, mode, "projection", table, "SELECT id, CAST(" + path + " AS JSON) FROM " + table));
                    jobs.add(new Job(dataset, storage, mode, "selective", table, "SELECT id FROM " + table + " WHERE " + predicate));
                }
            }
        }
        return jobs;
    }

    private static void verifyPlans(Cluster cluster, List<Job> jobs, Path output, int fork) throws Exception {
        Path directory = Files.createDirectories(output.resolve("plans").resolve("fork-" + fork));
        try (var connection = cluster.connection(); var statement = connection.createStatement()) {
            statement.execute("SET SESSION optimize_metadata_queries = false");
            statement.execute("SET SESSION " + CATALOG + ".variant_projection_pushdown_enabled = true");
            for (Job job : jobs) {
                statement.execute("SET SESSION " + CATALOG + ".variant_filter_mode = '" + job.mode() + "'");
                StringBuilder text = new StringBuilder();
                try (var result = statement.executeQuery("EXPLAIN (TYPE DISTRIBUTED) " + job.sql())) {
                    while (result.next()) text.append(result.getString(1)).append('\n');
                }
                String plan = text.toString();
                String name = job.dataset().name() + "-" + job.storage() + "-" + job.mode() + "-" + job.workload();
                Files.writeString(directory.resolve(name + ".txt"), plan);
                if (job.storage().equals("native")) {
                    if (job.workload().equals("projection")) {
                        String field = job.dataset().path().get(job.dataset().path().size() - 1);
                        assertThat(plan).as(name).contains(":" + field + ":").doesNotContain(":doc:row(");
                    }
                } else if (job.workload().equals("whole")) {
                    assertThat(plan).as(name).contains(":doc:variant").doesNotContain(":doc:variant:[");
                } else if (job.workload().equals("projection")) {
                    assertThat(plan).as(name + " must request a restricted VARIANT path").contains(":doc:variant:[");
                } else if (job.mode().equals("off")) {
                    assertThat(plan).as(name + " must retain the engine predicate")
                            .contains(job.dataset().elementPath().isEmpty() ? "geomesa_variant_equals" : "geomesa_variant_any_equals")
                            .doesNotContain("VariantTableHandle[");
                } else {
                    assertThat(plan).as(name + " must enforce its predicate in the connector")
                            .contains("VariantTableHandle[")
                            .doesNotContain(job.dataset().elementPath().isEmpty() ? "geomesa_variant_equals" : "geomesa_variant_any_equals");
                }
            }
        }
    }

    private static void measure(Cluster cluster, BufferedWriter csv, int fork, int iteration, Job job) throws Exception {
        List<Row> rows = new ArrayList<>();
        QueryStats stats;
        long elapsed;
        try (var connection = cluster.connection(); var statement = connection.createStatement()) {
            statement.execute("SET SESSION optimize_metadata_queries = false");
            statement.execute("SET SESSION " + CATALOG + ".variant_filter_mode = '" + job.mode() + "'");
            statement.execute("SET SESSION " + CATALOG + ".variant_projection_pushdown_enabled = true");
            long start = System.nanoTime();
            try (var result = statement.executeQuery(job.sql())) {
                while (result.next()) rows.add(new Row(result.getLong(1), job.workload().equals("selective") ? null : result.getString(2)));
                elapsed = System.nanoTime() - start;
                stats = result.unwrap(TrinoResultSet.class).getStats();
            }
        }
        assertThat(stats.getState()).isEqualTo("FINISHED");
        Set<Long> expected = job.workload().equals("selective") ? job.dataset().matches() : job.dataset().source().keySet();
        Set<Long> seen = new HashSet<>();
        for (Row row : rows) {
            assertThat(seen.add(row.id())).as("Duplicate id in %s/%s/%s/%s", job.dataset().name(), job.storage(), job.mode(), job.workload()).isTrue();
            assertThat(expected.contains(row.id())).isTrue();
            if (!job.workload().equals("selective")) {
                JsonNode doc = job.dataset().source().get(row.id());
                JsonNode value = job.workload().equals("whole") ? doc : at(doc, job.dataset().path());
                assertJson(value, row.value() == null ? null : JSON.readTree(row.value()));
            }
        }
        assertThat(seen.equals(expected)).isTrue();
        assertThat(rows.stream().mapToLong(Row::id).sum()).isEqualTo(expected.stream().mapToLong(Long::longValue).sum());
        csv.write(String.join(",", Integer.toString(fork), job.dataset().name(), job.storage(), job.mode(), job.workload(),
                Integer.toString(iteration), Boolean.toString(iteration >= 0), Integer.toString(rows.size()), Long.toString(elapsed),
                Long.toString(stats.getCpuTimeMillis()), Long.toString(stats.getElapsedTimeMillis()), Long.toString(stats.getProcessedRows()),
                Long.toString(stats.getProcessedBytes()), Long.toString(stats.getPhysicalInputBytes()), Long.toString(stats.getPeakMemoryBytes()), stats.getQueryId()));
        csv.newLine(); csv.flush();
    }

    private static void assertJson(JsonNode expected, JsonNode actual) {
        if (expected == null || expected.isNull()) { assertThat(actual == null || actual.isNull()).isTrue(); return; }
        assertThat(actual).isNotNull();
        if (expected.isNumber()) { assertThat(actual.isNumber()).isTrue(); assertThat(actual.decimalValue()).isEqualByComparingTo(expected.decimalValue()); }
        else if (expected.isObject()) {
            assertThat(actual.isObject()).isTrue();
            Set<String> names = new HashSet<>(); expected.fieldNames().forEachRemaining(names::add);
            Set<String> actualNames = new HashSet<>(); actual.fieldNames().forEachRemaining(actualNames::add);
            assertThat(actualNames).isEqualTo(names);
            names.forEach(name -> assertJson(expected.get(name), actual.get(name)));
        } else if (expected.isArray()) {
            assertThat(actual.isArray()).isTrue(); assertThat(actual.size()).isEqualTo(expected.size());
            for (int i = 0; i < expected.size(); i++) assertJson(expected.get(i), actual.get(i));
        } else assertThat(actual).isEqualTo(expected);
    }

    private static Map<String, Object> containerEnvironment(TrinoContainer node) {
        var info = node.getContainerInfo();
        var limits = info.getHostConfig();
        var result = new LinkedHashMap<String, Object>();
        result.put("container_id", node.getContainerId()); result.put("image_id", info.getImageId());
        result.put("memory_limit_bytes", limits.getMemory()); result.put("nano_cpus", limits.getNanoCPUs());
        result.put("cpu_period", limits.getCpuPeriod()); result.put("cpu_quota", limits.getCpuQuota());
        result.put("cpuset_cpus", limits.getCpusetCpus());
        return result;
    }

    private static Map<String, Object> clientEnvironment() {
        var result = new LinkedHashMap<String, Object>();
        result.put("time", java.time.Instant.now().toString());
        result.put("java_version", System.getProperty("java.version"));
        result.put("os_arch", System.getProperty("os.arch"));
        result.put("available_processors", Runtime.getRuntime().availableProcessors());
        result.put("heap_max_bytes", Runtime.getRuntime().maxMemory());
        result.put("system_load_average", java.lang.management.ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage());
        return result;
    }

    private static JsonNode at(JsonNode value, List<String> path) { for (String key : path) value = value == null ? null : value.get(key); return value; }
    private static boolean sameScalar(JsonNode a, JsonNode b) {
        if (a == null || a.isNull()) return b == null || b.isNull();
        return a.isNumber() && b.isNumber() ? a.decimalValue().compareTo(b.decimalValue()) == 0 : a.equals(b);
    }
    private static String tableName(Dataset d, String mode) { return d.name().replace('-', '_') + "_" + mode; }
    private static Set<String> fileModes(Dataset dataset) {
        Set<String> modes = new HashSet<>();
        for (JsonNode file : dataset.definition().get("files")) {
            String mode = file.get("mode").asText();
            if (FILE_MODES.contains(mode)) modes.add(mode);
        }
        return modes;
    }
    private static String identifier(String s) { return "\"" + s.replace("\"", "\"\"") + "\""; }
    private static String literal(String s) { return "'" + s.replace("'", "''") + "'"; }
    private static Path requiredDirectory(String key) {
        String value = System.getProperty(key); assertThat(value).as("Set %s", key).isNotBlank();
        Path path = Path.of(value); assertThat(Files.isDirectory(path)).isTrue(); return path;
    }
    private static String hash(Path file) throws Exception {
        var digest = MessageDigest.getInstance("SHA-256");
        try (var in = Files.newInputStream(file)) { byte[] buffer = new byte[65536]; int n; while ((n = in.read(buffer)) != -1) digest.update(buffer, 0, n); }
        return HexFormat.of().formatHex(digest.digest());
    }
}

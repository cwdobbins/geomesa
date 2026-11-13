/* Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * Licensed under the Apache License, Version 2.0. */
package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.testing.PlanTester;
import org.apache.iceberg.SchemaParser;

import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Planned SQL measurements consuming actual sampled/shared-writer Parquet and independent source manifests. */
public final class ActualWriterSqlMeasurement {
    private static final ObjectMapper JSON = new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS);
    private static final List<ConnectorPageSource> SOURCES = new ArrayList<>();
    private static final com.sun.management.ThreadMXBean THREADS = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
    private static final com.sun.management.OperatingSystemMXBean PROCESS = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    private record Cell(String name, String mode, boolean projection, boolean selective, String sql, List<Object> expected) {}
    private ActualWriterSqlMeasurement() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 5) throw new IllegalArgumentException("Usage: ActualWriterSqlMeasurement <writer-fork-directory> <new-output.csv> <warmup> <repeats> <fork>");
        Path root = Path.of(args[0]); Path output = Path.of(args[1]);
        if (Files.exists(output)) throw new IllegalArgumentException("Refusing existing measurement output");
        int warmup = Integer.parseInt(args[2]), repeats = Integer.parseInt(args[3]), fork = Integer.parseInt(args[4]);
        if (warmup < 0 || repeats < 1) throw new IllegalArgumentException("Invalid repetitions");
        THREADS.setThreadAllocatedMemoryEnabled(true);
        JsonNode manifest = JSON.readTree(root.resolve("manifest.json").toFile());
        try (var csv = Files.newBufferedWriter(output)) {
            csv.write("case,layout,cell,mode,projection,selective,iteration,rows,file_bytes,wall_ns,cpu_ns,allocated_bytes,gc_ms,gc_count,read_bytes,read_ns,leaf_decodes,leaf_decode_ns,decision_ns,reference_reconstruction_ns,output_reconstruction_ns,reference_rows,output_rows,typed_rows,fallback_rows,unknown_rows\n");
            for (JsonNode scenario : manifest.get("cases")) {
                String name = scenario.get("case").asText(), shape = scenario.get("shape").asText();
                int rowCount = scenario.get("source_rows").asInt();
                Path directory = root.resolve(name);
                Map<Long, JsonNode> source = new LinkedHashMap<>();
                long wholeLength = 0, selectedLength = 0, selectedCount = 0, selectedIdSum = 0;
                for (String line : Files.readAllLines(directory.resolve("expected.jsonl"))) {
                    JsonNode row = JSON.readTree(line); long id = row.get("id").longValue(); String text = row.get("doc").asText();
                    JsonNode value = JSON.readTree(text);
                    if (source.put(id, value) != null) throw new IllegalArgumentException("Duplicate source ID");
                    wholeLength += text.length();
                    if (matches(scenario, value)) { selectedCount++; selectedIdSum += id; selectedLength += text.length(); }
                }
                assertThat(source).hasSize(rowCount);
                assertThat(selectedCount).isEqualTo(scenario.get("matching_count").longValue());
                assertThat(selectedIdSum).isEqualTo(scenario.get("matching_id_sum").longValue());
                for (JsonNode file : scenario.get("files")) {
                    String layout = file.get("mode").asText();
                    boolean nativeStorage = file.path("storage").asText().equals("native");
                    var schema = SchemaParser.fromJson(Files.readString(file.has("iceberg_schema") ? root.resolve(file.get("iceberg_schema").asText()) : directory.resolve("iceberg-schema.json")));
                    String predicate = predicate(scenario, nativeStorage);
                    String filter = "SELECT count(*), coalesce(sum(id), 0) FROM docs WHERE " + predicate;
                    String whole = "SELECT sum(length(json_format(CAST(doc AS JSON)))) FROM docs";
                    String selectedWhole = "SELECT count(*), coalesce(sum(id), 0), coalesce(sum(length(json_format(CAST(doc AS JSON)))), 0) FROM docs WHERE " + predicate;
                    String projection;
                    Object projected;
                    if (scenario.has("predicate")) {
                        JsonNode condition = scenario.get("predicate");
                        String expression = pathExpression(condition, nativeStorage);
                        if (condition.get("kind").asText().equals("string")) {
                            projection = "SELECT sum(length(CAST(" + expression + " AS VARCHAR))) FROM docs";
                            projected = source.values().stream().map(v -> pathValue(v, condition)).filter(v -> !v.isNull()).mapToLong(v -> v.asText().length()).sum();
                        }
                        else {
                            projection = "SELECT sum(CAST(" + expression + " AS DOUBLE)) FROM docs";
                            projected = source.values().stream().map(v -> pathValue(v, condition)).filter(v -> !v.isNull()).mapToDouble(JsonNode::doubleValue).sum();
                        }
                    }
                    else {
                        projection = shape.equals("array")
                                ? "SELECT count_if(geomesa_variant_any_equals(doc, ARRAY[], ARRAY['key'], CAST('hit' AS VARIANT))) FROM docs"
                                : "SELECT sum(length(CAST(" + (shape.equals("nested") ? "doc['meta']['key']" : "doc['key']") + " AS VARCHAR))) FROM docs";
                        projected = shape.equals("array") ? (long) rowCount / 10 : rowCount * 4L - rowCount / 10;
                    }
                    List<Cell> cells = new ArrayList<>(List.of(
                            new Cell("whole", "off", false, false, whole, List.of(wholeLength)),
                            new Cell("projection", "off", true, true, projection, List.of(projected)),
                            new Cell("filter", "off", true, true, filter, List.of(selectedCount, selectedIdSum))));
                    if (!nativeStorage) cells.addAll(List.of(
                            new Cell("filter", "reference", true, true, filter, List.of(selectedCount, selectedIdSum)),
                            new Cell("filter", "typed", true, true, filter, List.of(selectedCount, selectedIdSum)),
                            new Cell("filtered-whole", "typed", true, false, selectedWhole, List.of(selectedCount, selectedIdSum, selectedLength)),
                            new Cell("filtered-whole", "typed", true, true, selectedWhole, List.of(selectedCount, selectedIdSum, selectedLength))));
                    Collections.rotate(cells, fork % cells.size());
                    byte[] bytes = Files.readAllBytes(root.resolve(file.get("file").asText()));
                    for (Cell cell : cells) {
                        Map<String, String> properties = Map.of("variant_filter_mode", cell.mode(),
                                "variant_projection_pushdown_enabled", Boolean.toString(cell.projection()),
                                "parquet_shredded_variant_selective_rebuild", Boolean.toString(cell.selective()));
                        try (PlanTester tester = VariantSqlSmoke.tester(properties, bytes, rowCount, SOURCES::add, schema)) {
                            if (cell.name().equals("whole")) checkSourceSpots(tester, source, rowCount);
                            assertThat(execute(tester, cell.sql())).as(name + "/" + layout + "/" + cell).isEqualTo(cell.expected());
                            for (int i = 0; i < warmup; i++) assertThat(execute(tester, cell.sql())).isEqualTo(cell.expected());
                            for (int iteration = 0; iteration < repeats; iteration++) {
                                long beforeAlloc = allocated(), beforeCpu = PROCESS.getProcessCpuTime(), beforeGcMs = gcMs(), beforeGcCount = gcCount();
                                long start = System.nanoTime();
                                List<Object> result = execute(tester, cell.sql());
                                long elapsed = System.nanoTime() - start, cpu = PROCESS.getProcessCpuTime() - beforeCpu, alloc = allocated() - beforeAlloc;
                                assertThat(result).isEqualTo(cell.expected());
                                csv.write(String.join(",", name, layout, cell.name(), cell.mode(), Boolean.toString(cell.projection()), Boolean.toString(cell.selective()),
                                        Integer.toString(iteration), Integer.toString(rowCount), Integer.toString(bytes.length), Long.toString(elapsed), Long.toString(cpu), Long.toString(alloc),
                                        Long.toString(gcMs() - beforeGcMs), Long.toString(gcCount() - beforeGcCount),
                                        Long.toString(SOURCES.stream().mapToLong(ConnectorPageSource::getCompletedBytes).sum()), Long.toString(SOURCES.stream().mapToLong(ConnectorPageSource::getReadTimeNanos).sum()),
                                        Long.toString(metric("Parquet VARIANT leaf decodes")), Long.toString(metric("Parquet VARIANT leaf decode nanos")), Long.toString(metric("Parquet VARIANT decision nanos")),
                                        Long.toString(metric("Parquet VARIANT reference reconstruction nanos")), Long.toString(metric("Parquet VARIANT output reconstruction nanos")),
                                        Long.toString(metric("Parquet VARIANT reference rows")), Long.toString(metric("Parquet VARIANT output rows")),
                                        Long.toString(metric("Variant typed rows")), Long.toString(metric("Variant fallback rows")), Long.toString(metric("Variant unknown rows"))) + "\n");
                                csv.flush();
                            }
                        }
                    }
                    System.out.println("ACTUAL_WRITER_SQL_CASE_COMPLETE " + name + "/" + layout);
                }
            }
        }
        System.out.println("ACTUAL_WRITER_SQL_MEASUREMENT_SUCCESS fork=" + fork);
    }

    private static void checkSourceSpots(PlanTester tester, Map<Long, JsonNode> source, int rows) {
        TreeSet<Long> ids = new TreeSet<>(List.of(0L, 1L, 31L, 32L, 1023L, 1024L, (long) rows - 1, rows * 9L / 10, rows * 99L / 100));
        Random random = new Random(20261112);
        for (int i = 0; i < 64; i++) ids.add((long) random.nextInt(rows));
        ids.removeIf(id -> id >= rows);
        SOURCES.clear();
        var result = tester.executePlan(session -> tester.createPlan(session, "SELECT id, CAST(doc AS JSON) FROM docs WHERE id IN (" + ids.stream().map(Object::toString).collect(Collectors.joining(",")) + ") ORDER BY id"), new PlanTester.MaterializedResultOutput());
        assertThat(result.getMaterializedRows()).hasSize(ids.size());
        for (var row : result.getMaterializedRows()) {
            try { assertThat(JSON.readTree(row.getField(1).toString())).isEqualTo(source.get(((Number) row.getField(0)).longValue())); }
            catch (java.io.IOException e) { throw new java.io.UncheckedIOException(e); }
        }
    }
    private static JsonNode pathValue(JsonNode value, JsonNode condition) {
        for (JsonNode part : condition.get("path")) value = value.path(part.asText());
        return value;
    }
    private static String quoted(String text) { return "'" + text.replace("'", "''") + "'"; }
    private static String pathExpression(JsonNode condition, boolean nativeStorage) {
        String expression = "doc";
        for (JsonNode part : condition.get("path")) expression += nativeStorage ? ".\"" + part.asText().replace("\"", "\"\"") + "\"" : "[" + quoted(part.asText()) + "]";
        return expression;
    }
    private static boolean matches(JsonNode scenario, JsonNode value) {
        String shape = scenario.get("shape").asText();
        if (scenario.has("predicate")) {
            JsonNode condition = scenario.get("predicate"), actual = pathValue(value, condition), expected = condition.get("value");
            return actual.isNumber() && expected.isNumber() ? actual.decimalValue().compareTo(expected.decimalValue()) == 0 : actual.equals(expected);
        }
        if (shape.startsWith("unstable")) return value.get("n").isNumber() && value.get("n").decimalValue().compareTo(java.math.BigDecimal.valueOf(7)) == 0;
        if (shape.equals("nested")) return value.get("meta").get("key").asText().equals("hit");
        if (shape.equals("array")) { for (JsonNode element : value) if (element.get("key").asText().equals("hit")) return true; return false; }
        return value.get("key").asText().equals("hit");
    }
    private static String predicate(JsonNode scenario, boolean nativeStorage) {
        String shape = scenario.get("shape").asText();
        if (scenario.has("predicate")) {
            JsonNode condition = scenario.get("predicate");
            String literal = condition.get("kind").asText().equals("string") ? quoted(condition.get("value").asText()) : condition.get("value").asText();
            if (nativeStorage) return pathExpression(condition, true) + " = " + literal;
            List<String> path = new ArrayList<>();
            condition.get("path").forEach(part -> path.add(quoted(part.asText())));
            return "geomesa_variant_equals(doc, ARRAY[" + String.join(",", path) + "], CAST(" + literal + " AS VARIANT))";
        }
        if (shape.startsWith("unstable")) return "geomesa_variant_equals(doc, ARRAY['n'], CAST(7 AS VARIANT))";
        if (shape.equals("array")) return "geomesa_variant_any_equals(doc, ARRAY[], ARRAY['key'], CAST('hit' AS VARIANT))";
        return "geomesa_variant_equals(doc, ARRAY[" + (shape.equals("nested") ? "'meta'," : "") + "'key'], CAST('hit' AS VARIANT))";
    }
    private static List<Object> execute(PlanTester tester, String sql) {
        SOURCES.clear();
        return tester.executePlan(session -> tester.createPlan(session, sql), new PlanTester.MaterializedResultOutput()).getMaterializedRows().getFirst().getFields();
    }
    private static long metric(String name) { return SOURCES.stream().map(source -> source.getMetrics().getMetrics().get(name)).filter(Objects::nonNull).mapToLong(value -> ((io.trino.plugin.base.metrics.LongCount) value).getTotal()).sum(); }
    private static long allocated() { return THREADS.getTotalThreadAllocatedBytes(); }
    private static long gcMs() { return ManagementFactory.getGarbageCollectorMXBeans().stream().mapToLong(bean -> Math.max(0, bean.getCollectionTime())).sum(); }
    private static long gcCount() { return ManagementFactory.getGarbageCollectorMXBeans().stream().mapToLong(bean -> Math.max(0, bean.getCollectionCount())).sum(); }
}

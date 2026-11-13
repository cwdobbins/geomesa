/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import io.trino.Session;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.variant.Variant;
import io.trino.testing.PlanTester;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageTypeParser;
import org.locationtech.geomesa.trino.variant.GeoMesaVariantFunctions;

import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

/** Actual planned SQL over deterministic Parquet, with correctness checked outside timing. */
public final class VariantSqlBenchmark {
    private static final int ROWS = Integer.getInteger("variant.benchmark.rows", 10_000);
    private static final int WARMUP = Integer.getInteger("variant.benchmark.warmup", 3);
    private static final int REPEATS = Integer.getInteger("variant.benchmark.repeats", 5);
    private record Scenario(int selectivity, int paths, String shape, String layout, boolean whole, boolean selective, boolean irregular, boolean composed) {}
    private static final List<ConnectorPageSource> SOURCES = new ArrayList<>();
    private VariantSqlBenchmark() {}

    public static void main(String[] args) throws Exception {
        List<Scenario> cases = new ArrayList<>();
        for (int selectivity : List.of(0, 1, 10, 50, 100)) cases.add(new Scenario(selectivity, 1, "flat", "full", false, true, false, false));
        for (int paths : List.of(4, 16)) for (String shape : List.of("flat", "nested", "list")) cases.add(new Scenario(10, paths, shape, "full", false, true, false, false));
        for (String layout : List.of("full", "partial", "unshredded")) for (boolean whole : List.of(false, true)) {
            cases.add(new Scenario(10, 4, "flat", layout, whole, true, false, false));
        }
        cases.add(new Scenario(10, 4, "flat", "full", true, false, false, false));
        cases.add(new Scenario(10, 4, "nested", "partial", true, true, true, false));
        for (String layout : List.of("full", "partial")) cases.add(new Scenario(10, 4, "flat", layout, true, true, false, true));
        for (int paths : List.of(1, 4, 16)) cases.add(new Scenario(100, paths, "projection", "full", false, true, false, false));
        cases = cases.stream().distinct().toList();
        try (var csv = Files.newBufferedWriter(Path.of(args[0]))) {
            csv.write("case,selectivity,paths,shape,layout,whole,selective,irregular,composed,mode,iteration,rows,file_bytes,wall_ns,allocated_bytes,gc_ms,gc_count,read_bytes,read_ns,leaf_decodes,leaf_decode_ns,decision_ns,reference_reconstruction_ns,output_reconstruction_ns,reference_rows,output_rows,typed_rows,fallback_rows,native_decided_rows,unknown_rows\n");
            for (int index = 0; index < cases.size(); index++) {
                Scenario scenario = cases.get(index);
                byte[] bytes = parquet(scenario);
                try (PlanTester baseline = tester(scenario, "off", bytes)) {
                    String exact = scenario.shape().equals("projection") ? "true" : predicate(scenario, false);
                    long expectedCount = IntStream.range(0, ROWS).filter(id -> matches(scenario, id)).count();
                    assertThat(execute(baseline, scenario, "off", "SELECT count(*) FROM docs WHERE " + exact).getOnlyValue()).isEqualTo(expectedCount);
                    String output = scenario.shape().equals("projection") ? "sum(" + IntStream.range(0, scenario.paths())
                        .mapToObj(i -> "CAST(doc['p" + i + "'] AS BIGINT)").collect(Collectors.joining(" + ")) + ")"
                        : scenario.whole() ? "coalesce(sum(length(json_format(CAST(doc AS JSON)))), 0)" : "count(*)";
                    var expected = execute(baseline, scenario, "off", "SELECT " + output + " FROM docs WHERE " + exact).getMaterializedRows();
                    List<String> modes = new ArrayList<>(scenario.shape().equals("projection") ? List.of("projection-off", "projection") : List.of("off", "reference", "typed"));
                    if (!scenario.shape().equals("list") && !scenario.shape().equals("projection") && !scenario.irregular()) modes.addAll(List.of("native-off", "native"));
                    // Rotate order between cases; separate JVM forks are run by the driver.
                    Collections.rotate(modes, index % modes.size());
                    for (String mode : modes) {
                      try (PlanTester tester = tester(scenario, mode, bytes)) {
                        String sql = "SELECT " + output + " FROM docs WHERE " + (scenario.shape().equals("projection") ? "true" : predicate(scenario, mode.startsWith("native")));
                        assertThat(execute(tester, scenario, mode, sql).getMaterializedRows()).as(scenario + " " + mode).isEqualTo(expected);
                        if (mode.equals("typed") && scenario.layout().equals("full")) {
                            assertThat(metric("Variant typed rows")).isEqualTo(IntStream.range(0, ROWS).filter(id -> admitted(scenario, id)).count());
                            assertThat(metric("Variant fallback rows")).isZero();
                            if (!scenario.whole()) assertThat(metric("Parquet VARIANT output rows")).isZero();
                        }
                        if (mode.equals("native") && scenario.layout().equals("full")) {
                            assertThat(metric("Variant native decided rows")).isEqualTo(IntStream.range(0, ROWS).filter(id -> admitted(scenario, id)).count());
                        }
                        for (int warmup = 0; warmup < WARMUP; warmup++) execute(tester, scenario, mode, sql);
                        for (int iteration = 0; iteration < REPEATS; iteration++) {
                            long allocated = allocated(), gcTime = gcTime(), gcCount = gcCount(), start = System.nanoTime();
                            execute(tester, scenario, mode, sql);
                            long elapsed = System.nanoTime() - start;
                            csv.write(String.join(",", Integer.toString(index), Integer.toString(scenario.selectivity()), Integer.toString(scenario.paths()),
                                scenario.shape(), scenario.layout(), Boolean.toString(scenario.whole()), Boolean.toString(scenario.selective()), Boolean.toString(scenario.irregular()), Boolean.toString(scenario.composed()),
                                mode, Integer.toString(iteration), Integer.toString(ROWS), Integer.toString(bytes.length), Long.toString(elapsed), Long.toString(allocated() - allocated),
                                Long.toString(gcTime() - gcTime), Long.toString(gcCount() - gcCount), Long.toString(SOURCES.stream().mapToLong(ConnectorPageSource::getCompletedBytes).sum()),
                                Long.toString(SOURCES.stream().mapToLong(ConnectorPageSource::getReadTimeNanos).sum()), Long.toString(metric("Parquet VARIANT leaf decodes")),
                                Long.toString(metric("Parquet VARIANT leaf decode nanos")), Long.toString(metric("Parquet VARIANT decision nanos")),
                                Long.toString(metric("Parquet VARIANT reference reconstruction nanos")), Long.toString(metric("Parquet VARIANT output reconstruction nanos")),
                                Long.toString(metric("Parquet VARIANT reference rows")), Long.toString(metric("Parquet VARIANT output rows")),
                                Long.toString(metric("Variant typed rows")), Long.toString(metric("Variant fallback rows")), Long.toString(metric("Variant native decided rows")), Long.toString(metric("Variant unknown rows"))) + "\n");
                            csv.flush();
                        }
                      }
                    }
                }
                System.out.println("BENCHMARK completed " + index + " " + scenario);
            }
        }
    }

    private static PlanTester tester(Scenario scenario, String mode, byte[] bytes) {
        return VariantSqlSmoke.tester(Map.of("variant_filter_mode", mode.equals("native") ? "typed" : (mode.equals("native-off") || mode.startsWith("projection")) ? "off" : mode,
                "variant_native_prefilter_enabled", Boolean.toString(mode.equals("native")), "variant_projection_pushdown_enabled", Boolean.toString(!mode.equals("projection-off")),
                "parquet_shredded_variant_selective_rebuild", Boolean.toString(scenario.selective())), bytes, ROWS, SOURCES::add, scenario.composed());
    }
    private static io.trino.testing.MaterializedResult execute(PlanTester tester, Scenario scenario, String mode, String sql) {
        SOURCES.clear();
        return tester.executePlan(session -> tester.createPlan(session, sql), new PlanTester.MaterializedResultOutput());
    }
    private static String predicate(Scenario s, boolean nativeHint) {
        return IntStream.range(0, s.paths()).mapToObj(i -> {
            String key = "p" + i;
            if (s.shape().equals("list")) return "geomesa_variant_any_equals(doc, ARRAY['items'], ARRAY['" + key + "'], CAST(7 AS VARIANT))";
            if (nativeHint) return "doc" + (s.shape().equals("nested") ? "['n']" : "") + "['" + key + "'] = CAST(7 AS VARIANT)";
            return "geomesa_variant_equals(doc, ARRAY[" + (s.shape().equals("nested") ? "'n'," : "") + "'" + key + "'], CAST(7 AS VARIANT))";
        }).collect(Collectors.joining(" AND "));
    }
    private static boolean matches(Scenario s, int id) { return admitted(s, id) && id % 100 < s.selectivity() && !(s.irregular() && (id % 17 == 0 || id % 19 == 0)); }
    private static boolean admitted(Scenario s, int id) { return !s.composed() || (id % 3 != 0 && id % 16 < 8); }
    private static long metric(String name) {
        return SOURCES.stream().map(source -> source.getMetrics().getMetrics().get(name)).filter(Objects::nonNull)
            .mapToLong(value -> ((io.trino.plugin.base.metrics.LongCount) value).getTotal()).sum();
    }
    private static long allocated() {
        var bean = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        return bean.getTotalThreadAllocatedBytes();
    }
    private static long gcTime() { return ManagementFactory.getGarbageCollectorMXBeans().stream().mapToLong(bean -> Math.max(0, bean.getCollectionTime())).sum(); }
    private static long gcCount() { return ManagementFactory.getGarbageCollectorMXBeans().stream().mapToLong(bean -> Math.max(0, bean.getCollectionCount())).sum(); }

    private static byte[] parquet(Scenario s) throws Exception {
        String fields = IntStream.range(0, 16).mapToObj(i -> "required group p" + i + " { optional binary value; optional int64 typed_value; }").collect(Collectors.joining());
        String shape = switch (s.shape()) {
            case "nested" -> "required group n { optional binary value; optional group typed_value { " + fields + " } }";
            case "list" -> "required group items { optional binary value; optional group typed_value (LIST) { repeated group list { required group element { optional binary value; optional group typed_value { " + fields + " } } } } }";
            default -> fields;
        };
        String typed = s.layout().equals("unshredded") ? "" : "optional group typed_value { " + shape + " required group padding { optional binary value; optional binary typed_value (STRING); } }";
        var schema = MessageTypeParser.parseMessageType("message table { optional group doc (VARIANT(1)) = 1 { required binary metadata; optional binary value; " + typed + " } required int64 id = 2; optional binary __vis__ (STRING) = 3; optional binary geom = 4; optional float x = 5; optional float y = 6; }");
        Path directory = Files.createTempDirectory("variant-benchmark-");
        Path file = directory.resolve("docs.parquet");
        try {
            try (var writer = ExampleParquetWriter.builder(new LocalOutputFile(file)).withType(schema).withRowGroupSize(256 * 1024).withPageSize(8192)
                    .withCompressionCodec(org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY).build()) {
                for (int id = 0; id < ROWS; id++) {
                    Group row = new SimpleGroup(schema).append("id", (long) id);
                    if (s.composed()) row.append("__vis__", id % 3 == 0 ? "B" : "A").append("x", (float) (id % 16)).append("y", 0.0f)
                        .append("geom", Binary.fromConstantByteArray(new org.locationtech.jts.io.WKBWriter().write(
                            new org.locationtech.jts.geom.GeometryFactory().createPoint(new org.locationtech.jts.geom.Coordinate(id % 16, 0)))));
                    if (!(s.irregular() && id % 17 == 0)) {
                        long value = id % 100 < s.selectivity() ? 7L : 8L;
                        String padding = "payload-" + id + "-" + "x".repeat(256);
                        boolean absent = s.irregular() && id % 19 == 0;
                        Map<io.airlift.slice.Slice, Variant> values = new LinkedHashMap<>();
                        if (!absent) for (int i = 0; i < 16; i++) values.put(utf8Slice("p" + i), Variant.ofLong(value));
                        Map<io.airlift.slice.Slice, Variant> document = new LinkedHashMap<>();
                        switch (s.shape()) {
                            case "nested" -> document.put(utf8Slice("n"), Variant.ofObject(values));
                            case "list" -> document.put(utf8Slice("items"), Variant.ofArray(List.of(Variant.ofObject(values), Variant.ofObject(values))));
                            default -> document.putAll(values);
                        }
                        document.put(utf8Slice("padding"), Variant.ofString(utf8Slice(padding)));
                        Variant variant = Variant.ofObject(document);
                        boolean shredded = !s.layout().equals("unshredded") && !(s.layout().equals("partial") && id % 4 == 0) && !absent;
                        Group doc = row.addGroup("doc").append("metadata", Binary.fromConstantByteArray((shredded ? Variant.NULL_VALUE : variant).metadata().toSlice().getBytes()));
                        if (!shredded) doc.append("value", Binary.fromConstantByteArray(variant.data().getBytes()));
                        else {
                            Group object = doc.addGroup("typed_value");
                            object.addGroup("padding").append("typed_value", padding);
                            if (s.shape().equals("list")) {
                                Group array = object.addGroup("items").addGroup("typed_value");
                                for (int element = 0; element < 2; element++) writeFields(array.addGroup("list").addGroup("element").addGroup("typed_value"), value);
                            } else writeFields(s.shape().equals("nested") ? object.addGroup("n").addGroup("typed_value") : object, value);
                        }
                    }
                    writer.write(row);
                }
            }
            return Files.readAllBytes(file);
        } finally { Files.deleteIfExists(file); Files.delete(directory); }
    }
    private static void writeFields(Group object, long value) { for (int i = 0; i < 16; i++) object.addGroup("p" + i).append("typed_value", value); }
}

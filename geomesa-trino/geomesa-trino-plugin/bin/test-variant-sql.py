#!/usr/bin/env python3
# Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
# Licensed under the Apache License, Version 2.0 (https://www.apache.org/licenses/LICENSE-2.0).

"""Run the VARIANT engine and Parquet smoke tests using a built Trino checkout.

The plugin's ordinary Maven tests deliberately exclude the engine dependency graph.
This runner puts Trino's own test dependencies first, preserving its runtime versions.
It does not start a server or validate the stock-server plugin classloader boundary.
"""
import argparse
import pathlib
import subprocess
import sys


def run(command, cwd, log):
    with log.open("w") as output:
        result = subprocess.run(command, cwd=cwd, stdout=output, stderr=subprocess.STDOUT)
    if result.returncode:
        print(f"Failed; see {log}", file=sys.stderr)
        print(log.read_text()[-12000:], file=sys.stderr)
        sys.exit(result.returncode)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trino-checkout", type=pathlib.Path, required=True)
    parser.add_argument("--maven-repo", type=pathlib.Path, help="Maven repository containing the matching stock-spi build")
    parser.add_argument("--benchmark", action="store_true", help="Run the SQL measurement matrix after the smoke tests")
    parser.add_argument("--forks", type=int, default=2)
    parser.add_argument("--rows", type=int, default=10000)
    parser.add_argument("--warmup", type=int, default=3)
    parser.add_argument("--repeats", type=int, default=5)
    args = parser.parse_args()
    repository = [] if args.maven_repo is None else [f"-Dmaven.repo.local={args.maven_repo.resolve()}"]
    trino = args.trino_checkout.resolve()
    plugin = pathlib.Path(__file__).resolve().parent.parent
    output = plugin / "target/variant-smoke"
    output.mkdir(parents=True, exist_ok=True)
    if not (plugin / "target/test-classes/org/locationtech/geomesa/trino/spatial/iceberg/connector/VariantTestFixtures.class").is_file():
        parser.error("Compile/run the plugin unit tests first (mvn -pl geomesa-trino/geomesa-trino-plugin test).")
    if not (trino / "plugin/trino-iceberg/target/classes/io/trino/plugin/iceberg/IcebergColumnHandle.class").is_file():
        parser.error("Compile the Trino Iceberg module first.")
    engine_cp = output / "engine-classpath.txt"
    plugin_cp = output / "plugin-classpath.txt"
    run([str(trino / "mvnw"), "-o", *repository, "-pl", "plugin/trino-iceberg", "dependency:build-classpath",
         "-Dmdep.includeScope=test", f"-Dmdep.outputFile={engine_cp}"], trino, output / "engine-classpath.log")
    run(["mvn", "-o", *repository, "dependency:build-classpath", "-Dmdep.includeScope=test",
         f"-Dmdep.outputFile={plugin_cp}"], plugin, output / "plugin-classpath.log")
    classpath = ":".join([str(plugin / "target/classes"), str(plugin / "target/test-classes"),
                          str(trino / "plugin/trino-iceberg/target/classes"),
                          engine_cp.read_text().strip(), plugin_cp.read_text().strip()])
    classes = output / "classes"
    classes.mkdir(exist_ok=True)
    sources = sorted(str(p) for p in (plugin / "src/test/sql").rglob("*.java"))
    run(["javac", "-cp", classpath, "-d", str(classes), *sources], plugin, output / "compile.log")
    for test in ("VariantSqlSmoke", "VariantParquetSmoke"):
        log = output / f"{test}.log"
        run(["java", "-Xmx2g", "--add-modules=jdk.incubator.vector", "-cp", f"{classes}:{classpath}",
             f"org.locationtech.geomesa.trino.spatial.iceberg.connector.{test}"], plugin, log)
        print(f"{test}: passed ({log})", flush=True)
    if args.benchmark:
        for fork in range(args.forks):
            log = output / f"benchmark-{fork}.log"
            csv = output / f"benchmark-{fork}.csv"
            run(["java", "-Xms2g", "-Xmx2g", "--add-modules=jdk.incubator.vector",
                 f"-Dvariant.benchmark.rows={args.rows}", f"-Dvariant.benchmark.warmup={args.warmup}",
                 f"-Dvariant.benchmark.repeats={args.repeats}", "-cp", f"{classes}:{classpath}",
                 "org.locationtech.geomesa.trino.spatial.iceberg.connector.VariantSqlBenchmark", str(csv)], plugin, log)
            print(f"Benchmark fork {fork}: passed ({csv})", flush=True)


if __name__ == "__main__":
    main()

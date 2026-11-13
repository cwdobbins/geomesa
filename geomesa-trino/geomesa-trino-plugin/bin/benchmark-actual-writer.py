#!/usr/bin/env python3
"""Freeze the embedded-engine runtime, then measure planned SQL on actual shared-writer files."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shlex
import shutil
import subprocess
import time
import xml.etree.ElementTree as ET


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--trino-checkout', type=Path, required=True)
    parser.add_argument('--writer-results', type=Path, required=True)
    parser.add_argument('--fork-repo', type=Path, required=True)
    parser.add_argument('--stock-repo', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--trino-maven')
    parser.add_argument('--maven', default='mvn')
    parser.add_argument('--trino-maven-arg', action='append', default=[])
    parser.add_argument('--maven-arg', action='append', default=[])
    parser.add_argument('--java', default=str(Path(os.environ['JAVA_HOME']) / 'bin/java') if os.environ.get('JAVA_HOME') else 'java')
    parser.add_argument('--javac', default=str(Path(os.environ['JAVA_HOME']) / 'bin/javac') if os.environ.get('JAVA_HOME') else 'javac')
    parser.add_argument('--java-arg', action='append', default=[])
    parser.add_argument('--warmup', type=int, default=2)
    parser.add_argument('--repeats', type=int, default=5)
    parser.add_argument('--forks', type=int, default=3)
    args = parser.parse_args()
    if args.forks < 1 or args.warmup < 0 or args.repeats < 1:
        parser.error('Invalid repetition counts')
    output = args.output.resolve(); output.mkdir(parents=True, exist_ok=False)
    trino = args.trino_checkout.resolve(); plugin = Path(__file__).resolve().parents[1]
    commands = []

    def run(command, cwd, name):
        rendered = shlex.join(command)
        print(rendered if len(rendered) < 400 else f'{command[0]} [full arguments in commands.json] -> {name}', flush=True)
        with (output / name).open('w') as log:
            result = subprocess.run(command, cwd=cwd, stdout=log, stderr=subprocess.STDOUT)
        commands.append({'argv': command, 'cwd': str(cwd), 'log': name, 'exit': result.returncode})
        (output / 'commands.json').write_text(json.dumps(commands, indent=2) + '\n')
        if result.returncode:
            raise SystemExit(f'Failed; inspect {output / name}')

    engine_cp = output / 'engine-classpath.txt'; plugin_cp = output / 'plugin-classpath.txt'
    run([args.trino_maven or str(trino / 'mvnw'), '-B', *args.trino_maven_arg,
         f'-Dmaven.repo.local={args.fork_repo.resolve()}', '-pl', 'plugin/trino-iceberg', 'dependency:build-classpath',
         '-Dmdep.includeScope=test', f'-Dmdep.outputFile={engine_cp}'], trino, 'engine-classpath.log')
    run([args.maven, '-B', *args.maven_arg, f'-Dmaven.repo.local={args.stock_repo.resolve()}',
         'dependency:build-classpath', '-Dmdep.includeScope=test', f'-Dmdep.outputFile={plugin_cp}'], plugin, 'plugin-classpath.log')
    root_pom = ET.parse(trino / 'pom.xml').getroot(); ns = {'m': 'http://maven.apache.org/POM/4.0.0'}
    version = root_pom.findtext('m:version', namespaces=ns)
    iceberg = args.fork_repo.resolve() / 'io/trino/trino-iceberg' / version / f'trino-iceberg-{version}.jar'
    entries = [str(plugin / 'target/classes'), str(plugin / 'target/test-classes'), str(iceberg)]
    entries.extend(engine_cp.read_text().strip().split(os.pathsep)); entries.extend(plugin_cp.read_text().strip().split(os.pathsep))
    runtime = output / 'runtime'; runtime.mkdir()
    frozen_entries = []; runtime_hashes = {}; seen = {}
    for index, value in enumerate(entries):
        source = Path(value)
        if source.is_dir():
            destination = runtime / f'{index:04d}-{source.name}'
            before = {str(p.relative_to(source)): sha(p) for p in sorted(source.rglob('*')) if p.is_file()}
            shutil.copytree(source, destination)
            after = {str(p.relative_to(source)): sha(p) for p in sorted(source.rglob('*')) if p.is_file()}
            copied = {str(p.relative_to(destination)): sha(p) for p in sorted(destination.rglob('*')) if p.is_file()}
            assert before == copied == after, f'Classes changed during freezing: {source}'
            runtime_hashes[str(destination.relative_to(output))] = before
        else:
            digest = sha(source)
            if digest in seen:
                continue
            destination = runtime / f'{index:04d}-{source.name}'
            shutil.copy2(source, destination)
            assert digest == sha(destination) == sha(source), f'JAR changed during freezing: {source}'
            seen[digest] = destination
            runtime_hashes[str(destination.relative_to(output))] = {'sha256': digest, 'original': str(source)}
        frozen_entries.append(str(destination))
    cp = os.pathsep.join(frozen_entries)
    sources = sorted((plugin / 'src/test/sql').rglob('*.java'))
    frozen_sources = output / 'sources'; frozen_sources.mkdir()
    source_hashes = {}
    for source in sources:
        destination = frozen_sources / source.relative_to(plugin / 'src/test/sql'); destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination); source_hashes[str(source.relative_to(plugin))] = sha(destination)
    classes = output / 'classes'; classes.mkdir()
    run([args.javac, '--add-modules', 'jdk.incubator.vector', '-cp', cp, '-d', str(classes),
         *[str(path) for path in sorted(frozen_sources.rglob('*.java'))]], plugin, 'compile.log')
    java = [args.java, *args.java_arg, '--add-modules=jdk.incubator.vector', '--enable-native-access=ALL-UNNAMED', '-cp', str(classes) + os.pathsep + cp]
    (output / 'frozen-classpath.txt').write_text(str(classes) + os.pathsep + cp + '\n')
    compiled_hashes = {str(p.relative_to(output)): sha(p) for p in sorted(classes.rglob('*')) if p.is_file()}
    provenance = {'sources_sha256': source_hashes, 'runtime': runtime_hashes,
        'compiled_sha256': compiled_hashes, 'parameters': vars(args),
        'scope': 'Actual writer files, embedded planner and reader over memory filesystem. File loading/setup/source checks excluded; timed SQL includes planning/execution/materialization. Native ROW projection uses lightweight metadata without ordinary Iceberg projection pushdown; use distributed native measurements for replacement comparisons. Not cold storage or distributed throughput.'}
    (output / 'provenance-before.json').write_text(json.dumps(provenance, default=str, indent=2) + '\n')
    run([args.java, '-version'], plugin, 'java-version.log')
    # Exercise the existing helper's default schema path after adding real-schema support.
    run(java + ['org.locationtech.geomesa.trino.spatial.iceberg.connector.VariantSqlSmoke'], plugin, 'smoke.log')
    fixtures = {}
    pressure = []
    def machine():
        result = {'time_ns': time.time_ns(), 'system': platform.system(), 'release': platform.release(),
                  'machine': platform.machine(), 'cpu_count': os.cpu_count()}
        if hasattr(os, 'getloadavg'):
            result['load_average'] = os.getloadavg()
        for name in ('/proc/stat', '/proc/pressure/cpu', '/proc/pressure/memory'):
            path = Path(name)
            if path.is_file(): result[name] = path.read_text()
        return result
    for fork in range(args.forks):
        fixture = args.writer_results.resolve() / f'fork-{fork}'
        assert (fixture / 'manifest.json').is_file(), f'Missing writer fork {fixture}'
        before = {str(p.relative_to(fixture)): sha(p) for p in sorted(fixture.rglob('*')) if p.is_file()}
        fixtures[f'fork-{fork}'] = before
        pressure.append({'fork': fork, 'when': 'before', 'machine': machine()})
        run(java + ['org.locationtech.geomesa.trino.spatial.iceberg.connector.ActualWriterSqlMeasurement',
                    str(fixture), str(output / f'reader-{fork}.csv'), str(args.warmup), str(args.repeats), str(fork)], plugin, f'reader-{fork}.log')
        pressure.append({'fork': fork, 'when': 'after', 'machine': machine()})
        assert before == {str(p.relative_to(fixture)): sha(p) for p in sorted(fixture.rglob('*')) if p.is_file()}, 'Fixtures changed during measurement'
        (output / 'machine-samples.json').write_text(json.dumps(pressure, indent=2) + '\n')
    assert compiled_hashes == {str(p.relative_to(output)): sha(p) for p in sorted(classes.rglob('*')) if p.is_file()}, 'Compiled benchmark changed'
    for relative, expected in runtime_hashes.items():
        path = output / relative
        actual = {str(p.relative_to(path)): sha(p) for p in sorted(path.rglob('*')) if p.is_file()} if path.is_dir() else {'sha256': sha(path), 'original': expected['original']}
        assert actual == expected, f'Frozen runtime changed: {relative}'
    provenance['fixtures_sha256'] = fixtures
    (output / 'provenance.json').write_text(json.dumps(provenance, default=str, indent=2) + '\n')
    print(f'Actual-writer SQL measurements retained in {output}', flush=True)


if __name__ == '__main__':
    main()

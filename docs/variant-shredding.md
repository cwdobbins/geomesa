# VARIANT writing, query execution and pruning

This branch pairs GeoMesa `6.0.0-SNAPSHOT` / Scala 2.12 with the Trino and exporter
`variant-filter-pushdown` branches. GeoMesa and exporter keep their current source
branch bases; Trino retains the release-484 compatibility base.

## Implementation status — 2026-11-13

The bounded implementation is complete on `variant-filter-pushdown` across Trino,
GeoMesa and `geomesa-parquet-exporter`. Engineering acceptance and the bounded
source-data pilot are complete; production enablement remains workload-specific.

| Area | Implemented scope |
| --- | --- |
| Reader and execution | Lossless whole/pruned reads, typed probes, reference fallback, projection, exact function enforcement and conservative native hints |
| Datastore candidates | Positive case-sensitive string comparisons and scalar numeric ranges; original GeoTools residual, counts and paging semantics retained |
| Shared writer/exporter | Explicit layouts, bounded per-file sampling, exact physical type matching, lossless fallback, publication and bounded recovery |
| Row-group pruning | Scalar strings, safe integer/decimal ranges, and string equality through one array step |
| Iceberg file pruning | Exact string equality on nonempty ASCII object-key paths, after manifest planning and before worker splits |
| Compatibility | Stock Trino 484, mixed layouts, missing/null values, visibility, schema evolution, position/equality deletes and metadata-query safeguards |

Sampling, client candidates, filter mode, projection and native hints remain opt-in.
The established Iceberg selective-rebuild default is independently true; standalone
Parquet defaults to false. Existing `json-schema` attributes remain native nested
Parquet. Sampling does not migrate their schemas or existing data.

## Specification and configuration

The maintained user contract is in [query configuration](user/trino/query_config.rst),
[connector configuration](user/trino/connector_config.rst),
[runtime configuration](user/trino/runtime_config.rst) and
[installation](user/trino/install.rst). SQL functions work with connector filtering
off; client candidate generation and connector execution are independent opt-ins.

`geomesa_variant_get` and exact equality/membership functions retain complete
reference semantics. Scalar and one-array-step candidates retain missing/null,
wrong-shaped, coercible or unsupported observations. Object-field wildcard
functions run in the engine over complete input. Candidate generation does not
extract predicates from OR/NOT; counts, paging and residual source columns preserve
GeoTools semantics. Exact connector claims are exact for the function, not for the
GeoTools predicate that generated it. Native hints retain the engine residual.

The shared writer accepts explicit per-attribute layouts through
`geomesa.parquet.variant.shredding`. Exact type matching and fallback preserve
unmatched values; root/object annotations and null counts must remain valid.
The exporter supplies bounded per-file layout sampling and explicit recovery of
proved committed attempts. Uncertain publication is never permission to append
again. Source visibility, retention and checkpoint contracts remain authoritative.

## Numeric and row-group safety

Scalar `may_gt/gte/lt/lte` functions are necessary candidates, not exact GeoTools
predicates. Safe integer comparisons respect the ±2^53 coercion boundary. Finite
decimal/float/double range evaluation retains ambiguous rounding, non-finite,
missing/null, wrong-shaped and unsupported values for the original residual.
Integer/decimal group bounds require supported encodings, valid statistics,
complete fallback paths and the required absence of typed nulls. FLOAT/DOUBLE
min/max do not prove NaN absence, so those groups are retained. Ignoring Parquet
statistics disables these row-group decisions.

String array pruning supports one array step with object keys on either side.
Repeated fallback counts must match typed entry counts, not source row counts.
Exact membership can exclude nonmatching nulls; conservative candidates must keep
empty/missing/uncertain selections. Independent predicates never establish
same-element correlation. Unknown metadata retains data.

## Iceberg file-bound contract

Only typed-mode exact string equality on nonempty ASCII shorthand object paths
can reject files. Conservative candidates, numeric/array file predicates, unusual
literal keys and root equality retain reader evaluation. Stable field IDs bind
against the selected scan schema, including renames and historical snapshots.

`IcebergTableScanFilter` is coordinator-local and never serialized. Required
column statistics are requested independently of output projection and dynamic
filters. Inclusive evaluation runs after ordinary manifest planning, before split
creation, preserving ordinary predicates, visibility inputs and delete handling.
Workers still enforce the original function on retained files. Missing/incompatible
bounds retain files; corrupt metadata is not silently converted to missing data.

Iceberg 1.12's scan-expression logging rejects extract terms, so the implementation
does not attach them to `TableScan.filter`. Little-endian duplicate buffer views
satisfy VARIANT bound decoding without mutating shared bytes, order or positions.
Root counts are not used as extracted-path completeness evidence. Metrics
`fileFilterSkippedFiles` and `fileFilterSkippedBytes` describe this extra stage;
`dataFiles` retains its ordinary pre-filter planning meaning. This is file
exclusion, not manifest pruning. `parquet_ignore_statistics` controls row-group
statistics; reference/off modes disable the file hook.

## Build and verification

Run Maven/plugin builds with JDK 25 and supported toolchains; non-plugin GeoMesa
modules retain their configured Java 17 release target. First build/install the
paired Trino `484-shredded-3` artifacts with `stock-spi` into a dedicated cache.
Then build/install the plugin into that same cache and run datastore tests against
its newly packaged ZIP on stock Trino 484. Use configured repository credentials,
cache paths and Docker settings rather than workstation defaults.

```sh
mvn -B -t "$VARIANT_TOOLCHAINS" -Dmaven.repo.local="$VARIANT_STOCK_REPO" \
  -pl geomesa-trino/geomesa-trino-plugin install
mvn -B -t "$VARIANT_TOOLCHAINS" -Dmaven.repo.local="$VARIANT_STOCK_REPO" \
  -pl geomesa-trino/geomesa-trino-datastore test -Dtest.trino.docker.tag=484
```

Set these variables explicitly for the environment. Populate dependencies before
using offline Maven. Verify stock marker `484`, paired reader/toolkit jars and no
bundled SPI/engine in the ZIP. Embedded SQL checks are documented in the
[smoke-test README](../geomesa-trino/geomesa-trino-plugin/src/test/sql/README.md).
The explicit `VariantQueryMeasurementIT` accepts shape selection, recorded
constant-predicate controls and exact one-array-step predicates; see the exporter's
source-pilot report for inputs, parameters and timing boundaries.

## Validation and performance

The latest full plugin gate passed **321 tests**. The full datastore gate passed
**295 functional tests**, including 16 shared-writer cases, with one Docker
worker-startup port collision and a successful retry. Its XML includes an extra
initialization retry event. Selected Trino checks passed 39 Iceberg tests and six
toolkit compatibility tests; final stock-spi CI/install, ZIP verification and
packaging passed. Historical broader gates are retained as dated evidence, not
claimed as rerun by documentation or history consolidation.

The source-data pilot passed 180 measured writes in six fresh JVMs and 1,260
source-checked queries in four fresh stock clusters: 900 measured queries, 180
asserted plans and 20,153,420 checked returned rows. Both explicit measurement
invocations passed. The unchanged, hash-verified writer runtime supplied fresh
measurements after an unrelated default-cache snapshot mismatch prevented a new
writer build. No failed-build measurements enter the report.

Retain native storage for the tested workloads. Collection-info's modest filter
gain comes with 1.59× file size, 2.80× write time and slower whole reads.
Identity-data's small aggregate filter gain changes direction between clusters;
files grow 27% and whole reads slow 54%. Position-error remains a native control.
The AMR sample has no populated itineraries; every itinerary in the smaller
1,667,197-row summary snapshot is null. Synthetic sorted fixtures establish
pruning mechanisms, not production speedup predictions. Samples are stratified,
attribute-level local copies, not population estimates or cold-S3 benchmarks.

The exporter maintains the authoritative `docs/variant-shredding-assessment.md`
and dated `docs/variant-measurement-results` evidence. Source/artifact hashes and
older commit IDs remain historical provenance after the squash. No measurements
are relabeled to the new commit identity.

## Remaining plan

1. Obtain a populated itinerary source or concrete application query frequencies;
   pursue only a justified pilot with realistic full-record files, original
   locality and cold remote storage. Check write/storage/whole-read costs alongside
   selective reads. Production migration, visibility/recovery acceptance and
   rollout remain separate from the isolated benchmark.
2. Extend conservative candidate file pruning only with trusted path completeness
   and compatible type/coercion guarantees. Existing bounds alone are insufficient.
3. Treat numeric/array file bounds, manifest pruning, FLOAT/DOUBLE group skipping
   with proven NaN absence, multiple array steps and same-element multi-key
   correlation as separate implementations with new correctness evidence.
4. Review and publish the paired artifacts only when explicitly requested. Install
   matching readers on every node before opt-in generation; retain compatible
   readers for existing shredded files when disabling future writer sampling.

The implemented numeric → array → object-file pruning sequence is complete for
its documented scope. No production deployment or default enablement has occurred.

## Documentation and history

This tracked guide is available in a clean clone. `docs/variant`, `docs/superpowers`
and the local AGENTS file retain their existing ignore state. Local plans/specs
are refreshed when present; old evidence and source hashes remain dated history.
Consolidation keeps one feature commit above the existing source base without
squashing upstream work or relabeling old validation artifacts.

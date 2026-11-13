# VARIANT engine and Parquet smoke tests

For the current paired-repository setup, build order and full validation boundary,
start with [the current specification and plan](../../../../../docs/variant-shredding.md). Phases 1–4 and
the bounded numeric, array and object-file pruning sequence are implemented.

These tests run the real SQL planner/evaluator and Iceberg Parquet page source without
Docker. They use the GeoMesa functions and scan wrappers from `target/classes`.
They do **not** test the stock server's isolated plugin classloader, distributed worker
transport, ORC/Avro, or final packaged plugin deployment.

Compile/test the GeoMesa plugin with Java 25 and the repository's Maven toolchain configuration:

```sh
mvn -pl geomesa-trino/geomesa-trino-plugin test
```

Build the matching Trino checkout's Iceberg module and its local dependencies, then run
from the GeoMesa repository:

```sh
python3 geomesa-trino/geomesa-trino-plugin/bin/test-variant-sql.py \
  --trino-checkout /path/to/trino --maven-repo /path/to/stock-maven-cache
```

The runner resolves classpaths offline; populate Maven's dependency cache first if needed.
It gives Trino's own engine dependency versions priority, avoiding conflicts with
GeoMesa's application dependencies. Sources, classes and logs are separated under
`src/test/sql` and `target/variant-smoke`. The ordinary Maven unit tests keep their
small engine dependency footprint.

`VariantSqlSmoke` checks SQL NULL invocation, function semantics, off/reference result
and plan equivalence, hidden TRY/CASE/IF residuals, count(*), whole outputs, independent
array-element scopes, and typed Constant/handle JSON round trips using engine codecs.
Its scan reads a real shredded Parquet fixture through `CustomFooterProviderFactory`.
Projection coverage includes 22 off/on result comparisons (filter off/reference), native
subscript/cast and own-function plans that actually carry path-bearing handles, whole
residual absorption, and matching native shape errors. An ordinary residual filter can
prevent the engine from offering projections directly on the scan; pruning is not
expected for every query shape.

Metadata optimization coverage enables `optimize_metadata_queries` and compares DISTINCT,
min, max and count(DISTINCT) with reference filtering off/on. The scan delegate exposes
discrete partition values, reproducing the wrong-answer path if a filtered handle permits
the optimizer to replace its scan with those values.

`ShreddedVariantEndToEndTest` in the datastore module adds the stock-server counterpart:
eight SQL shapes, four filter/projection combinations, and an all-unshredded reference.
It also writes eight identity partitions of shredded Parquet and checks the four metadata
query forms with both matching and nonmatching predicates (eight additional comparisons).
Compilation alone does not validate that test; execute it with Docker and the freshly
packaged plugin on stock Trino 484 before claiming a fresh stock-server validation.

`VariantParquetSmoke` writes actual shredded and unshredded Parquet with typed and
fallback values and reads it through the custom provider factory. Its 384 combinations
cover position deletes, visibility filtering, footer configuration, predicate-only
scans and ordinary/whole/pruned outputs. It checks actual missing sibling fields in
pruned typed rows and their presence in whole output. It is a correctness test, not an
end-to-end performance benchmark.

The current full plugin/datastore gates are 321 and 295 functional tests, with one
Docker startup retry recorded in the datastore evidence. The shared-writer test
now has 16 cases spanning numeric, fractional, array and file bounds in addition
to the original projection/metadata comparisons described above. These are dated
results, not a replacement for executing affected tests after code changes.

`VariantQueryMeasurementIT` is an explicit distributed measurement harness. It
supports `geomesa.variant.measurement-shapes`, an explicit
`geomesa.variant.measurement-allow-degenerate-predicates` control for constant-field
sources, and an optional nonempty predicate `element_path` for exact array membership.
Every query configuration checks plans and original source values. The source-data
pilot passed 1,260 queries (900 measured) on four fresh clusters. Its methods,
artifacts and limitations live in the companion exporter's `docs/variant-shredding-assessment.md`.

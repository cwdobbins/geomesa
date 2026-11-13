Trino Query Properties
======================

GeoMesa provides advanced query capabilities through GeoTools query hints. You can use these hints to control
various aspects of query processing, or to trigger distributed analytic processing. For an overview of query
hints, see :ref:`query_hints`.

.. _trino_variant_prefilter_hint:

Trino VARIANT Prefilter
-----------------------

This hint enables or disables a necessary SQL prefilter for case-sensitive string equality
on supported object paths, list-member paths and one-level object-field wildcards, plus positive scalar numeric
range comparisons on object paths, into schemaless JSON attributes stored as VARIANT. The original predicate still runs
client-side to preserve GeoTools matching semantics. Other data stores do not use this hint.

The hint overrides ``geomesa.trino.filter.client-variant-pushdown`` for the current query. If omitted, the system
property applies and defaults to ``false``.

================================== =========== =====================
Key                                Type        GeoServer Conversion
================================== =========== =====================
QueryHints.TRINO_VARIANT_PREFILTER ``Boolean`` ``true`` or ``false``
================================== =========== =====================

.. tabs::

    .. code-tab:: java

        import org.locationtech.geomesa.index.conf.QueryHints;

        query.getHints().put(QueryHints.TRINO_VARIANT_PREFILTER(), Boolean.TRUE);

    .. code-tab:: scala

        import org.locationtech.geomesa.index.conf.QueryHints

        query.getHints.put(QueryHints.TRINO_VARIANT_PREFILTER, true)

    .. code-tab:: none GeoServer

        ...&viewparams=TRINO_VARIANT_PREFILTER:true

For command-line exports, use ``--hints 'TRINO_VARIANT_PREFILTER=true'``. Use ``false`` to disable the prefilter for
a query when the system property enables it globally.

The client-side filtering mode still applies: ``none`` rejects predicates requiring a residual even when the hint is
``true``. In ``partial`` mode, disabling the prefilter can cause a query to fail if no other predicate can be pushed
down. See :ref:`trino_runtime_configuration` for the supported predicates and filtering modes.

VARIANT SQL Functions
---------------------

The spatial Iceberg plugin provides total functions with object-key paths represented by
``ARRAY(VARCHAR)``. Keys are case-sensitive literal strings: dots, brackets, empty strings and
names such as ``value`` have no special meaning. An empty array selects the input value itself.

* ``geomesa_variant_get(document, path)`` returns a VARIANT. A missing key, non-object encountered
  along the path, SQL NULL argument or NULL path element returns SQL NULL. A VARIANT null value
  remains a VARIANT null, distinct from SQL NULL.
* ``geomesa_variant_equals(document, path, expected)`` returns a non-null BOOLEAN. Missing values,
  wrong shapes and SQL NULL arguments return FALSE. Otherwise it uses Trino's VARIANT equality,
  including numeric comparisons across supported numeric kinds.
* ``geomesa_variant_any_equals(document, list_path, element_path, expected)`` returns TRUE if any
  element of the selected array matches ``expected`` at ``element_path``. Non-arrays, empty arrays,
  missing values and SQL NULL arguments return FALSE. An empty ``element_path`` compares each
  element itself. Separate calls can match different elements; they do not imply that one element
  satisfies both conditions.

* ``geomesa_variant_contains(document, list_path, expected)`` tests exact array membership. It is
  equivalent to ``geomesa_variant_any_equals`` with an empty element path.
* ``geomesa_variant_fields_equal(document, object_path, value_path, expected)`` tests whether any
  immediate field of the selected object matches at ``value_path``. Missing keys, non-objects,
  empty objects and SQL NULL arguments return FALSE. An empty value path compares field values
  themselves. It selects one object level; it does not recurse or correlate separate calls.

These functions work with connector optimization disabled. For example:

.. code-block:: sql

    SELECT geomesa_variant_get(attributes, ARRAY['user', 'country'])
    FROM iceberg.default.features
    WHERE geomesa_variant_equals(attributes, ARRAY['status'], CAST('active' AS VARIANT));

    SELECT count(*)
    FROM iceberg.default.features
    WHERE geomesa_variant_any_equals(
        attributes, ARRAY['items'], ARRAY['sku'], CAST('sku-42' AS VARIANT));

Datastore candidate functions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

GeoTools equality can convert numbers to strings and match collections. Its semantics differ from
strict VARIANT equality, so the datastore generates conservative candidate functions and retains the
complete GeoTools residual:

* ``geomesa_variant_may_equal(document, path, expected)`` returns FALSE only when the selected value
  and expected value are both strings and differ. Other types, missing paths, SQL/VARIANT nulls,
  wrong shapes and invalid/null path arguments return TRUE.
* ``geomesa_variant_any_may_equal(document, list_path, element_path, expected)`` returns FALSE only
  for a nonempty array whose selected element values are all proved unequal strings. Empty arrays,
  non-arrays, missing values, wrong shapes and any uncertain element return TRUE.
* ``geomesa_variant_fields_may_equal(document, object_path, value_path, expected)`` applies the same
  candidate rule to immediate object fields. Empty/non-object/uncertain selections return TRUE.

* ``geomesa_variant_may_gt/gte/lt/lte(document, path, expected)`` provide conservative scalar numeric
  range candidates. Safe integer comparisons respect the ±2^53 coercion boundary. Finite
  decimal/float/double comparisons retain rounding ambiguity, non-finite values, missing/null,
  wrong-shaped and unsupported observations. They never replace the original GeoTools residual.

TRUE means a row needs residual evaluation; it does not mean the GeoTools predicate matched.
String-candidate functions retain extra rows when the expected value is not a string; numeric
candidates likewise retain unsupported constants and observations.
Do not negate a candidate function to implement inequality or treat it as exact GeoTools equality.

With ``TRINO_VARIANT_PREFILTER=true``, positive case-sensitive string comparisons can generate these
calls for paths such as ``"$.props.name" = 'match'``, ``"$.props.items[*].name" = 'match'``,
``"$.props.tags[*]" = 'match'`` and ``"$.props.bag.*.name" = 'match'``. Reversed operands and static
``jsonPath('$.props.name')`` or ``jsonPath(props, 'items[*].name')`` expressions are supported too.
Literal ``['*']`` is a key, distinct from a wildcard. Paths are limited to 64 steps after the
attribute head and at most one wildcard. The attribute must be schemaless ``json=true`` VARIANT.

Positive scalar ``>``, ``>=``, ``<`` and ``<=`` comparisons on object paths also generate necessary
candidates, including reversed operands. Numeric equality, numeric array/wildcard ranges,
case-insensitive comparisons, dynamic paths, indices/slices, deep scans, path functions/filters
and multiple wildcards retain client residuals. Candidate conditions are
never extracted from inside OR or NOT. All GeoTools MatchAction modes retain their residual;
empty selections are kept to avoid changing their semantics. Residual source attributes remain
available even when omitted from requested output, and paging occurs after the residual.

Install the matching plugin ZIP on every Trino node before enabling this generation in an updated
datastore. It requires these functions even when connector filtering is off. Enabling the client
hint does not enable connector filtering: choose ``reference`` or ``typed`` separately through the
catalog/session settings below. Object-field wildcard functions currently remain engine-evaluated
and read complete input; scalar/array candidate functions and containment can use phase 3 takeover.
No default changes are required, and the Trino reader dependency remains ``484-shredded-3``.

Connector filtering and projection
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The reference connector filter is opt-in. Set the catalog property
``geomesa.variant.filter-mode=reference``, or enable it for a session (replace ``iceberg`` with your
catalog name):

.. code-block:: sql

    SET SESSION iceberg.variant_filter_mode = 'reference';

The default is ``off``. Reference mode can enforce mandatory equality, array-equality, containment and scalar/array candidate calls with
constant paths and expected values on top-level VARIANT columns. Unsupported expressions remain
engine filters. It requests the required paths from the Parquet reader and also evaluates correctly
on unshredded files. Existing delete, spatial and visibility filters run first. Whole VARIANT outputs
retain their complete values. Path depth and planning complexity limits can leave a call in the
engine; they do not truncate runtime arrays or change function results.

Projection pruning is independently opt-in. Set the catalog property
``geomesa.variant.projection-pushdown-enabled=true``, or enable it for a session:

.. code-block:: sql

    SET SESSION iceberg.variant_projection_pushdown_enabled = true;

    SELECT attributes['user']['country'] FROM iceberg.default.features;

The default is ``false``. For supported expressions offered by the planner, the connector requests
only the needed VARIANT paths and leaves subscript, cast and function evaluation in Trino. Whole
uses of a document require a complete read. Unsupported expressions and planner shapes can prevent
pruning; CASE/IF/TRY retain their normal evaluation behavior. Literal object-key chains and constant
paths in the functions above can supply demands, including array-element paths from
``geomesa_variant_any_equals``. This option works with filtering off, in reference mode, or in typed mode.

Typed predicates and native prefilters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Typed mode enforces the same total functions as reference mode. On shredded Parquet it evaluates
boolean, string and integer equality leaves and supported numeric-range candidates directly when
the layout and coercion boundary prove the answer. Unresolved rows,
complex values and other primitive kinds use the complete reference evaluator. Claimed functions
also run on unshredded Parquet and Avro; a file that cannot use typed evaluation is still filtered.

.. code-block:: sql

    SET SESSION iceberg.variant_filter_mode = 'typed';
    SET SESSION iceberg.variant_native_prefilter_enabled = true;

    SELECT attributes FROM iceberg.default.features
    WHERE attributes['status'] IN (CAST('active' AS VARIANT), CAST('pending' AS VARIANT));

Native prefilters are independently opt-in and require typed mode. They recognize mandatory
``=`` and ``IN`` comparisons to constant VARIANT values, ``IS NULL`` and ``IS NOT NULL`` on constant
object-key chains. The engine predicate stays in the plan. A missing key and a SQL NULL document
have SQL NULL semantics; a VARIANT null is a value. Wrong shapes, fallback data and unsupported
leaf comparisons return UNKNOWN and reach the engine. Native hints do not reconstruct a fallback
document just to decide a row. Casts, OR branches, general NOT expressions and opaque expressions
are not compiled as native hints.

The catalog equivalents are ``geomesa.variant.filter-mode=typed`` and
``geomesa.variant.native-prefilter-enabled=true``. Defaults remain ``off`` and ``false``. The optional
engine-expression evaluator is not exposed; native prefilters use only the proven primitive subset.

Whole outputs and surviving engine residuals retain complete documents. Predicate and output
views share physical leaf decodes. Pruning may save reconstruction without reducing file bytes:
whole-column demand and Parquet prefetch can still read the complete column. High match rates,
fallback-heavy data and many paths may cost more than they save; measure representative queries
before enabling these options. ``parquet_shredded_variant_selective_rebuild`` independently controls
whether ordinary output reconstruction follows selected rows (the Iceberg default is true).

Query operator metrics include ``Variant typed rows``, ``Variant fallback rows``,
``Variant native decided rows``, ``Variant unknown rows`` and ``Variant rejected rows``.
Parquet reports cumulative leaf decodes, leaf decode nanoseconds and reconstructed output rows.
Counts are per physical-column predicate group; multiple groups can count a row more than once.
Leaf decode time includes reads initiated by decoding and is not an independent I/O measurement.
The batch owns borrowed typed views and sparse fallback rows until advance or close, and charges
retained memory conservatively, including shared backing blocks in multiple views. Cached fallback
rows own compact copies so memory accounting remains proportional to their encoded size.

Pruning currently benefits Parquet. Avro retains whole-value reads. Iceberg 1.12-written ORC VARIANT
files encounter an existing reader-format incompatibility with either setting. These options are
independent of the GeoTools query hint above.

Bounded statistics pruning
^^^^^^^^^^^^^^^^^^^^^^^^^

Typed mode can skip Parquet row groups for scalar string equality, safe integer/decimal ranges
and string membership through one array step. Missing or invalid metadata and unresolved
fallback retain groups. Conservative candidates additionally need completeness/null guarantees;
empty arrays and missing selections must survive for the GeoTools residual. Repeated fallback
counts are compared with typed entry counts, not row counts. FLOAT/DOUBLE min/max alone do not
prove NaN absence, so floating-point groups are retained. ``parquet_ignore_statistics`` disables
these row-group statistics decisions.

Typed-mode ``geomesa_variant_equals`` with a string constant and a nonempty object path can also
exclude entire Iceberg data files using inclusive VARIANT path bounds. Root and key names must
use ASCII shorthand identifiers. Other literal keys retain their normal row-level semantics.
This stage runs after manifest planning and before splits; it does not prune manifests. Field IDs
resolve against the selected snapshot schema, preserving rename/time-travel behavior. Missing or
incompatible bounds retain files, and workers still enforce the original predicate.

Conservative GeoTools candidates, numeric/array file predicates and root equality do not use this
file filter. ``fileFilterSkippedFiles`` and ``fileFilterSkippedBytes`` report its exclusions;
``dataFiles`` remains the ordinary pre-filter planning count. ``parquet_ignore_statistics`` is a
row-group control; choose reference/off mode to disable the file-bound stage. No new switch or
default is introduced by this continuation.

Multiple array steps, same-element multi-key correlation, numeric/array file pruning and manifest
pruning remain outside the completed scope. Small-file prefetch, fallback and physical locality
can eliminate expected byte savings; compare native nested Parquet and whole-document consumers
before enabling a workload. Existing ``json-schema`` fields remain native and are not converted
by enabling exporter sampling.

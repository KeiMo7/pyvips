.. py:module:: pyvips.batch

``Batch processing``
====================

The batch API is opt-in.  Single-image calls such as
``Image.new_from_file()`` and ``Image.write_to_file()`` keep their existing
behaviour and exceptions.

The main entry point is :func:`pyvips.process_batch`::

    import pyvips

    summary = pyvips.process_batch(
        "inputs.json",
        "resize",
        output_dir="converted",
        output_suffix=".jpg",
        operation_args=(0.5,),
        retries=2,
        retry_interval=0.5,
        state_dir="batch-state",
    )

    for item in summary.results:
        print(item.item_id, item.filename, item.status, item.duration_seconds)

The result records are written to JSONL as the batch runs.  ``summary.results``
is a lazy sequence backed by that file, so a large batch does not keep all item
records in memory.  Each item's image object and callback return value are
dropped after the item is written or fails, before the next item starts.
The implementation uses only the Python standard library and existing
``pyvips`` APIs.

Inputs and item identity
------------------------

``inputs`` can be:

* an iterable of items;
* a path to a JSON array, JSON object with an ``items`` key, JSONL/NDJSON,
  CSV, or newline-delimited path list.

Each item can be:

* an input path string or :class:`pathlib.Path`;
* a ``BatchItem``;
* an ``(input, output)`` pair;
* an ``(id, input, output)`` triple;
* a mapping with ``input`` or ``source``, plus optional ``id`` and ``output``.

For files downloaded from object storage, pass an explicit stable ``id``.
Without an explicit ID, the API hashes the canonical input descriptor.  It
never derives identity from a basename, output directory, or path depth.
Explicit IDs must be unique within a batch.

JSON manifest example::

    {
      "version": 1,
      "items": [
        {
          "id": "objects/2026/11/a very long original name.jpg",
          "input": "/tmp/download-7f2d1a",
          "output": "converted/a.jpg"
        },
        {
          "id": "objects/2026/11/b.webp",
          "input": "/tmp/download-a93c40",
          "output": "converted/b.jpg"
        }
      ]
    }

JSONL example::

    {"id": "asset-1", "input": "/tmp/x", "output": "out/x.jpg"}
    {"id": "asset-2", "input": "/tmp/y", "output": "out/y.jpg"}

CSV files need an ``input`` (or ``source``) column and may contain ``id`` and
``output`` columns.

Operations
----------

``operation`` is either a callable or a named operation.

A callable receives one :class:`pyvips.BatchItem` plus any
``operation_args``/``operation_kwargs``.  It can:

* return a ``pyvips.Image``, which the batch writes to ``item.output``;
* return an output path when no output was declared;
* return ``None`` after writing the output itself;
* return a mapping containing an ``output`` key.

If a callback catches a load or write exception itself, re-raise it as
``BatchStageError("load")`` or ``BatchStageError("write")`` to preserve the
correct failure stage::

    def convert(item):
        try:
            image = pyvips.Image.new_from_file(item.input)
        except Exception as exc:
            raise pyvips.BatchStageError("load") from exc
        return image.resize(0.5)

Named operations have these forms:

* ``"convert"`` or ``"copy"`` loads and saves the image;
* ``"thumbnail"`` calls ``Image.thumbnail``;
* any other name is called as a method on the loaded image.

For named operations, ``output_dir`` and either ``output_suffix`` or an input
file suffix determine the output path.  With an explicit item ID this output
name is stable across runs.

Parameters
----------

``retries``
    Number of retries after the first attempt.  Defaults to ``0``.

``retry_interval``
    Fixed seconds to sleep before each retry.  Defaults to ``0``.

``resume``
    Skip previously successful items.  Defaults to ``True``.  Failed items are
    attempted again; successful items with existing outputs are not rerun.

``retry_failed``
    Forces resume mode and is intended for an operator-triggered retry pass.
    Previously successful items are still skipped, so only failed or not-yet-
    attempted items consume work.

``batch_id``
    Stable checkpoint namespace.  Defaults to a hash of the ordered input
    descriptors, or the manifest path when ``inputs`` is a file path.  Pass an
    explicit value when the same logical batch may be represented by changing
    temporary paths.

``state_dir``, ``checkpoint_path``, ``summary_dir``
    Override checkpoint and summary locations.  The defaults are under
    ``state_dir/batch-<batch_id>/``.  Results and summaries include a unique
    run ID, so repeated runs do not overwrite earlier summary files.

``on_result``
    Optional callback called with each completed ``BatchItemResult``.  The
    JSONL file is already flushed before this callback is invoked.

Checkpoint format
-----------------

The checkpoint is append-only JSONL.  Its first line is a header::

    {"type":"header","schema_version":1,"batch_id":"...",
     "input_fingerprint":"...","item_count":10000,"created_at":"..."}

Every completed item appends::

    {"type":"item","item_id":"...","index":7,"input":"/tmp/x",
     "filename":"x","output":"out/x.jpg","status":"success",
     "attempts":2,"retries":1,"duration_seconds":0.42,
     "stage_durations_seconds":{"load":0.05,"process":0.30,"write":0.07}}

Failed records additionally contain ``stage``, ``exception_type``,
``exception_module``, ``error_message``, and ``error_detail``.  The latest
record for an item ID is its final state.

The header fingerprint covers the ordered inputs and operation configuration.
If it does not match the current run, the old checkpoint is archived with a
``stale`` name and the batch performs a full run.  A malformed checkpoint is
archived with a ``corrupt`` name, a ``RuntimeWarning`` is emitted, and the
batch performs a full run.  Completed result files are not modified.

Summary
-------

Machine-readable summary fields:

``totals``
    ``total``, ``success``, ``failure``, ``skipped``, ``processed``,
    ``retries``, and ``attempts``.

``failure_types``
    Final failure counts grouped by exception class name.

``failure_stages``
    Final failure counts grouped by ``load``, ``process``, ``write``, or
    ``prepare``.

``duration_seconds``
    Processing-time distribution: count, total, minimum, mean, p50, p90, p95,
    p99, and maximum.  Retry sleep time is excluded.

``slowest_items``
    Up to ten item IDs, filenames, statuses, and durations for slow-input
    investigation.

``paths``
    Paths to the checkpoint, per-item JSONL results, JSON summary, and text
    summary.

``warnings``
    Non-fatal degradations, including recovered checkpoints and summary write
    failures.

Each entry in ``summary.results`` contains:

* ``item_id``, ``index``, ``input``, ``filename``, and ``output``;
* ``status`` and, for failures, ``stage``, ``exception_type``,
  ``exception_module``, ``error_message``, and ``error_detail``;
* ``attempts``, ``retries``, ``duration_seconds``,
  ``wall_duration_seconds``, and ``stage_durations_seconds``;
* ``resumed`` to distinguish work done in this run from skipped successes.

Example human-readable summary::

    pyvips batch summary
    ====================
    batch: 582639cc8889ae5ff69636f96ed88e3d
    run: 20261118T225605094584Z-c890f2f7
    status: partial_failure
    total: 4  success: 3  failure: 1  skipped: 0
    processed: 4  attempts: 6  retries: 2
    elapsed: 0.053s
    duration: min=0.000s mean=0.008s p50=0.009s p95=0.015s p99=0.015s max=0.015s
    failures by type:
      ValueError: 1
    failures by stage:
      process: 1
    failed items:
      bad.jpg (44776b...) stage=process type=ValueError attempts=2: bad data
    slowest items:
      a.jpg (f6e02b...) 0.015s
      c.jpg (ddf146...) 0.009s

Failure handling
----------------

Item exceptions are caught, retried when configured, and recorded.  The batch
always returns a summary, including when every item fails.  The caller decides
whether to raise, alert, or continue downstream.  If checkpoint or summary
writing fails, completed item results remain in the JSONL result file and the
summary reports a warning.

.. include global.rst

.. py:currentmodule:: pyvips

Batch colour conversion
=======================

The :func:`process_batch` entry point converts mixed image batches while
making every fallback decision explicit.  It is disabled by default and must
be enabled by the caller::

    result = pyvips.process_batch(
        items,
        target_space="srgb",
        assumptions={"image-1": "cmyk"},
        output_dir="delivery",
        ledger_path="delivery/colour-decisions.jsonl",
        enabled=True,
    )

The batch layer only calls the existing libvips ``colourspace``,
``icc_import`` and ``icc_transform`` operations.  It does not introduce a
colour-management dependency and does not change the behaviour or signatures
of the existing :class:`Image` methods.

Decision rules
--------------

Complete embedded profile
    The colour-space signature in the ICC header is used.  If a
    ``target_profile`` is supplied, the image is converted with
    ``icc_transform``.  Otherwise ``colourspace`` is used.  No assumption is
    recorded.

Missing profile
    The per-item or per-image assumption is required.  Missing declarations
    produce a failed record with ``error_code`` set to
    ``missing_assumption`` and the affected ``image_id``.

Corrupt profile
    If the header still yields a supported source colour space, that partial
    information is used and the record is marked ``degraded``.  If no source
    space can be read, the profile is treated as missing and the declared
    assumption is required.

Same source and target
    The conversion is skipped, avoiding an unnecessary resample.  If an
    output is requested, the input file is copied without decoding or
    colour-transforming it.

Assumptions are resolved per item first.  A batch-level mapping keyed by
image ID, path, basename or absolute path is used only when the item does not
declare one.  ``default_assumption`` is the final fallback.

Non-RGB inputs
--------------

Grayscale, alpha-bearing and CMYK inputs use the same rules.  Alpha channels
are retained by the underlying libvips conversion operations.  Explicit
assumptions such as ``"b-w"`` or ``"cmyk"`` are recommended for inputs whose
profile is missing.

Duplicate inputs
----------------

Inputs with the same content hash and processing plan are converted once.
Every original input still receives a record.  Later copies have
``status="duplicate"``, ``duplicate_of`` set to the first image ID and point at
the reused output.

Conclusions and resumability
----------------------------

Every original input has one dictionary in :attr:`ColorBatchResult.records`.
The result also exposes ``completed``, ``failed``, ``duplicates``,
``incomplete`` and :meth:`ColorBatchResult.conclusion_counts`.

When ``ledger_path`` is set, each conclusion is appended as one JSON object
per line.  A stable ``record_key`` prevents a completed input from being
appended again.  On restart, valid existing records are reused, a partial
final line is removed before appending, and only missing inputs are
processed.  Output files are written through a ``.part`` file and atomically
renamed; failed or interrupted artifacts retain an ``.incomplete`` suffix.

API
---

.. autoclass:: BatchItem
.. autoclass:: ProfileInspection
.. autofunction:: inspect_icc_profile
.. autoclass:: ColorBatchResult
.. autofunction:: process_batch

.. py:module:: pyvips.batch

Controlled batch concurrency
============================

libvips runs an image pipeline on its own native worker pool.  A service
which also runs many pipelines concurrently can therefore oversubscribe a
machine, especially when batch items have very different runtimes.

The :class:`Batch` entry point adds a process-wide admission budget in front
of that native pool.  It uses only the Python standard library and the
existing ``concurrency_set()`` / ``concurrency_get()`` interface.  Image
objects and their operation semantics are unchanged.

Disabled by default
-------------------

With neither ``concurrency`` nor ``adaptive=True``, :class:`Batch` runs tasks
directly in the calling thread and does not touch the process-wide budget::

    with pyvips.Batch() as batch:
        tasks = batch.map(process, items)

Fixed limit::

    with pyvips.Batch(concurrency=8) as batch:
        tasks = batch.map(process, items)

Machine-derived, load-aware limit::

    with pyvips.Batch(adaptive=True) as batch:
        tasks = batch.map(process, items)

The default derives from the CPUs available to the process and leaves
headroom for the native libvips pool.  It is not a hard-coded CPU count.  An
explicit limit is clamped to the detected CPU ceiling; zero, negative and
non-integer limits are rejected at the entry point.

When enabled, the controller also calls ``concurrency_set()`` with a derived
per-pipeline native thread count (approximately ``CPU count / batch limit``,
minimum one).  This keeps the combined Python pipeline count and native
worker threads close to the machine size instead of multiplying two
independent pools.  Set ``native_concurrency=False`` on the controller when a
different native policy is managed elsewhere.

Shared quota
------------

All batches use :func:`get_concurrency_controller` by default.  The first
explicit limit establishes the shared process-wide budget.  Later batches can
have smaller local caps and still cannot make the process exceed that shared
budget::

    with pyvips.Batch(concurrency=4, name="thumbs") as thumbs, \
            pyvips.Batch(concurrency=4, name="exports") as exports:
        thumb_tasks = thumbs.map(make_thumbnail, thumbs_input)
        export_tasks = exports.map(export_large, exports_input)

Use :func:`set_batch_concurrency` to adjust the shared budget while work is
running.  Active work is not interrupted.  A lower limit stops new admissions
until active work drains; queued work is admitted when the limit rises again.

Mixed short and long tasks
--------------------------

Tag task classes so the scheduler can reserve headroom for short work::

    tasks = batch.map(
        process,
        items,
        task_class=lambda item: item.kind,  # "short", "normal" or "long"
    )

When a submission contains both ``short`` and ``long`` work, one slot is
reserved while short work remains.  Short queued tasks are selected first.
When the short work drains, the reserved slot is immediately returned to
long work.  Long work also bypasses the reservation after a starvation
interval.  Passing ``mixed=True`` keeps the reservation for the lifetime of
the batch instead.

Failures, cancellation and wait time
------------------------------------

An item exception is stored on its :class:`BatchTask` and does not stop the
batch.  Slots are always released in a ``finally`` block.

``Batch.cancel()`` cancels work which has not started, including work blocked
on the global quota.  Work already executing is allowed to finish.  Each task
reports ``queue_wait_ms`` and ``elapsed_ms``; the shared snapshot reports
queue depth, peak use, failure/cancellation totals and average/max wait.

Timeline export
---------------

Every queue, grant, release, cancellation and limit change is recorded with a
stable field set::

    pyvips.export_batch_concurrency_timeline("quota-timeline.json")

A complete generated sample is in ``examples/concurrency-timeline.json``.
Example event::

    {
      "available": 3,
      "batch": "thumbnails",
      "duration_ms": null,
      "event": "queued",
      "failed": null,
      "in_use": 3,
      "limit": 4,
      "monotonic": 8123.42,
      "previous_limit": null,
      "queued": 2,
      "reason": null,
      "seq": 17,
      "task": "thumbnails-12",
      "task_class": "short",
      "timestamp": 1795021982.85,
      "wait_ms": null
    }

``timestamp`` is Unix epoch seconds and ``monotonic`` is suitable for
duration calculations.  The JSON is emitted with sorted keys and can be sent
to an existing log or monitoring pipeline unchanged.

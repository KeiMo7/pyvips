# vim: set fileencoding=utf-8 :

import json
import threading
import time

import pytest

import pyvips
from pyvips import batch as batch_module


def test_batch_is_disabled_by_default():
    native = pyvips.concurrency_get()
    batch = pyvips.Batch()
    try:
        tasks = batch.map(lambda item: item + 1, range(3))
    finally:
        batch.close()

    assert batch.controller is None
    assert [task.value for task in tasks] == [1, 2, 3]
    assert pyvips.concurrency_get() == native


def test_short_batch_respects_limit_and_reports_queue_time():
    controller = pyvips.ConcurrencyController(
        cpu_count=4,
        native_concurrency=False,
    )
    batch = pyvips.Batch(concurrency=3, controller=controller)

    lock = threading.Lock()
    active = 0
    peak = 0

    def work(item):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        time.sleep(0.04)
        with lock:
            active -= 1
        return item * 2

    try:
        tasks = batch.map(work, range(12))
    finally:
        batch.close()

    assert [task.value for task in tasks] == [item * 2 for item in range(12)]
    assert all(task.ok for task in tasks)
    assert peak <= 3
    assert controller.snapshot()["peak_in_use"] <= 3
    assert max(task.queue_wait_ms for task in tasks) > 0
    assert controller.snapshot()["in_use"] == 0


def test_shared_quota_across_batches():
    controller = pyvips.ConcurrencyController(
        cpu_count=8,
        native_concurrency=False,
    )
    first = pyvips.Batch(concurrency=2, controller=controller, name="first")
    second = pyvips.Batch(concurrency=2, controller=controller, name="second")

    lock = threading.Lock()
    active = 0
    peak = 0

    def work(item):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        time.sleep(0.03)
        with lock:
            active -= 1
        return item

    results = {}

    def run_first():
        results["first"] = first.map(work, range(6))

    def run_second():
        results["second"] = second.map(work, range(6))

    threads = [
        threading.Thread(target=run_first),
        threading.Thread(target=run_second),
    ]
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=3)

        assert all(not thread.is_alive() for thread in threads)
        assert peak <= 2
        assert controller.snapshot()["peak_in_use"] <= 2
        assert len(results["first"]) == 6
        assert len(results["second"]) == 6
    finally:
        first.close()
        second.close()


def test_mixed_batch_reserves_a_slot_for_short_work():
    controller = pyvips.ConcurrencyController(
        cpu_count=4,
        native_concurrency=False,
    )
    batch = pyvips.Batch(
        concurrency=3,
        controller=controller,
        reserve_short_slot=True,
    )

    release_long = threading.Event()
    long_started = [threading.Event() for _ in range(3)]
    lock = threading.Lock()
    started_count = 0

    def long_work(index):
        nonlocal started_count
        with lock:
            started_count += 1
        long_started[index].set()
        release_long.wait(2)
        return "long"

    long_tasks = [
        batch.submit(long_work, index, task_class="long")
        for index in range(3)
    ]
    try:
        assert long_started[0].wait(1)
        assert long_started[1].wait(1)
        time.sleep(0.05)

        # The third long task is held behind the reserved short-task slot.
        assert not long_started[2].is_set()

        short_tasks = batch.map(
            lambda item: item,
            range(3),
            task_class="short",
        )
        assert all(task.ok for task in short_tasks)
        assert all(task.elapsed_ms < 500 for task in short_tasks)
        assert not long_started[2].is_set()
    finally:
        release_long.set()
        for task in long_tasks:
            task.wait(2)
        batch.close()

    assert started_count == 3


def test_short_reservation_is_released_after_short_work_finishes():
    controller = pyvips.ConcurrencyController(
        cpu_count=4,
        native_concurrency=False,
    )
    batch = pyvips.Batch(concurrency=3, controller=controller)

    def work(item):
        time.sleep(0.03)
        return item

    tasks = batch.map(
        work,
        [("short", 0), ("short", 1), ("long", 0), ("long", 1),
         ("long", 2), ("long", 3)],
        task_class=lambda item: item[0],
    )
    batch.close()

    assert all(task.ok for task in tasks)
    assert controller.snapshot()["peak_in_use"] == 3
    assert controller._short_task_reservation == 0


def test_runtime_limit_change_and_cancel_do_not_interrupt_started_work():
    controller = pyvips.ConcurrencyController(
        cpu_count=4,
        native_concurrency=False,
    )
    batch = pyvips.Batch(concurrency=2, controller=controller)

    release = threading.Event()
    started = [threading.Event(), threading.Event()]

    def long_work(index):
        started[index].set()
        release.wait(2)
        return index

    def queued_work(index):
        release.wait(2)
        return index

    running = [
        batch.submit(long_work, index, task_class="long")
        for index in range(2)
    ]
    assert started[0].wait(1)
    assert started[1].wait(1)

    queued = [
        batch.submit(queued_work, index, task_class="long")
        for index in range(2, 8)
    ]
    controller.set_limit(1, reason="high water mark")
    assert all(task.status == "running" for task in running)

    cancelled_at = time.monotonic()
    cancelled = batch.cancel()
    assert time.monotonic() - cancelled_at < 0.5
    assert cancelled == len(queued)
    assert all(task.status == "cancelled" for task in queued)
    assert all(task.status == "running" for task in running)

    release.set()
    for task in running:
        task.wait(2)
    batch.close()

    snapshot = controller.snapshot()
    assert snapshot["in_use"] == 0
    assert snapshot["limit"] == 1


def test_raising_runtime_limit_admits_queued_work():
    controller = pyvips.ConcurrencyController(
        cpu_count=4,
        native_concurrency=False,
    )
    batch = pyvips.Batch(concurrency=1, controller=controller)

    release_first = threading.Event()
    second_started = threading.Event()

    def first():
        release_first.wait(2)
        return "first"

    def second():
        second_started.set()
        return "second"

    first_task = batch.submit(first)
    second_task = batch.submit(second)
    try:
        time.sleep(0.05)
        assert not second_started.is_set()
        assert task_is_pending_or_waiting(second_task)

        batch.set_concurrency(2, reason="low pressure")
        assert second_started.wait(1)
        release_first.set()
        first_task.wait(2)
        second_task.wait(2)
    finally:
        release_first.set()
        batch.close()

    assert first_task.value == "first"
    assert second_task.value == "second"
    assert controller.snapshot()["peak_in_use"] == 2


def task_is_pending_or_waiting(task):
    return task.status == "pending" or (
        task.status == "running" and not task.done
    )


def test_task_exception_is_captured_and_does_not_leak_slot():
    controller = pyvips.ConcurrencyController(
        cpu_count=4,
        native_concurrency=False,
    )
    batch = pyvips.Batch(concurrency=1, controller=controller)

    def work(item):
        if item == 1:
            raise RuntimeError("bad image")
        return item

    try:
        tasks = batch.map(work, range(4))
    finally:
        batch.close()

    assert tasks[1].status == "failed"
    assert isinstance(tasks[1].exception, RuntimeError)
    assert [task.value for task in tasks if task.ok] == [0, 2, 3]
    assert controller.snapshot()["in_use"] == 0
    assert controller.snapshot()["total_failed"] == 1


def test_invalid_limits_are_rejected_and_cpu_ceiling_applies():
    for value in (0, -1, True, 1.5):
        with pytest.raises(ValueError):
            pyvips.Batch(concurrency=value)
        with pytest.raises(ValueError):
            pyvips.concurrency_set(value)

    controller = pyvips.ConcurrencyController(
        cpu_count=2,
        native_concurrency=False,
    )
    assert controller.set_limit(20) == 2


def test_cpu_detection_failure_uses_conservative_fallback(monkeypatch):
    monkeypatch.setattr(batch_module, "_detect_cpu_count", lambda: None)
    assert pyvips.effective_cpu_count() == 1
    assert pyvips.default_concurrency() == 1

    controller = pyvips.ConcurrencyController(native_concurrency=False)
    assert controller.limit == 1
    assert controller.set_limit(4, reason="explicit override") == 4


def test_native_threads_are_derived_from_shared_limit(monkeypatch):
    calls = []
    monkeypatch.setattr(
        batch_module,
        "concurrency_set",
        lambda value: calls.append(value),
    )

    controller = pyvips.ConcurrencyController(cpu_count=8)
    controller.set_limit(4)
    assert controller.native_threads_for_limit() == 2
    assert calls[-1] == 2

    controller.set_limit(1)
    assert controller.native_threads_for_limit() == 8
    assert calls[-1] == 8


def test_timeline_export_has_stable_fields(tmp_path):
    controller = pyvips.ConcurrencyController(
        cpu_count=4,
        native_concurrency=False,
    )
    batch = pyvips.Batch(concurrency=2, controller=controller, name="audit")
    try:
        batch.map(lambda item: item, range(3))
    finally:
        batch.close()

    output = tmp_path / "timeline.json"
    text = controller.export_timeline(str(output))
    assert text == output.read_text(encoding="utf-8")

    events = json.loads(text)
    expected_fields = {
        "seq",
        "timestamp",
        "monotonic",
        "event",
        "limit",
        "previous_limit",
        "in_use",
        "queued",
        "available",
        "batch",
        "task",
        "task_class",
        "wait_ms",
        "duration_ms",
        "reason",
        "failed",
    }
    assert events
    assert all(set(event) == expected_fields for event in events)
    assert {"limit_changed", "queued", "acquired", "released"} <= {
        event["event"] for event in events
    }
    assert all(event["batch"] in (None, "audit") for event in events)


def test_adaptive_pressure_reduces_limit_without_interrupting_work():
    controller = pyvips.ConcurrencyController(
        cpu_count=8,
        native_concurrency=False,
    )
    controller.set_limit(8)
    controller.configure_adaptive(
        True,
        cooldown=0,
        load_provider=lambda: None,
    )

    lease = controller.acquire(batch="pressure", task="one")
    try:
        assert controller.limit == 8
        reduced = controller.adapt(0.95)
        assert reduced < 8
        assert controller.snapshot()["in_use"] == 1
    finally:
        lease.release()
        controller.configure_adaptive(False)

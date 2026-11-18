#!/usr/bin/env python3
"""Compare fixed and controlled concurrency on a real libvips mixed batch.

The workload is generated in memory, so the script is self-contained:

    python3 examples/batch-concurrency.py \
        --short 48 --long 8 --fixed 8 --controlled 4 \
        --timeline artifacts/concurrency-timeline.json

``fixed`` models the existing service: a standard Python thread pool with the
native libvips pool left at its normal size.  ``controlled`` reserves a slot
for short work and derives the per-pipeline native thread count from the
shared limit.
"""

import argparse
import json
import os
import random
import threading
import time

import pyvips


def _cpu_times():
    try:
        with open("/proc/stat", encoding="ascii") as stream:
            fields = stream.readline().split()[1:]
        values = [int(value) for value in fields]
    except (OSError, ValueError, IndexError):
        return None

    idle = values[3] + (values[4] if len(values) > 4 else 0)
    return sum(values), idle


class LoadSampler:
    def __init__(self, interval=0.05):
        self.interval = interval
        self.load_samples = []
        self.cpu_samples = []
        self.thread_samples = []
        self.context_switches = 0
        self._stop = threading.Event()
        self._thread = None

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join()

    def _run(self):
        previous = _cpu_times()
        while not self._stop.wait(self.interval):
            try:
                load = os.getloadavg()[0] / max(1, os.cpu_count() or 1)
                self.load_samples.append(load)
            except (OSError, AttributeError):
                pass

            current = _cpu_times()
            if previous is not None and current is not None:
                total = current[0] - previous[0]
                idle = current[1] - previous[1]
                if total > 0:
                    self.cpu_samples.append(100.0 * (total - idle) / total)
            previous = current
            try:
                with open("/proc/self/status", encoding="ascii") as stream:
                    status = stream.read().splitlines()
                values = {
                    key.strip(): value.strip()
                    for key, value in (
                        line.split(":", 1)
                        for line in status
                        if ":" in line
                    )
                }
                self.thread_samples.append(int(values["Threads"]))
                self.context_switches = (
                    int(values.get("voluntary_ctxt_switches", 0))
                    + int(values.get("nonvoluntary_ctxt_switches", 0))
                )
            except (OSError, KeyError, ValueError):
                pass

    def summary(self):
        return {
            "peak_normalized_load_1m": (
                round(max(self.load_samples), 4)
                if self.load_samples
                else None
            ),
            "mean_normalized_load_1m": (
                round(sum(self.load_samples) / len(self.load_samples), 4)
                if self.load_samples
                else None
            ),
            "peak_host_cpu_percent": (
                round(max(self.cpu_samples), 2)
                if self.cpu_samples
                else None
            ),
            "mean_host_cpu_percent": (
                round(sum(self.cpu_samples) / len(self.cpu_samples), 2)
                if self.cpu_samples
                else None
            ),
            "peak_process_threads": (
                max(self.thread_samples) if self.thread_samples else None
            ),
            "context_switches": self.context_switches or None,
        }


def make_work():
    def work(spec):
        kind, index = spec
        if kind == "short":
            image = pyvips.Image.gaussnoise(
                256,
                256,
                mean=128,
                sigma=30,
                seed=index + 1,
            )
            image = image.resize(0.5)
            image.write_to_buffer(".jpg", Q=85)
        else:
            image = pyvips.Image.gaussnoise(
                2048,
                2048,
                mean=128,
                sigma=30,
                seed=index + 1000,
            )
            image = image.gaussblur(4).resize(2.0)
            image.write_to_buffer(".png", compression=6)
        return kind

    return work


def run_fixed(specs, concurrency, cpu_count):
    work = make_work()
    original_native = pyvips.concurrency_get()
    pyvips.concurrency_set(cpu_count)
    next_index = [0]
    index_lock = threading.Lock()
    errors = []

    def worker():
        while True:
            with index_lock:
                if next_index[0] >= len(specs):
                    return
                index = next_index[0]
                next_index[0] += 1
            try:
                work(specs[index])
            except BaseException as exc:
                errors.append(exc)
                return

    threads = [
        threading.Thread(target=worker, daemon=True)
        for _ in range(max(1, concurrency))
    ]
    sampler = LoadSampler()
    started = time.monotonic()
    sampler.start()
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
    finally:
        elapsed = time.monotonic() - started
        sampler.stop()
        pyvips.concurrency_set(original_native)

    return {
        "mode": "fixed",
        "concurrency": concurrency,
        "native_threads_per_pipeline": cpu_count,
        "elapsed_seconds": round(elapsed, 3),
        "tasks_per_second": round(len(specs) / elapsed, 3),
        "task_count": len(specs),
        "failed": len(errors),
        **sampler.summary(),
    }


def run_controlled(
    specs,
    concurrency,
    cpu_count,
    adaptive,
    timeline_path,
):
    work = make_work()
    controller = pyvips.ConcurrencyController(
        cpu_count=cpu_count,
        native_concurrency=True,
    )
    batch = pyvips.Batch(
        concurrency=None if adaptive else concurrency,
        adaptive=adaptive,
        mixed=False,
        controller=controller,
        adaptive_interval=0.1,
        adaptive_cooldown=0.2,
        name="controlled",
    )
    sampler = LoadSampler()
    started = time.monotonic()
    sampler.start()
    try:
        tasks = batch.map(
            work,
            specs,
            task_class=lambda spec: spec[0],
        )
    finally:
        elapsed = time.monotonic() - started
        sampler.stop()
        batch.close()
        if timeline_path:
            controller.export_timeline(timeline_path)

    snapshot = controller.snapshot()
    return {
        "mode": "adaptive" if adaptive else "controlled-fixed",
        "configured_concurrency": None if adaptive else concurrency,
        "native_threads_per_pipeline_at_end": snapshot[
            "native_threads_per_task"
        ],
        "elapsed_seconds": round(elapsed, 3),
        "tasks_per_second": round(len(tasks) / elapsed, 3),
        "task_count": len(tasks),
        "failed": sum(1 for task in tasks if task.status == "failed"),
        "peak_in_use": snapshot["peak_in_use"],
        "max_queue_wait_ms": round(
            max(
                task.queue_wait_ms or 0.0
                for task in tasks
            ),
            3,
        ),
        **sampler.summary(),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--short", type=int, default=48)
    parser.add_argument("--long", type=int, default=8)
    parser.add_argument("--fixed", type=int, default=None)
    parser.add_argument("--controlled", type=int, default=4)
    parser.add_argument("--adaptive", action="store_true")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--timeline", default=None)
    args = parser.parse_args()

    cpu_count = max(1, len(os.sched_getaffinity(0))) \
        if hasattr(os, "sched_getaffinity") else max(1, os.cpu_count() or 1)
    fixed = args.fixed or cpu_count

    specs = (
        [("short", index) for index in range(args.short)]
        + [("long", index) for index in range(args.long)]
    )
    random.Random(args.seed).shuffle(specs)

    # Cache off so both runs render the same operations from scratch.
    pyvips.cache_set_max(0)

    fixed_result = run_fixed(specs, fixed, cpu_count)
    controlled_result = run_controlled(
        specs,
        args.controlled,
        cpu_count,
        args.adaptive,
        args.timeline,
    )

    result = {
        "cpu_count": cpu_count,
        "workload": {
            "short": args.short,
            "long": args.long,
            "seed": args.seed,
        },
        "fixed": fixed_result,
        "controlled": controlled_result,
    }
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

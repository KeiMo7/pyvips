"""Controlled batch execution for libvips workloads.

The native libvips pipeline has its own worker pool.  Running many image
pipelines from Python at the same time can therefore oversubscribe a machine
even when the Python side has a sensible-looking worker count.

This module adds a small, process-wide admission controller in front of the
native pool.  It is deliberately built from the standard library threading
primitives and the existing ``vips_concurrency_set/get`` interface.  No image
operation semantics are changed.
"""

import json
import math
import os
import threading
import time
from collections import deque

from .voperation import concurrency_get, concurrency_set


_SHORT = "short"
_NORMAL = "normal"
_LONG = "long"

_TASK_CLASS_PRIORITY = {
    _SHORT: 0,
    _NORMAL: 1,
    _LONG: 2,
}

_TASK_CLASS_ALIASES = {
    "short": _SHORT,
    "small": _SHORT,
    "thumbnail": _SHORT,
    "normal": _NORMAL,
    "default": _NORMAL,
    "long": _LONG,
    "large": _LONG,
    "export": _LONG,
}

_CONSERVATIVE_CPU_FALLBACK = 1
_DEFAULT_CPU_DIVISOR = 2
_ADAPTIVE_HIGH_PRESSURE = 0.85
_ADAPTIVE_LOW_PRESSURE = 0.65


class TaskCancelled(Exception):
    """Raised internally when a task is cancelled before it starts."""


class TaskTimedOut(TimeoutError):
    """Raised when waiting for a concurrency slot exceeds its timeout."""


def _validate_positive_int(value, name="concurrency"):
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _normalize_task_class(task_class):
    if task_class is None:
        return _NORMAL

    if not isinstance(task_class, str):
        raise ValueError("task_class must be 'short', 'normal' or 'long'")

    try:
        return _TASK_CLASS_ALIASES[task_class.lower()]
    except KeyError:
        raise ValueError("task_class must be 'short', 'normal' or 'long'")


def _detect_cpu_count():
    """Return the CPUs available to this process, or None if detection fails."""

    sched_getaffinity = getattr(os, "sched_getaffinity", None)
    if sched_getaffinity is not None:
        try:
            count = len(sched_getaffinity(0))
            if count > 0:
                return count
        except (OSError, TypeError, ValueError):
            pass

    try:
        count = os.cpu_count()
    except (AttributeError, TypeError, ValueError):
        count = None

    if isinstance(count, int) and count > 0:
        return count

    return None


def effective_cpu_count():
    """Return usable CPU count, falling back conservatively on failure."""

    detected = _detect_cpu_count()
    if detected is not None:
        return detected

    # Do not guess a large machine size when the OS cannot tell us.  One slot
    # keeps the process usable and lets an explicit limit raise the ceiling.
    return _CONSERVATIVE_CPU_FALLBACK


def default_concurrency(cpu_count=None):
    """Derive the conservative default batch concurrency for a machine.

    The native libvips pipeline also runs work in parallel.  Using half of the
    available logical CPUs leaves headroom for that native pool and for the
    rest of the service.  An explicit limit can raise this up to the CPU count.
    """

    if cpu_count is None:
        cpu_count = effective_cpu_count()
    else:
        cpu_count = _validate_positive_int(cpu_count, "cpu_count")

    return max(1, cpu_count // _DEFAULT_CPU_DIVISOR)


class _Waiter:
    __slots__ = (
        "sequence",
        "batch",
        "task",
        "task_class",
        "queued_at",
        "granted",
        "cancelled",
        "lease",
    )

    def __init__(self, sequence, batch, task, task_class):
        self.sequence = sequence
        self.batch = batch
        self.task = task
        self.task_class = task_class
        self.queued_at = time.monotonic()
        self.granted = False
        self.cancelled = False
        self.lease = None


class _Lease:
    def __init__(self, controller, waiter):
        self._controller = controller
        self._waiter = waiter
        self._acquired_at = time.monotonic()
        self._released = False

    @property
    def wait_ms(self):
        return (self._acquired_at - self._waiter.queued_at) * 1000.0

    def release(self, failed=False):
        self._controller._release(self, failed=failed)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.release(failed=exc_type is not None)
        return False


class ConcurrencyController:
    """Process-wide concurrency budget shared by all :class:`Batch` objects.

    The controller grants at most ``limit`` active slots.  Lowering the limit
    never interrupts work which is already running: new work simply waits
    until active work drains below the new limit.
    """

    def __init__(
        self,
        cpu_count=None,
        native_concurrency=True,
        max_events=4096,
    ):
        if cpu_count is None:
            detected = _detect_cpu_count()
            self._cpu_count = (
                detected
                if detected is not None
                else _CONSERVATIVE_CPU_FALLBACK
            )
            self._cpu_count_known = detected is not None
        else:
            self._cpu_count = _validate_positive_int(cpu_count, "cpu_count")
            self._cpu_count_known = True

        self._native_concurrency = bool(native_concurrency)
        try:
            self._original_native_concurrency = int(concurrency_get())
        except Exception:
            self._original_native_concurrency = None

        self._condition = threading.Condition(threading.RLock())
        self._limit = None
        self._base_limit = None
        self._limit_source = None
        self._ceiling = self._cpu_count
        self._in_use = 0
        self._waiters = []
        self._next_sequence = 0
        self._active_by_class = {
            _SHORT: 0,
            _NORMAL: 0,
            _LONG: 0,
        }
        self._cancel_waiters = {}

        self._short_task_reservation = 0
        self._short_task_reservation_manual = 0
        self._short_reservation_owners = set()
        self._long_starvation_seconds = 1.0

        self._adaptive = False
        self._adaptive_ceiling = self._cpu_count
        self._load_provider = None
        self._adaptive_interval = 0.5
        self._adaptive_cooldown = 0.25
        self._last_limit_change = 0.0
        self._monitor_stop = None
        self._monitor_thread = None

        self._listeners = []
        self._timeline = deque(maxlen=_validate_positive_int(max_events,
                                                             "max_events"))
        self._event_sequence = 0

        self._total_acquired = 0
        self._total_released = 0
        self._total_failed = 0
        self._total_cancelled = 0
        self._total_wait_ms = 0.0
        self._max_wait_ms = 0.0
        self._peak_in_use = 0

    @property
    def cpu_count(self):
        return self._cpu_count

    @property
    def limit(self):
        with self._condition:
            self._ensure_limit_locked()
            return self._limit

    @property
    def base_limit(self):
        with self._condition:
            self._ensure_limit_locked()
            return self._base_limit

    @property
    def max_limit(self):
        with self._condition:
            if not self._cpu_count_known:
                return max(self._limit or 0, self._adaptive_ceiling)
            return min(self._cpu_count, self._adaptive_ceiling)

    @property
    def adaptive(self):
        with self._condition:
            return self._adaptive

    def set_cpu_count(self, cpu_count):
        """Set the machine CPU ceiling before the controller is first used."""

        cpu_count = _validate_positive_int(cpu_count, "cpu_count")
        with self._condition:
            if self._limit is not None:
                raise ValueError("cpu_count cannot change after concurrency is configured")
            self._cpu_count = cpu_count
            self._cpu_count_known = True
            self._ceiling = cpu_count
            self._adaptive_ceiling = cpu_count

    def _record_locked(
        self,
        event,
        batch=None,
        task=None,
        task_class=None,
        wait_ms=None,
        duration_ms=None,
        reason=None,
        failed=None,
        previous_limit=None,
    ):
        limit = self._limit if self._limit is not None else 0
        queued = len(self._waiters)
        self._event_sequence += 1
        self._timeline.append({
            "seq": self._event_sequence,
            "timestamp": time.time(),
            "monotonic": time.monotonic(),
            "event": event,
            "limit": limit,
            "previous_limit": previous_limit,
            "in_use": self._in_use,
            "queued": queued,
            "available": max(0, limit - self._in_use),
            "batch": batch,
            "task": task,
            "task_class": task_class,
            "wait_ms": None if wait_ms is None else round(wait_ms, 3),
            "duration_ms": None if duration_ms is None else round(duration_ms, 3),
            "reason": reason,
            "failed": failed,
        })

    def _apply_native_concurrency_locked(self, limit):
        if not self._native_concurrency:
            return

        try:
            # Each active pipeline gets a slice of the native pool.  Keeping
            # batch_limit * native_threads near the CPU count avoids the
            # 8 pipelines x 8 native threads oversubscription problem.
            native_threads = self.native_threads_for_limit(limit)
            concurrency_set(native_threads)
        except Exception as exc:
            self._record_locked(
                "native_concurrency_error",
                reason=str(exc),
            )

    def native_threads_for_limit(self, limit=None):
        """Return the native thread count used for each active pipeline."""

        if limit is None:
            with self._condition:
                limit = self._limit

        if limit is None:
            limit = 1
        limit = max(1, int(limit))

        # If CPU discovery failed, one native thread is the safe choice: it
        # avoids multiplying an unknown machine size by the batch limit.
        if not self._cpu_count_known:
            return 1

        return max(1, self._cpu_count // limit)

    def set_native_concurrency(self, enabled):
        """Enable or disable automatic native thread-pool coordination."""

        with self._condition:
            self._native_concurrency = bool(enabled)
            if self._native_concurrency and self._limit is not None:
                self._apply_native_concurrency_locked(self._limit)

    def _ensure_limit_locked(self):
        if self._limit is not None:
            return False

        derived = default_concurrency(self._cpu_count)
        self._limit = derived
        self._base_limit = derived
        self._limit_source = "default"
        self._apply_native_concurrency_locked(derived)
        self._record_locked("limit_initialized", reason="machine default")
        self._dispatch_locked()
        return True

    def _set_limit_locked(self, limit, reason, source):
        limit = _validate_positive_int(limit, "limit")
        self._ensure_limit_locked()

        if self._cpu_count_known:
            ceiling = min(self._cpu_count, self._adaptive_ceiling)
        else:
            ceiling = max(limit, self._adaptive_ceiling)
        clamped = limit > ceiling
        if clamped:
            limit = ceiling

        previous = self._limit
        if limit != previous:
            self._last_limit_change = time.monotonic()

        self._limit = limit
        self._limit_source = source
        if source in ("manual", "batch"):
            self._base_limit = limit

        event_reason = reason
        if clamped:
            event_reason = f"{reason}; clamped to CPU count {ceiling}"

        self._apply_native_concurrency_locked(limit)
        self._record_locked(
            "limit_changed",
            reason=event_reason,
            previous_limit=previous,
        )
        self._dispatch_locked()
        return limit

    def set_limit(self, limit, reason="manual"):
        """Set the process-wide slot limit without interrupting active work."""

        with self._condition:
            changed = self._set_limit_locked(limit, reason, "manual")
        self._notify_listeners()
        return changed

    def request_limit(self, limit, reason="batch"):
        """Apply a batch-requested limit if no process-wide limit exists yet.

        The first explicit request establishes the shared budget.  Later
        batches can have a smaller local cap, but they cannot silently multiply
        the process-wide budget.
        """

        limit = _validate_positive_int(limit, "concurrency")
        with self._condition:
            if self._limit is None or self._limit_source == "default":
                effective = self._set_limit_locked(limit, reason, "batch")
            else:
                effective = self._limit
        self._notify_listeners()
        return effective

    def configure_adaptive(
        self,
        enabled=True,
        ceiling=None,
        load_provider=None,
        interval=0.5,
        cooldown=0.25,
    ):
        """Enable or disable load-driven changes to the shared limit.

        ``load_provider`` must return a normalized pressure (for example,
        one-minute load average divided by CPU count), or None when pressure
        cannot be sampled.
        """

        if interval <= 0:
            raise ValueError("interval must be greater than zero")

        with self._condition:
            self._ensure_limit_locked()

            self._adaptive = bool(enabled)
            if ceiling is not None:
                self._adaptive_ceiling = min(
                    self._cpu_count,
                    _validate_positive_int(ceiling, "ceiling"),
                )
            else:
                self._adaptive_ceiling = self._cpu_count

            if load_provider is not None:
                self._load_provider = load_provider
            elif self._load_provider is None:
                self._load_provider = self._default_load_provider

            self._adaptive_interval = float(interval)
            self._adaptive_cooldown = max(0.0, float(cooldown))

            if self._adaptive:
                self._start_monitor_locked()
            else:
                self._stop_monitor_locked()

    def _default_load_provider(self):
        getloadavg = getattr(os, "getloadavg", None)
        if getloadavg is None:
            return None

        try:
            load = getloadavg()[0]
        except (OSError, TypeError, ValueError):
            return None

        return load / max(1, self._cpu_count)

    def _start_monitor_locked(self):
        if self._monitor_thread is not None and self._monitor_thread.is_alive():
            return

        stop_event = threading.Event()
        self._monitor_stop = stop_event

        def monitor():
            while not stop_event.wait(self._adaptive_interval):
                try:
                    pressure = self._load_provider()
                except Exception as exc:
                    with self._condition:
                        self._record_locked("pressure_sample_error", reason=str(exc))
                    continue

                if pressure is not None:
                    try:
                        self.adapt(pressure)
                    except Exception:
                        # A monitoring failure must not take down the batch.
                        pass

        thread = threading.Thread(
            target=monitor,
            name="pyvips-concurrency-monitor",
            daemon=True,
        )
        self._monitor_thread = thread
        thread.start()

    def _stop_monitor_locked(self):
        stop_event = self._monitor_stop
        thread = self._monitor_thread
        self._monitor_stop = None
        self._monitor_thread = None
        if stop_event is not None:
            stop_event.set()
        return thread

    def adapt(self, pressure):
        """Apply a normalized external-pressure sample to the current limit."""

        pressure = float(pressure)
        with self._condition:
            self._ensure_limit_locked()
            now = time.monotonic()

            if self._limit <= 1 and pressure <= _ADAPTIVE_LOW_PRESSURE:
                return self._limit

            # React to high pressure immediately; grow cautiously on low
            # pressure and only when there is queued work to admit.
            if pressure >= _ADAPTIVE_HIGH_PRESSURE:
                step = max(1, int(math.ceil(self._limit * 0.25)))
                new_limit = max(1, self._limit - step)
                if new_limit < self._limit:
                    self._last_limit_change = now
                    self._limit = new_limit
                    self._limit_source = "adaptive"
                    self._apply_native_concurrency_locked(new_limit)
                    self._record_locked(
                        "limit_changed",
                        reason="external pressure high",
                        previous_limit=self._limit + step,
                    )
                    self._dispatch_locked()
            elif (
                pressure <= _ADAPTIVE_LOW_PRESSURE
                and self._waiters
                and self._limit < min(self._cpu_count, self._adaptive_ceiling)
                and now - self._last_limit_change >= self._adaptive_cooldown
            ):
                previous = self._limit
                self._limit += 1
                self._last_limit_change = now
                self._limit_source = "adaptive"
                self._apply_native_concurrency_locked(self._limit)
                self._record_locked(
                    "limit_changed",
                    reason="external pressure low",
                    previous_limit=previous,
                )
                self._dispatch_locked()

            result = self._limit
        self._notify_listeners()
        return result

    def set_short_task_reservation(self, slots):
        """Reserve up to ``slots`` slots for short work in mixed batches."""

        if isinstance(slots, bool) or not isinstance(slots, int) or slots < 0:
            raise ValueError("slots must be zero or a positive integer")

        with self._condition:
            self._short_task_reservation_manual = slots
            self._refresh_short_task_reservation_locked()
            self._dispatch_locked()

    def _refresh_short_task_reservation_locked(self):
        self._short_task_reservation = max(
            self._short_task_reservation_manual,
            1 if self._short_reservation_owners else 0,
        )

    def reserve_short_task_slot(self, owner):
        """Register a batch which wants one slot reserved for short work."""

        with self._condition:
            self._short_reservation_owners.add(owner)
            self._refresh_short_task_reservation_locked()
            self._dispatch_locked()

    def release_short_task_slot(self, owner):
        """Remove a batch's short-work reservation."""

        with self._condition:
            self._short_reservation_owners.discard(owner)
            self._refresh_short_task_reservation_locked()
            self._dispatch_locked()

    def add_listener(self, listener):
        with self._condition:
            if listener not in self._listeners:
                self._listeners.append(listener)

    def remove_listener(self, listener):
        with self._condition:
            try:
                self._listeners.remove(listener)
            except ValueError:
                pass

    def _notify_listeners(self):
        with self._condition:
            listeners = list(self._listeners)
        for listener in listeners:
            try:
                listener()
            except Exception:
                pass

    def _pick_waiter_locked(self):
        if not self._waiters:
            return None

        live = [waiter for waiter in self._waiters if not waiter.cancelled]
        if not live:
            self._waiters = []
            return None

        now = time.monotonic()
        oldest = min(live, key=lambda waiter: waiter.sequence)

        # Let old normal/long work through after a while so a steady stream of
        # short work cannot starve the other classes indefinitely.
        if (
            oldest.task_class == _NORMAL
            and now - oldest.queued_at >= self._long_starvation_seconds
        ):
            return oldest

        if (
            oldest.task_class == _LONG
            and now - oldest.queued_at >= self._long_starvation_seconds
            and self._active_by_class[_LONG] < self._limit
        ):
            return oldest

        short = [waiter for waiter in live if waiter.task_class == _SHORT]
        if short:
            return min(short, key=lambda waiter: waiter.sequence)

        normal = [waiter for waiter in live if waiter.task_class == _NORMAL]
        if normal:
            return min(normal, key=lambda waiter: waiter.sequence)

        long_waiters = [waiter for waiter in live if waiter.task_class == _LONG]
        if not long_waiters:
            return None

        long_cap = self._limit
        if self._short_task_reservation:
            long_cap = max(1, self._limit - self._short_task_reservation)

        if self._active_by_class[_LONG] >= long_cap:
            return None

        return min(long_waiters, key=lambda waiter: waiter.sequence)

    def _dispatch_locked(self):
        if self._limit is None:
            return

        while self._in_use < self._limit:
            waiter = self._pick_waiter_locked()
            if waiter is None:
                break

            try:
                self._waiters.remove(waiter)
            except ValueError:
                continue

            waiter.granted = True
            waiter.lease = _Lease(self, waiter)
            self._in_use += 1
            self._active_by_class[waiter.task_class] += 1
            self._total_acquired += 1
            self._peak_in_use = max(self._peak_in_use, self._in_use)
            self._condition.notify_all()

    def acquire(
        self,
        batch=None,
        task=None,
        task_class=_NORMAL,
        timeout=None,
        cancel_event=None,
    ):
        """Wait for and acquire one process-wide slot.

        The returned lease must be released in a ``finally`` block.  Waiting
        does not consume a slot and is reported in the controller timeline.
        """

        task_class = _normalize_task_class(task_class)
        if timeout is not None:
            timeout = float(timeout)
            if timeout < 0:
                raise ValueError("timeout must not be negative")

        with self._condition:
            self._ensure_limit_locked()

            if cancel_event is not None and cancel_event.is_set():
                raise TaskCancelled()

            waiter = _Waiter(
                self._next_sequence,
                batch,
                task,
                task_class,
            )
            self._next_sequence += 1
            self._waiters.append(waiter)
            if cancel_event is not None:
                self._cancel_waiters[cancel_event] = waiter

            self._record_locked(
                "queued",
                batch=batch,
                task=task,
                task_class=task_class,
            )

            deadline = None
            if timeout is not None:
                deadline = time.monotonic() + timeout

            try:
                while True:
                    self._dispatch_locked()

                    if waiter.granted:
                        wait_ms = (time.monotonic() - waiter.queued_at) * 1000.0
                        self._total_wait_ms += wait_ms
                        self._max_wait_ms = max(self._max_wait_ms, wait_ms)
                        self._record_locked(
                            "acquired",
                            batch=batch,
                            task=task,
                            task_class=task_class,
                            wait_ms=wait_ms,
                        )
                        return waiter.lease

                    cancelled = waiter.cancelled or (
                        cancel_event is not None and cancel_event.is_set()
                    )
                    if cancelled:
                        waiter.cancelled = True
                        self._remove_waiter_locked(waiter)
                        self._total_cancelled += 1
                        self._record_locked(
                            "cancelled",
                            batch=batch,
                            task=task,
                            task_class=task_class,
                            reason="cancelled while queued",
                        )
                        raise TaskCancelled()

                    if deadline is not None:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            self._remove_waiter_locked(waiter)
                            self._total_cancelled += 1
                            self._record_locked(
                                "cancelled",
                                batch=batch,
                                task=task,
                                task_class=task_class,
                                reason="queue timeout",
                            )
                            raise TaskTimedOut("timed out waiting for concurrency slot")
                    else:
                        remaining = None

                    self._condition.wait(remaining)
            finally:
                if cancel_event is not None:
                    self._cancel_waiters.pop(cancel_event, None)

    def _remove_waiter_locked(self, waiter):
        try:
            self._waiters.remove(waiter)
        except ValueError:
            pass

    def cancel_waiting(self, cancel_event):
        """Cancel a queued acquisition identified by its cancellation event."""

        with self._condition:
            waiter = self._cancel_waiters.get(cancel_event)
            if waiter is not None and not waiter.granted:
                waiter.cancelled = True
                self._remove_waiter_locked(waiter)
                self._condition.notify_all()

    def _release(self, lease, failed=False):
        with self._condition:
            if lease._released:
                return
            lease._released = True

            waiter = lease._waiter
            self._in_use = max(0, self._in_use - 1)
            self._active_by_class[waiter.task_class] = max(
                0,
                self._active_by_class[waiter.task_class] - 1,
            )
            self._total_released += 1
            if failed:
                self._total_failed += 1

            duration_ms = (time.monotonic() - lease._acquired_at) * 1000.0
            self._record_locked(
                "released",
                batch=waiter.batch,
                task=waiter.task,
                task_class=waiter.task_class,
                wait_ms=lease.wait_ms,
                duration_ms=duration_ms,
                failed=bool(failed),
            )

            self._dispatch_locked()
            self._condition.notify_all()

    def snapshot(self):
        """Return a point-in-time view of shared quota usage and counters."""

        with self._condition:
            self._ensure_limit_locked()
            return {
                "cpu_count": self._cpu_count,
                "limit": self._limit,
                "base_limit": self._base_limit,
                "max_limit": min(self._cpu_count, self._adaptive_ceiling),
                "available": max(0, self._limit - self._in_use),
                "in_use": self._in_use,
                "queued": len(self._waiters),
                "active_by_class": dict(self._active_by_class),
                "peak_in_use": self._peak_in_use,
                "total_acquired": self._total_acquired,
                "total_released": self._total_released,
                "total_failed": self._total_failed,
                "total_cancelled": self._total_cancelled,
                "avg_wait_ms": (
                    round(self._total_wait_ms / self._total_acquired, 3)
                    if self._total_acquired
                    else 0.0
                ),
                "max_wait_ms": round(self._max_wait_ms, 3),
                "adaptive": self._adaptive,
                "native_concurrency": self._native_concurrency,
                "native_threads_per_task": self.native_threads_for_limit(),
            }

    def timeline(self, event=None, limit=None):
        """Return timeline events as ordinary dictionaries."""

        with self._condition:
            events = list(self._timeline)

        if event is not None:
            events = [item for item in events if item["event"] == event]
        if limit is not None:
            if limit <= 0:
                events = []
            else:
                events = events[-limit:]
        return events

    def export_timeline(self, path=None, indent=2):
        """Export stable, JSON-serializable timeline events.

        The file is written when ``path`` is supplied.  The JSON string is
        returned in either case so callers can forward it directly to a log or
        monitoring pipeline.
        """

        payload = json.dumps(
            self.timeline(),
            ensure_ascii=False,
            sort_keys=True,
            indent=indent,
        )
        if path is not None:
            with open(path, "w", encoding="utf-8") as stream:
                stream.write(payload)
        return payload

    def reset(self):
        """Reset controller state.  Intended for tests and fresh worker models."""

        with self._condition:
            self._stop_monitor_locked()

            for waiter in self._waiters:
                waiter.cancelled = True
            self._waiters = []
            self._cancel_waiters = {}
            self._in_use = 0
            self._active_by_class = {
                _SHORT: 0,
                _NORMAL: 0,
                _LONG: 0,
            }
            self._short_task_reservation = 0
            self._short_task_reservation_manual = 0
            self._short_reservation_owners = set()
            self._limit = None
            self._base_limit = None
            self._limit_source = None
            self._adaptive = False
            self._adaptive_ceiling = self._cpu_count
            self._timeline.clear()
            self._event_sequence = 0
            self._total_acquired = 0
            self._total_released = 0
            self._total_failed = 0
            self._total_cancelled = 0
            self._total_wait_ms = 0.0
            self._max_wait_ms = 0.0
            self._peak_in_use = 0
            self._condition.notify_all()

            if (
                self._native_concurrency
                and self._original_native_concurrency is not None
            ):
                try:
                    concurrency_set(self._original_native_concurrency)
                except Exception:
                    pass


_global_controller = None
_global_lock = threading.Lock()


def get_concurrency_controller():
    """Return the process-wide controller shared by all batches."""

    global _global_controller
    with _global_lock:
        if _global_controller is None:
            _global_controller = ConcurrencyController()
        return _global_controller


def reset_batch_concurrency(cpu_count=None, native_concurrency=True):
    """Replace the process-wide controller.

    This is primarily useful in tests and when a worker process is reused for
    an entirely new workload model.
    """

    global _global_controller
    with _global_lock:
        if _global_controller is not None:
            _global_controller.reset()
        _global_controller = ConcurrencyController(
            cpu_count=cpu_count,
            native_concurrency=native_concurrency,
        )
        return _global_controller


def set_batch_concurrency(limit, reason="manual"):
    """Set the shared process-wide batch concurrency limit."""

    return get_concurrency_controller().set_limit(limit, reason=reason)


def get_batch_concurrency():
    """Return the shared process-wide batch concurrency limit."""

    return get_concurrency_controller().limit


def batch_concurrency_snapshot():
    """Return shared quota statistics."""

    return get_concurrency_controller().snapshot()


def batch_concurrency_timeline(event=None, limit=None):
    """Return shared quota timeline events."""

    return get_concurrency_controller().timeline(event=event, limit=limit)


def export_batch_concurrency_timeline(path=None, indent=2):
    """Export the shared quota timeline as JSON."""

    return get_concurrency_controller().export_timeline(path=path, indent=indent)


class BatchTask:
    """Result handle for one task submitted to a :class:`Batch`."""

    def __init__(self, function, args, kwargs, task_class, batch_name, sequence):
        self.id = f"{batch_name}-{sequence}"
        self.name = getattr(function, "__name__", repr(function))
        self.task_class = task_class
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.value = None
        self.exception = None
        self.status = "pending"
        self._enqueued_at = time.monotonic()
        self._started_at = None
        self._finished_at = None
        self._cancel_event = threading.Event()
        self._done = threading.Event()
        self._lock = threading.Lock()

    @property
    def ok(self):
        return self.status == "succeeded"

    @property
    def done(self):
        return self._done.is_set()

    @property
    def queue_wait_ms(self):
        if self._started_at is None:
            return None
        return round((self._started_at - self._enqueued_at) * 1000.0, 3)

    @property
    def elapsed_ms(self):
        if self._started_at is None or self._finished_at is None:
            return None
        return round((self._finished_at - self._started_at) * 1000.0, 3)

    def _begin(self):
        with self._lock:
            if self.status == "cancelled" or self._cancel_event.is_set():
                self.status = "cancelled"
                self._finished_at = time.monotonic()
                self._done.set()
                return False
            self.status = "running"
            self._started_at = time.monotonic()
            return True

    def _finish(self, value=None, exception=None):
        with self._lock:
            if self.status == "cancelled":
                return
            if exception is None:
                self.status = "succeeded"
                self.value = value
            else:
                self.status = "failed"
                self.exception = exception
            self._finished_at = time.monotonic()
            self._done.set()

    def _cancel(self):
        with self._lock:
            if self.status not in ("pending", "running"):
                return False
            if self.status == "running":
                return False
            self.status = "cancelled"
            self._finished_at = time.monotonic()
            self._cancel_event.set()
            self._done.set()
            return True

    def wait(self, timeout=None):
        if not self._done.wait(timeout):
            raise TimeoutError(f"task {self.id} is not done")
        return self

    def result(self, timeout=None, raise_error=True):
        """Wait for the task and return its value.

        With ``raise_error=False`` the captured exception is returned instead
        of being raised, which is useful when building batch summaries.
        """

        self.wait(timeout)
        if self.status == "cancelled":
            raise TaskCancelled(f"task {self.id} was cancelled")
        if self.exception is not None:
            if raise_error:
                raise self.exception
            return self.exception
        return self.value

    def __repr__(self):
        return (
            f"<BatchTask {self.id} status={self.status!r} "
            f"class={self.task_class!r}>"
        )


class Batch:
    """Batch entry point with optional controlled concurrency.

    With no ``concurrency`` and ``adaptive=False`` the batch is disabled and
    tasks run directly in the calling thread, preserving the old behavior.
    Set ``concurrency`` for a fixed limit or ``adaptive=True`` for a
    machine-derived, load-aware limit.
    """

    _sequence = 0
    _sequence_lock = threading.Lock()

    def __init__(
        self,
        concurrency=None,
        max_concurrency=None,
        *,
        enabled=None,
        adaptive=False,
        controller=None,
        cpu_count=None,
        native_concurrency=True,
        mixed=False,
        reserve_short_slot=None,
        load_provider=None,
        adaptive_interval=0.5,
        adaptive_cooldown=0.25,
        name=None,
    ):
        if concurrency is not None and max_concurrency is not None:
            if concurrency != max_concurrency:
                raise ValueError("concurrency and max_concurrency disagree")
        if concurrency is None:
            concurrency = max_concurrency
        if concurrency is not None:
            concurrency = _validate_positive_int(concurrency, "concurrency")

        if enabled is None:
            enabled = concurrency is not None or bool(adaptive)

        with Batch._sequence_lock:
            Batch._sequence += 1
            default_name = f"batch-{Batch._sequence}"
        self.name = name or default_name
        self.enabled = bool(enabled)
        self.mixed = bool(mixed)
        self._reserve_short_slot = reserve_short_slot
        self._force_short_reservation = bool(
            mixed or reserve_short_slot is True
        )
        self._closed = False
        self._cancelled = False
        self._condition = threading.Condition(threading.RLock())
        self._jobs = []
        self._tasks = []
        self._workers = []
        self._short_workers = []
        self._short_slot_reserved = False
        self._worker_cap = 0
        self._task_sequence = 0
        self._timeline_export_path = None

        if not self.enabled:
            self.controller = None
            return

        if controller is None:
            controller = get_concurrency_controller()
            if cpu_count is not None:
                try:
                    controller.set_cpu_count(cpu_count)
                except ValueError:
                    pass
            controller.set_native_concurrency(native_concurrency)

        self.controller = controller
        self._local_limit = concurrency

        if concurrency is not None:
            controller.request_limit(
                concurrency,
                reason=f"{self.name} requested limit",
            )
        else:
            # Accessing the property establishes the machine-derived default.
            controller.limit

        if adaptive:
            ceiling = concurrency if concurrency is not None else controller.cpu_count
            controller.configure_adaptive(
                True,
                ceiling=ceiling,
                load_provider=load_provider,
                interval=adaptive_interval,
                cooldown=adaptive_cooldown,
            )

        if mixed and self._reserve_short_slot is None:
            self._reserve_short_slot = True
        if concurrency is None:
            self._worker_cap = controller.cpu_count
        else:
            self._worker_cap = min(concurrency, controller.cpu_count)
        self._apply_short_reservation()
        controller.add_listener(self._on_limit_change)

    def _apply_short_reservation(self):
        if self.controller is None or self._reserve_short_slot is None:
            return

        with self._condition:
            pending_short = self._reserve_short_slot and (
                self._force_short_reservation
                or any(
                    task.task_class == _SHORT and not task.done
                    for task in self._tasks
                )
            )

        if pending_short and not self._short_slot_reserved:
            self.controller.reserve_short_task_slot(self)
            self._short_slot_reserved = True
        elif not pending_short and self._short_slot_reserved:
            self.controller.release_short_task_slot(self)
            self._short_slot_reserved = False

    def _on_limit_change(self):
        self._ensure_workers()

    def _next_task_sequence(self):
        with self._condition:
            self._task_sequence += 1
            return self._task_sequence

    def _make_task(self, function, args, kwargs, task_class):
        task_class = _normalize_task_class(task_class)
        sequence = self._next_task_sequence()
        return BatchTask(
            function,
            args,
            kwargs,
            task_class,
            self.name,
            sequence,
        )

    def _ensure_workers(self):
        if not self.enabled:
            return

        with self._condition:
            if self._closed:
                return

            target = max(1, self._worker_cap)
            while len(self._workers) < target:
                worker = threading.Thread(
                    target=self._worker_loop,
                    name=f"pyvips-{self.name}-worker-{len(self._workers) + 1}",
                    daemon=True,
                )
                self._workers.append(worker)
                worker.start()

            if self._reserve_short_slot and not self._short_workers:
                worker = threading.Thread(
                    target=self._worker_loop,
                    kwargs={"short_only": True},
                    name=f"pyvips-{self.name}-short-worker",
                    daemon=True,
                )
                self._short_workers.append(worker)
                worker.start()

    def _worker_loop(self, short_only=False):
        while True:
            task = self._take_task(short_only=short_only)
            if task is None:
                return

            if task._cancel_event.is_set():
                task._cancel()
                self._forget_task(task)
                continue

            lease = None
            failed = False
            try:
                lease = self.controller.acquire(
                    batch=self.name,
                    task=task.id,
                    task_class=task.task_class,
                    cancel_event=task._cancel_event,
                )
                if not task._begin():
                    continue

                try:
                    value = task.function(*task.args, **task.kwargs)
                except BaseException as exc:
                    failed = True
                    task._finish(exception=exc)
                else:
                    task._finish(value=value)
            except TaskCancelled:
                task._cancel()
            except BaseException as exc:
                failed = True
                task._finish(exception=exc)
            finally:
                if lease is not None:
                    lease.release(failed=failed)
                self._forget_task(task)

    def _take_task(self, short_only=False):
        with self._condition:
            while True:
                candidates = self._jobs
                if short_only:
                    candidates = [
                        task for task in self._jobs
                        if task.task_class == _SHORT
                    ]
                if candidates:
                    task = min(
                        candidates,
                        key=lambda item: (
                            _TASK_CLASS_PRIORITY[item.task_class],
                            item._enqueued_at,
                        ),
                    )
                    self._jobs.remove(task)
                    return task

                if self._closed:
                    return None

                self._condition.wait()

    def _run_direct(self, task):
        if not task._begin():
            return task
        try:
            value = task.function(*task.args, **task.kwargs)
        except BaseException as exc:
            task._finish(exception=exc)
        else:
            task._finish(value=value)
        return task

    def _forget_task(self, task):
        with self._condition:
            try:
                self._tasks.remove(task)
            except ValueError:
                pass
        self._apply_short_reservation()

    def submit(self, function, *args, task_class="normal", **kwargs):
        """Submit one callable and return its :class:`BatchTask` handle."""

        task = self._make_task(function, args, kwargs, task_class)
        if not self.enabled:
            return self._run_direct(task)

        with self._condition:
            if self._closed:
                raise RuntimeError("batch is closed")
            if self._cancelled:
                raise RuntimeError("batch was cancelled")
            self._jobs.append(task)
            self._tasks.append(task)
            self._condition.notify_all()

        self._apply_short_reservation()
        self._ensure_workers()
        return task

    def submit_many(self, function, items, task_class="normal"):
        """Submit many callables at once, preserving short-task priority."""

        items = list(items)
        if callable(task_class):
            classes = [
                _normalize_task_class(task_class(item))
                for item in items
            ]
        elif isinstance(task_class, (list, tuple)):
            if len(task_class) != len(items):
                raise ValueError("task_class sequence must match items")
            classes = [_normalize_task_class(value) for value in task_class]
        else:
            normalized = _normalize_task_class(task_class)
            classes = [normalized] * len(items)

        tasks = [
            self._make_task(function, (item,), {}, cls)
            for item, cls in zip(items, classes)
        ]

        if not self.enabled:
            return [self._run_direct(task) for task in tasks]

        if self._reserve_short_slot is None and (
            _SHORT in classes and _LONG in classes
        ):
            self._reserve_short_slot = True
            self._apply_short_reservation()

        with self._condition:
            if self._closed:
                raise RuntimeError("batch is closed")
            if self._cancelled:
                raise RuntimeError("batch was cancelled")
            self._jobs.extend(tasks)
            self._tasks.extend(tasks)
            self._condition.notify_all()

        self._apply_short_reservation()
        self._ensure_workers()
        return tasks

    def map(self, function, items, task_class="normal"):
        """Run ``function`` for every item and return a list of task handles.

        A failing item is captured on its handle; it does not stop the rest of
        the batch and it cannot leak a concurrency slot.
        """

        tasks = self.submit_many(function, items, task_class=task_class)
        for task in tasks:
            task.wait()
        return tasks

    def cancel(self):
        """Cancel not-yet-started work while allowing started work to finish."""

        if not self.enabled:
            return 0

        cancelled_events = []
        with self._condition:
            self._cancelled = True
            for task in list(self._tasks):
                if task._cancel():
                    cancelled_events.append(task._cancel_event)
            self._jobs = [task for task in self._jobs if not task.done]
            self._condition.notify_all()

        for cancel_event in cancelled_events:
            self.controller.cancel_waiting(cancel_event)
        self._apply_short_reservation()
        return len(cancelled_events)

    def set_concurrency(self, limit, reason="runtime adjustment"):
        """Adjust the shared limit and this batch's local worker cap."""

        limit = _validate_positive_int(limit, "concurrency")
        if not self.enabled:
            raise RuntimeError("batch is disabled")

        with self._condition:
            self._local_limit = limit
            self._worker_cap = min(limit, self.controller.cpu_count)

        self.controller.set_limit(limit, reason=f"{self.name}: {reason}")
        self._ensure_workers()

    def snapshot(self):
        if self.controller is None:
            return {
                "enabled": False,
                "batch": self.name,
                "limit": None,
                "in_use": 0,
                "queued": 0,
            }
        result = self.controller.snapshot()
        result["batch"] = self.name
        result["enabled"] = True
        return result

    def timeline(self, event=None, limit=None):
        if self.controller is None:
            return []
        return self.controller.timeline(event=event, limit=limit)

    def export_timeline(self, path=None, indent=2):
        if self.controller is None:
            return "[]"
        return self.controller.export_timeline(path=path, indent=indent)

    def close(self, wait=True):
        """Close the batch, optionally waiting for already-started work."""

        if not self.enabled:
            self._closed = True
            return

        with self._condition:
            if self._closed:
                return
            self._closed = True
            self._cancelled = True
            cancelled_events = []
            for task in list(self._tasks):
                if task._cancel():
                    cancelled_events.append(task._cancel_event)
            self._jobs = []
            self._condition.notify_all()
            workers = list(self._workers) + list(self._short_workers)

        for cancel_event in cancelled_events:
            self.controller.cancel_waiting(cancel_event)

        self.controller.remove_listener(self._on_limit_change)
        self.controller.release_short_task_slot(self)
        self._short_slot_reserved = False

        if wait:
            for worker in workers:
                worker.join()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close(wait=True)
        return False


def run_batch(
    function,
    items,
    concurrency=None,
    *,
    max_concurrency=None,
    enabled=None,
    adaptive=False,
    task_class="normal",
    controller=None,
    cpu_count=None,
    native_concurrency=True,
    mixed=False,
    reserve_short_slot=None,
    load_provider=None,
    adaptive_interval=0.5,
    adaptive_cooldown=0.25,
    name=None,
):
    """Convenience wrapper around :class:`Batch` for one function/iterable."""

    batch = Batch(
        concurrency=concurrency,
        max_concurrency=max_concurrency,
        enabled=enabled,
        adaptive=adaptive,
        controller=controller,
        cpu_count=cpu_count,
        native_concurrency=native_concurrency,
        mixed=mixed,
        reserve_short_slot=reserve_short_slot,
        load_provider=load_provider,
        adaptive_interval=adaptive_interval,
        adaptive_cooldown=adaptive_cooldown,
        name=name,
    )
    try:
        return batch.map(function, items, task_class=task_class)
    finally:
        batch.close(wait=True)


__all__ = [
    "Batch",
    "BatchTask",
    "ConcurrencyController",
    "TaskCancelled",
    "TaskTimedOut",
    "batch_concurrency_snapshot",
    "batch_concurrency_timeline",
    "default_concurrency",
    "effective_cpu_count",
    "export_batch_concurrency_timeline",
    "get_batch_concurrency",
    "get_concurrency_controller",
    "reset_batch_concurrency",
    "run_batch",
    "set_batch_concurrency",
]

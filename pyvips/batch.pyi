import threading
from collections.abc import Callable, Iterable, Sequence
from typing import Any


class TaskCancelled(Exception): ...
class TaskTimedOut(TimeoutError): ...

def effective_cpu_count() -> int: ...
def default_concurrency(cpu_count: int | None = None) -> int: ...

class ConcurrencyController:
    def __init__(
        self,
        cpu_count: int | None = None,
        native_concurrency: bool = True,
        max_events: int = 4096,
    ) -> None: ...
    @property
    def cpu_count(self) -> int: ...
    @property
    def limit(self) -> int: ...
    @property
    def base_limit(self) -> int: ...
    @property
    def max_limit(self) -> int: ...
    @property
    def adaptive(self) -> bool: ...
    def set_cpu_count(self, cpu_count: int) -> None: ...
    def native_threads_for_limit(self, limit: int | None = None) -> int: ...
    def set_native_concurrency(self, enabled: bool) -> None: ...
    def set_limit(self, limit: int, reason: str = "manual") -> int: ...
    def request_limit(self, limit: int, reason: str = "batch") -> int: ...
    def configure_adaptive(
        self,
        enabled: bool = True,
        ceiling: int | None = None,
        load_provider: Callable[[], float | None] | None = None,
        interval: float = 0.5,
        cooldown: float = 0.25,
    ) -> None: ...
    def adapt(self, pressure: float) -> int: ...
    def set_short_task_reservation(self, slots: int) -> None: ...
    def add_listener(self, listener: Callable[[], None]) -> None: ...
    def remove_listener(self, listener: Callable[[], None]) -> None: ...
    def acquire(
        self,
        batch: str | None = None,
        task: str | None = None,
        task_class: str = "normal",
        timeout: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> Any: ...
    def cancel_waiting(self, cancel_event: threading.Event) -> None: ...
    def snapshot(self) -> dict[str, Any]: ...
    def timeline(
        self, event: str | None = None, limit: int | None = None
    ) -> list[dict[str, Any]]: ...
    def export_timeline(self, path: str | None = None, indent: int = 2) -> str: ...
    def reset(self) -> None: ...

def get_concurrency_controller() -> ConcurrencyController: ...
def reset_batch_concurrency(
    cpu_count: int | None = None, native_concurrency: bool = True
) -> ConcurrencyController: ...
def set_batch_concurrency(limit: int, reason: str = "manual") -> int: ...
def get_batch_concurrency() -> int: ...
def batch_concurrency_snapshot() -> dict[str, Any]: ...
def batch_concurrency_timeline(
    event: str | None = None, limit: int | None = None
) -> list[dict[str, Any]]: ...
def export_batch_concurrency_timeline(
    path: str | None = None, indent: int = 2
) -> str: ...

class BatchTask:
    id: str
    name: str
    task_class: str
    value: Any
    exception: BaseException | None
    status: str
    @property
    def ok(self) -> bool: ...
    @property
    def done(self) -> bool: ...
    @property
    def queue_wait_ms(self) -> float | None: ...
    @property
    def elapsed_ms(self) -> float | None: ...
    def wait(self, timeout: float | None = None) -> BatchTask: ...
    def result(
        self, timeout: float | None = None, raise_error: bool = True
    ) -> Any: ...

class Batch:
    name: str
    enabled: bool
    mixed: bool
    controller: ConcurrencyController | None
    def __init__(
        self,
        concurrency: int | None = None,
        max_concurrency: int | None = None,
        *,
        enabled: bool | None = None,
        adaptive: bool = False,
        controller: ConcurrencyController | None = None,
        cpu_count: int | None = None,
        native_concurrency: bool = True,
        mixed: bool = False,
        reserve_short_slot: bool | None = None,
        load_provider: Callable[[], float | None] | None = None,
        adaptive_interval: float = 0.5,
        adaptive_cooldown: float = 0.25,
        name: str | None = None,
    ) -> None: ...
    def submit(
        self,
        function: Callable[..., Any],
        *args: Any,
        task_class: str = "normal",
        **kwargs: Any,
    ) -> BatchTask: ...
    def submit_many(
        self,
        function: Callable[[Any], Any],
        items: Iterable[Any],
        task_class: (
            str
            | Callable[[Any], str]
            | Sequence[str]
        ) = "normal",
    ) -> list[BatchTask]: ...
    def map(
        self,
        function: Callable[[Any], Any],
        items: Iterable[Any],
        task_class: (
            str
            | Callable[[Any], str]
            | Sequence[str]
        ) = "normal",
    ) -> list[BatchTask]: ...
    def cancel(self) -> int: ...
    def set_concurrency(
        self, limit: int, reason: str = "runtime adjustment"
    ) -> None: ...
    def snapshot(self) -> dict[str, Any]: ...
    def timeline(
        self, event: str | None = None, limit: int | None = None
    ) -> list[dict[str, Any]]: ...
    def export_timeline(self, path: str | None = None, indent: int = 2) -> str: ...
    def close(self, wait: bool = True) -> None: ...
    def __enter__(self) -> Batch: ...
    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool: ...

def run_batch(
    function: Callable[[Any], Any],
    items: Iterable[Any],
    concurrency: int | None = None,
    *,
    max_concurrency: int | None = None,
    enabled: bool | None = None,
    adaptive: bool = False,
    task_class: str | Callable[[Any], str] | Sequence[str] = "normal",
    controller: ConcurrencyController | None = None,
    cpu_count: int | None = None,
    native_concurrency: bool = True,
    mixed: bool = False,
    reserve_short_slot: bool | None = None,
    load_provider: Callable[[], float | None] | None = None,
    adaptive_interval: float = 0.5,
    adaptive_cooldown: float = 0.25,
    name: str | None = None,
) -> list[BatchTask]: ...

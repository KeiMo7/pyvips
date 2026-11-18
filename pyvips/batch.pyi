from collections.abc import Iterable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any, Callable

from .vimage import Image

BatchInput = str | Path | Mapping[str, Any] | Iterable[Any]

class BatchStageError(Exception):
    stage: str

    def __init__(self, stage: str, message: str | None = ...) -> None: ...

class BatchItem:
    item_id: str
    index: int
    input: str
    filename: str
    output: str | None
    metadata: Mapping[str, Any]

    @property
    def source(self) -> str: ...

class BatchItemResult:
    item_id: str
    index: int
    input: str
    filename: str
    output: str | None
    status: str
    stage: str | None
    exception_type: str | None
    exception_module: str | None
    error_message: str | None
    error_detail: str | None
    attempts: int
    retries: int
    duration_seconds: float
    wall_duration_seconds: float
    stage_durations_seconds: dict[str, float]
    resumed: bool
    started_at: str | None
    finished_at: str | None
    metadata: Mapping[str, Any]

    def to_dict(self) -> dict[str, Any]: ...
    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> BatchItemResult: ...

class BatchResultStream(Sequence[BatchItemResult]):
    path: str | None

    def __init__(self, path: str | None, count: int) -> None: ...
    def __len__(self) -> int: ...
    def __iter__(self) -> Iterator[BatchItemResult]: ...
    def __getitem__(self, index: int | slice) -> Any: ...

class BatchSummary:
    batch_id: str
    run_id: str
    status: str
    total_count: int
    success_count: int
    failure_count: int
    skipped_count: int
    processed_count: int
    retry_count: int
    attempts_count: int
    failure_types: dict[str, int]
    failure_stages: dict[str, int]
    duration_stats: dict[str, Any]
    slowest_items: list[dict[str, Any]]
    elapsed_seconds: float
    checkpoint_path: str
    results_path: str
    results: BatchResultStream
    machine_summary_path: str | None
    human_summary_path: str | None
    resumed: bool
    checkpoint_recovered: bool
    warnings: list[str]

    @property
    def items(self) -> BatchResultStream: ...
    def iter_results(self) -> Iterator[BatchItemResult]: ...
    def failed_items(self) -> Iterator[BatchItemResult]: ...
    def to_dict(self) -> dict[str, Any]: ...

def process_batch(
    inputs: BatchInput,
    operation: str | Callable[..., Any],
    *,
    output_dir: str | Path | None = ...,
    output_suffix: str | None = ...,
    operation_args: Sequence[Any] = ...,
    operation_kwargs: Mapping[str, Any] | None = ...,
    load_options: Mapping[str, Any] | None = ...,
    save_options: Mapping[str, Any] | None = ...,
    retries: int = ...,
    retry_interval: float = ...,
    state_dir: str | Path | None = ...,
    checkpoint_path: str | Path | None = ...,
    summary_dir: str | Path | None = ...,
    batch_id: str | None = ...,
    run_id: str | None = ...,
    resume: bool = ...,
    retry_failed: bool = ...,
    on_result: Callable[[BatchItemResult], Any] | None = ...,
) -> BatchSummary: ...

batch_process = process_batch
run_batch = process_batch

class BatchProcessor:
    operation: str | Callable[..., Any]
    options: dict[str, Any]

    def __init__(
        self, operation: str | Callable[..., Any], **options: Any
    ) -> None: ...
    def run(self, inputs: BatchInput, **options: Any) -> BatchSummary: ...

__all__: list[str]

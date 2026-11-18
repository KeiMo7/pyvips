from collections.abc import Iterable
from pathlib import Path
from typing import Any

class ColorBatchError(ValueError): ...
class ColorBatchDisabledError(ColorBatchError): ...
class ColorBatchLedgerError(ColorBatchError): ...

class BatchItem:
    image_id: str
    path: str | Path
    assumption: str | Path | None
    output_path: str | Path | None
    def __init__(
        self,
        image_id: str,
        path: str | Path,
        assumption: str | Path | None = ...,
        output_path: str | Path | None = ...,
    ) -> None: ...

class ProfileInspection:
    status: str
    source_space: str | None
    colour_space_signature: str | None
    description: str | None
    declared_size: int | None
    reason: str | None
    @property
    def usable(self) -> bool: ...
    def as_dict(self) -> dict[str, Any]: ...

class ColorBatchResult:
    records: list[dict[str, Any]]
    elapsed_seconds: float
    interrupted: bool
    ledger_path: str | None
    @property
    def completed(self) -> list[dict[str, Any]]: ...
    @property
    def failed(self) -> list[dict[str, Any]]: ...
    @property
    def duplicates(self) -> list[dict[str, Any]]: ...
    @property
    def incomplete(self) -> list[dict[str, Any]]: ...
    def conclusion_counts(self) -> dict[str, int]: ...
    def as_dict(self) -> dict[str, Any]: ...

def inspect_icc_profile(profile: bytes | None) -> ProfileInspection: ...

def process_batch(
    items: Iterable[Any],
    *,
    target_space: str | Any | None = ...,
    target_profile: str | Path | None = ...,
    assumptions: Any = ...,
    default_assumption: Any = ...,
    enabled: bool = ...,
    output_dir: str | Path | None = ...,
    ledger_path: str | Path | None = ...,
    batch_id: str | None = ...,
    intent: str = ...,
    black_point_compensation: bool = ...,
    resume: bool = ...,
) -> ColorBatchResult: ...

def convert_batch(*args: Any, **kwargs: Any) -> ColorBatchResult: ...
def colourspace_batch(*args: Any, **kwargs: Any) -> ColorBatchResult: ...
def color_space_batch(*args: Any, **kwargs: Any) -> ColorBatchResult: ...

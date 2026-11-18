"""Batch image processing with retries, checkpoints and summaries.

This module is deliberately separate from the single-image API.  Existing
``Image`` calls keep their current behaviour; callers opt in to batch
processing by calling :func:`process_batch`.
"""

from __future__ import annotations

import csv
import hashlib
import json
import logging
import os
import tempfile
import time
import uuid
import warnings
from collections import Counter
from collections.abc import Mapping as MappingABC
from collections.abc import Sequence as SequenceABC
from dataclasses import dataclass, field
from datetime import datetime, timezone
from itertools import chain
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional
from typing import Sequence, Tuple, Union

import pyvips

logger = logging.getLogger(__name__)

_CHECKPOINT_SCHEMA_VERSION = 1
_SUMMARY_SCHEMA_VERSION = 1
_SUCCESS = "success"
_FAILURE = "failure"


class BatchStageError(Exception):
    """Attach a processing stage to an exception raised by a batch callback.

    A callback normally reports the ``process`` stage.  If loading or writing
    happens inside the callback, it can raise this exception with ``"load"``
    or ``"write"`` to make the failure location explicit.
    """

    def __init__(self, stage: str, message: Optional[str] = None):
        self.stage = stage
        super().__init__(message or stage)


@dataclass(frozen=True)
class BatchItem:
    """One normalized item passed to a batch operation callback."""

    item_id: str
    index: int
    input: str
    filename: str
    output: Optional[str] = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @property
    def source(self) -> str:
        """Alias for :attr:`input`."""

        return self.input


@dataclass
class BatchItemResult:
    """Final result for one input item."""

    item_id: str
    index: int
    input: str
    filename: str
    output: Optional[str]
    status: str
    stage: Optional[str] = None
    exception_type: Optional[str] = None
    exception_module: Optional[str] = None
    error_message: Optional[str] = None
    error_detail: Optional[str] = None
    attempts: int = 0
    retries: int = 0
    duration_seconds: float = 0.0
    wall_duration_seconds: float = 0.0
    stage_durations_seconds: Dict[str, float] = field(default_factory=dict)
    resumed: bool = False
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-serializable representation."""

        return {
            "item_id": self.item_id,
            "index": self.index,
            "input": self.input,
            "filename": self.filename,
            "output": self.output,
            "status": self.status,
            "stage": self.stage,
            "exception_type": self.exception_type,
            "exception_module": self.exception_module,
            "error_message": self.error_message,
            "error_detail": self.error_detail,
            "attempts": self.attempts,
            "retries": self.retries,
            "duration_seconds": self.duration_seconds,
            "wall_duration_seconds": self.wall_duration_seconds,
            "stage_durations_seconds": dict(self.stage_durations_seconds),
            "resumed": self.resumed,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "metadata": _json_safe(self.metadata),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "BatchItemResult":
        """Build a result from a JSONL record."""

        return cls(
            item_id=value["item_id"],
            index=value["index"],
            input=value["input"],
            filename=value["filename"],
            output=value.get("output"),
            status=value["status"],
            stage=value.get("stage"),
            exception_type=value.get("exception_type"),
            exception_module=value.get("exception_module"),
            error_message=value.get("error_message"),
            error_detail=value.get("error_detail"),
            attempts=value.get("attempts", 0),
            retries=value.get("retries", 0),
            duration_seconds=value.get("duration_seconds", 0.0),
            wall_duration_seconds=value.get("wall_duration_seconds", 0.0),
            stage_durations_seconds=dict(
                value.get("stage_durations_seconds") or {}
            ),
            resumed=value.get("resumed", False),
            started_at=value.get("started_at"),
            finished_at=value.get("finished_at"),
            metadata=value.get("metadata") or {},
        )


class BatchResultStream(SequenceABC):
    """A lazy, re-iterable view of the per-item JSONL result file."""

    def __init__(self, path: Optional[str], count: int):
        self.path = path
        self._count = count

    def __len__(self) -> int:
        return self._count

    def __iter__(self) -> Iterator[BatchItemResult]:
        if not self.path:
            return
        with open(self.path, "r", encoding="utf-8") as stream:
            for line in stream:
                if line.strip():
                    yield BatchItemResult.from_dict(json.loads(line))

    def __getitem__(self, index: Union[int, slice]) -> Any:
        if isinstance(index, slice):
            return list(self)[index]

        if index < 0:
            index += self._count
        if index < 0 or index >= self._count:
            raise IndexError(index)

        for position, result in enumerate(self):
            if position == index:
                return result
        raise IndexError(index)


@dataclass
class BatchSummary:
    """Aggregate result returned by :func:`process_batch`.

    ``results`` is a lazy sequence backed by the JSONL result file, so callers
    can inspect all item records without the batch keeping them in memory.
    """

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
    failure_types: Dict[str, int]
    failure_stages: Dict[str, int]
    duration_stats: Dict[str, Any]
    slowest_items: List[Dict[str, Any]]
    elapsed_seconds: float
    checkpoint_path: str
    results_path: str
    results: BatchResultStream
    machine_summary_path: Optional[str] = None
    human_summary_path: Optional[str] = None
    resumed: bool = False
    checkpoint_recovered: bool = False
    warnings: List[str] = field(default_factory=list)

    @property
    def items(self) -> BatchResultStream:
        """Alias for :attr:`results`."""

        return self.results

    def iter_results(self) -> Iterator[BatchItemResult]:
        """Iterate item results from disk."""

        return iter(self.results)

    def failed_items(self) -> Iterator[BatchItemResult]:
        """Iterate only final failed items."""

        return (item for item in self.results if item.status == _FAILURE)

    def to_dict(self) -> Dict[str, Any]:
        """Return the machine-readable summary."""

        return {
            "schema_version": _SUMMARY_SCHEMA_VERSION,
            "batch_id": self.batch_id,
            "run_id": self.run_id,
            "status": self.status,
            "resumed": self.resumed,
            "checkpoint_recovered": self.checkpoint_recovered,
            "totals": {
                "total": self.total_count,
                "success": self.success_count,
                "failure": self.failure_count,
                "skipped": self.skipped_count,
                "processed": self.processed_count,
                "retries": self.retry_count,
                "attempts": self.attempts_count,
            },
            "failure_types": dict(self.failure_types),
            "failure_stages": dict(self.failure_stages),
            "duration_seconds": dict(self.duration_stats),
            "slowest_items": list(self.slowest_items),
            "elapsed_seconds": self.elapsed_seconds,
            "paths": {
                "checkpoint": self.checkpoint_path,
                "results": self.results_path,
                "summary_json": self.machine_summary_path,
                "summary_text": self.human_summary_path,
            },
            "warnings": list(self.warnings),
        }


@dataclass
class _PreparedItems:
    path: str
    count: int
    fingerprint: str
    item_ids: List[str]
    batch_id: str


@dataclass
class _CheckpointState:
    records: Dict[str, Dict[str, Any]]
    resumed: bool = False
    recovered: bool = False
    warnings: List[str] = field(default_factory=list)


@dataclass
class _ItemOutcome:
    status: str
    output: Optional[str]
    stage: Optional[str]
    exception_type: Optional[str]
    exception_module: Optional[str]
    error_message: Optional[str]
    error_detail: Optional[str]
    attempts: int
    retries: int
    duration: float
    wall_duration: float
    stage_durations: Dict[str, float]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, MappingABC):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    return repr(value)


def _json_dumps(value: Any) -> str:
    return json.dumps(_json_safe(value), ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"))


def _filename_from_input(value: str) -> str:
    without_options = value.split("[", 1)[0]
    if "://" in without_options:
        without_options = without_options.split("://", 1)[1]
    return os.path.basename(without_options.rstrip("/\\")) or without_options


def _canonical_input(value: str) -> str:
    if "://" in value or value.startswith("memory:"):
        return value
    return os.path.abspath(os.path.normpath(os.path.expanduser(value)))


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _safe_component(value: str, prefix: str = "batch") -> str:
    if value and len(value) <= 80 and all(
            char.isalnum() or char in "._-" for char in value):
        return value
    return "{}-{}".format(prefix, _sha256_text(value)[:32])


def _mapping_get(mapping: Mapping[str, Any], names: Sequence[str]) -> Any:
    for name in names:
        if name in mapping:
            return mapping[name]
    return None


def _normalize_item(raw: Any, index: int,
                    generated_counts: Dict[str, int]) -> BatchItem:
    explicit_id = None
    output = None
    metadata: Dict[str, Any] = {}

    if isinstance(raw, BatchItem):
        source = raw.input
        explicit_id = raw.item_id
        output = raw.output
        metadata = dict(raw.metadata)
    elif isinstance(raw, MappingABC):
        source = _mapping_get(raw, ("input", "source", "filename", "path",
                                    "file"))
        explicit_id = _mapping_get(raw, ("id", "item_id", "key"))
        output = _mapping_get(raw, ("output", "destination", "target"))
        metadata = {
            str(key): value for key, value in raw.items()
            if key not in {
                "input", "source", "filename", "path", "file", "id",
                "item_id", "key", "output", "destination", "target"
            }
        }
    elif isinstance(raw, (str, Path)):
        source = raw
    elif isinstance(raw, (tuple, list)):
        if len(raw) == 2:
            source, output = raw
        elif len(raw) == 3:
            explicit_id, source, output = raw
        else:
            raise ValueError(
                "batch item sequences must have 2 or 3 values")
    else:
        raise TypeError(
            "unsupported batch item type: {}".format(type(raw).__name__))

    if source is None:
        raise ValueError("batch item {} has no input path".format(index))
    source = str(source)

    if explicit_id is None:
        canonical = _canonical_input(source)
        occurrence = generated_counts.get(canonical, 0) + 1
        generated_counts[canonical] = occurrence
        id_input = canonical if occurrence == 1 else "{}\0{}".format(
            canonical, occurrence)
        item_id = _sha256_text(id_input)
    else:
        item_id = str(explicit_id)
        if not item_id:
            raise ValueError(
                "batch item {} has an empty item id".format(index))

    output_path = None if output is None else str(output)
    return BatchItem(
        item_id=item_id,
        index=index,
        input=source,
        filename=_filename_from_input(source),
        output=output_path,
        metadata=metadata,
    )


def _iter_manifest(path: Path) -> Iterator[Any]:
    suffix = path.suffix.lower()

    if suffix in (".jsonl", ".ndjson"):
        with path.open("r", encoding="utf-8") as stream:
            for line in stream:
                if line.strip():
                    yield json.loads(line)
        return

    if suffix == ".csv":
        with path.open("r", encoding="utf-8", newline="") as stream:
            yield from csv.DictReader(stream)
        return

    if suffix in (".txt", ".list"):
        with path.open("r", encoding="utf-8") as stream:
            for line in stream:
                value = line.strip()
                if value:
                    yield value
        return

    with path.open("r", encoding="utf-8") as stream:
        value = json.load(stream)
    if isinstance(value, MappingABC) and "items" in value:
        value = value["items"]
    if isinstance(value, (list, tuple)):
        yield from value
    else:
        yield value


def _input_values(inputs: Union[str, Path, Mapping[str, Any], Iterable[Any]]
                  ) -> Iterator[Any]:
    if isinstance(inputs, (str, Path)):
        path = Path(inputs)
        yield from _iter_manifest(path)
    elif isinstance(inputs, MappingABC):
        yield inputs
    else:
        yield from inputs


def _operation_fingerprint(operation: Any, operation_args: Sequence[Any],
                           operation_kwargs: Mapping[str, Any],
                           load_options: Mapping[str, Any],
                           save_options: Mapping[str, Any],
                           output_suffix: Optional[str]) -> Dict[str, Any]:
    if callable(operation):
        target = getattr(operation, "func", operation)
        target = getattr(target, "__func__", target)
        module = getattr(target, "__module__", type(target).__module__)
        qualname = getattr(
            target, "__qualname__",
            getattr(target, "__name__", type(target).__qualname__))
        code = getattr(target, "__code__", None)
        code_hash = None
        if code is not None:
            code_hash = _sha256_text(repr((
                code.co_code,
                code.co_names,
                code.co_varnames,
            )))
        operation_value = "callable:{}.{}:{}".format(
            module, qualname, code_hash or "")
        partial_args = None
        partial_kwargs = None
        if hasattr(operation, "func"):
            partial_args = _json_safe(getattr(operation, "args", ()))
            partial_kwargs = _json_safe(
                getattr(operation, "keywords", {}) or {})
    else:
        operation_value = str(operation)
        partial_args = None
        partial_kwargs = None

    return {
        "operation": operation_value,
        "partial_args": partial_args,
        "partial_kwargs": partial_kwargs,
        "operation_args": _json_safe(operation_args),
        "operation_kwargs": _json_safe(operation_kwargs),
        "load_options": _json_safe(load_options),
        "save_options": _json_safe(save_options),
        "output_suffix": output_suffix,
    }


def _resolve_output(item: BatchItem, output_dir: Optional[str],
                    output_suffix: Optional[str]) -> Optional[str]:
    if item.output is not None:
        if output_dir and not os.path.isabs(item.output):
            return os.path.join(output_dir, item.output)
        return item.output

    if output_dir is None:
        return None

    suffix = output_suffix
    if suffix is None:
        suffix = Path(item.input.split("[", 1)[0]).suffix
    if not suffix:
        return None
    if not suffix.startswith("."):
        suffix = "." + suffix

    stem = _sha256_text(item.item_id)[:32]
    return os.path.join(output_dir, stem + suffix)


def _prepare_items(inputs: Union[str, Path, Mapping[str, Any], Iterable[Any]],
                   operation: Any, operation_args: Sequence[Any],
                   operation_kwargs: Mapping[str, Any],
                   load_options: Mapping[str, Any],
                   save_options: Mapping[str, Any],
                   output_dir: Optional[str],
                   output_suffix: Optional[str],
                   batch_id: Optional[str]) -> _PreparedItems:
    manifest_identity = None
    if isinstance(inputs, (str, Path)):
        manifest_path = Path(inputs)
        if not manifest_path.exists():
            raise FileNotFoundError(str(manifest_path))
        manifest_identity = _canonical_input(str(manifest_path))

    operation_fingerprint = _operation_fingerprint(
        operation, operation_args, operation_kwargs, load_options,
        save_options, output_suffix)
    input_digest = hashlib.sha256()
    input_digest.update(_json_dumps({
        "checkpoint_schema": _CHECKPOINT_SCHEMA_VERSION,
    }).encode("utf-8"))
    digest = hashlib.sha256()
    digest.update(_json_dumps({
        "checkpoint_schema": _CHECKPOINT_SCHEMA_VERSION,
        "operation": operation_fingerprint,
    }).encode("utf-8"))

    spool = tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", suffix=".jsonl", delete=False)
    generated_counts: Dict[str, int] = {}
    seen_ids: Dict[str, int] = {}
    item_ids: List[str] = []
    count = 0

    try:
        for index, raw in enumerate(_input_values(inputs)):
            item = _normalize_item(raw, index, generated_counts)
            item = BatchItem(
                item_id=item.item_id,
                index=item.index,
                input=item.input,
                filename=item.filename,
                output=_resolve_output(item, output_dir, output_suffix),
                metadata=item.metadata,
            )

            if item.item_id in seen_ids:
                raise ValueError(
                    "duplicate batch item id {!r} at indexes {} and {}".
                    format(item.item_id, seen_ids[item.item_id], index))
            seen_ids[item.item_id] = index

            line = _json_dumps({
                "item_id": item.item_id,
                "index": item.index,
                "input": item.input,
                "filename": item.filename,
                "output": item.output,
                "metadata": item.metadata,
            })
            spool.write(line + "\n")
            input_digest.update(line.encode("utf-8"))
            input_digest.update(b"\n")
            digest.update(line.encode("utf-8"))
            digest.update(b"\n")
            item_ids.append(item.item_id)
            count += 1
    finally:
        spool.close()

    fingerprint = digest.hexdigest()

    if batch_id is None:
        if manifest_identity is not None:
            batch_id = "manifest-{}".format(
                _sha256_text(manifest_identity)[:32])
        else:
            batch_id = input_digest.hexdigest()[:32]
    else:
        batch_id = str(batch_id)

    return _PreparedItems(
        path=spool.name,
        count=count,
        fingerprint=fingerprint,
        item_ids=item_ids,
        batch_id=batch_id,
    )


def _iter_prepared(path: str) -> Iterator[BatchItem]:
    with open(path, "r", encoding="utf-8") as stream:
        for line in stream:
            if not line.strip():
                continue
            value = json.loads(line)
            yield BatchItem(
                item_id=value["item_id"],
                index=value["index"],
                input=value["input"],
                filename=value["filename"],
                output=value.get("output"),
                metadata=value.get("metadata") or {},
            )


def _path_for_run(directory: str, prefix: str, run_id: str,
                  suffix: str) -> str:
    os.makedirs(directory, exist_ok=True)
    candidate = os.path.join(directory, "{}-{}{}".format(
        prefix, run_id, suffix))
    counter = 1
    while os.path.exists(candidate):
        candidate = os.path.join(directory, "{}-{}-{}{}".format(
            prefix, run_id, counter, suffix))
        counter += 1
    return candidate


def _archive_checkpoint(path: str, run_id: str, reason: str) -> Optional[str]:
    if not os.path.exists(path):
        return None
    directory = os.path.dirname(path) or "."
    target = _path_for_run(
        directory, Path(path).stem + "-" + reason, run_id, ".jsonl")
    try:
        os.replace(path, target)
        return target
    except OSError:
        logger.exception("could not archive checkpoint %s", path)
        return None


def _load_checkpoint(path: str, prepared: _PreparedItems,
                     resume: bool) -> _CheckpointState:
    state = _CheckpointState(records={})

    if not os.path.exists(path):
        return state

    if not resume:
        archived = _archive_checkpoint(path, uuid.uuid4().hex[:12],
                                       "superseded")
        if archived:
            state.warnings.append(
                "resume disabled; previous checkpoint archived to {}".format(
                    archived))
        return state

    header = None
    try:
        with open(path, "r", encoding="utf-8") as stream:
            for line in stream:
                if not line.strip():
                    continue
                header = json.loads(line)
                break
    except (OSError, ValueError, KeyError, TypeError) as exc:
        message = (
            "checkpoint {} header is corrupt ({}); "
            "performing a full run".format(path, exc))
        state.warnings.append(message)
        state.recovered = True
        warnings.warn(message, RuntimeWarning, stacklevel=3)
        archived = _archive_checkpoint(
            path, uuid.uuid4().hex[:12], "corrupt")
        if archived:
            state.warnings.append(
                "corrupt checkpoint archived to {}".format(archived))
        return state

    if header is None:
        message = (
            "checkpoint {} is empty; performing a full run".format(path))
        state.warnings.append(message)
        state.recovered = True
        warnings.warn(message, RuntimeWarning, stacklevel=3)
        _archive_checkpoint(path, uuid.uuid4().hex[:12], "corrupt")
        return state

    if (not isinstance(header, MappingABC) or
            header.get("type") != "header" or
            header.get("schema_version") != _CHECKPOINT_SCHEMA_VERSION):
        message = (
            "checkpoint {} header is invalid; performing a full run".format(
                path))
        state.warnings.append(message)
        state.recovered = True
        warnings.warn(message, RuntimeWarning, stacklevel=3)
        archived = _archive_checkpoint(
            path, uuid.uuid4().hex[:12], "corrupt")
        if archived:
            state.warnings.append(
                "corrupt checkpoint archived to {}".format(archived))
        return state

    if (header.get("input_fingerprint") != prepared.fingerprint or
            header.get("batch_id") != prepared.batch_id or
            header.get("item_count") != prepared.count):
        message = (
            "checkpoint {} does not match the current inputs or operation; "
            "performing a full run".format(path))
        state.warnings.append(message)
        state.recovered = True
        warnings.warn(message, RuntimeWarning, stacklevel=3)
        archived = _archive_checkpoint(
            path, uuid.uuid4().hex[:12], "stale")
        if archived:
            state.warnings.append(
                "stale checkpoint archived to {}".format(archived))
        return state

    records: Dict[str, Dict[str, Any]] = {}
    try:
        with open(path, "r", encoding="utf-8") as stream:
            header_seen = False
            for line_number, line in enumerate(stream, 1):
                if not line.strip():
                    continue
                value = json.loads(line)
                if not header_seen:
                    header_seen = True
                    continue
                if not isinstance(value, MappingABC):
                    raise ValueError(
                        "invalid checkpoint record on line {}".format(
                            line_number))
                if value.get("type") != "item":
                    raise ValueError(
                        "unexpected checkpoint record on line {}".format(
                            line_number))
                item_id = value["item_id"]
                index = value["index"]
                if value.get("status") not in (_SUCCESS, _FAILURE):
                    raise ValueError("invalid item status")
                if index < 0 or index >= prepared.count:
                    raise ValueError("checkpoint item index out of range")
                if prepared.item_ids[index] != item_id:
                    raise ValueError("checkpoint item id does not match input")
                records[item_id] = value
    except (OSError, ValueError, KeyError, TypeError) as exc:
        message = (
            "checkpoint {} is corrupt ({}); performing a full run".format(
                path, exc))
        state.warnings.append(message)
        state.recovered = True
        warnings.warn(message, RuntimeWarning, stacklevel=3)
        archived = _archive_checkpoint(
            path, uuid.uuid4().hex[:12], "corrupt")
        if archived:
            state.warnings.append(
                "corrupt checkpoint archived to {}".format(archived))
        return state

    state.records = records
    state.resumed = True
    return state


def _append_json_line(stream: Any, value: Mapping[str, Any]) -> None:
    stream.write(_json_dumps(value) + "\n")
    stream.flush()
    try:
        os.fsync(stream.fileno())
    except OSError:
        pass


def _stage_duration(stage_durations: Dict[str, float], stage: str,
                    started: float) -> None:
    stage_durations[stage] = stage_durations.get(stage, 0.0) + (
        time.perf_counter() - started)


def _execute_string_operation(
        item: BatchItem, operation: str, operation_args: Sequence[Any],
        operation_kwargs: Mapping[str, Any], load_options: Mapping[str, Any],
        save_options: Mapping[str, Any]) -> Tuple[str, Dict[str, float]]:
    if not item.output:
        raise BatchStageError(
            "prepare", "an output path is required for named operations")

    stage_durations: Dict[str, float] = {}
    image = None
    result_image = None
    operation_name = operation.lower()

    try:
        if operation_name == "thumbnail":
            started = time.perf_counter()
            try:
                result_image = pyvips.Image.thumbnail(
                    item.input, *operation_args, **operation_kwargs)
            except Exception as exc:
                raise BatchStageError("process") from exc
            _stage_duration(stage_durations, "process", started)
        else:
            started = time.perf_counter()
            try:
                image = pyvips.Image.new_from_file(
                    item.input, **load_options)
            except Exception as exc:
                raise BatchStageError("load") from exc
            _stage_duration(stage_durations, "load", started)

            if operation_name in ("convert", "copy"):
                result_image = image
            else:
                started = time.perf_counter()
                try:
                    method = getattr(image, operation)
                except AttributeError as exc:
                    raise BatchStageError(
                        "process",
                        "unknown image operation {!r}".format(
                            operation)) from exc
                try:
                    result_image = method(
                        *operation_args, **operation_kwargs)
                except Exception as exc:
                    raise BatchStageError("process") from exc
                _stage_duration(stage_durations, "process", started)

        if not isinstance(result_image, pyvips.Image):
            raise BatchStageError(
                "process",
                "operation did not return a pyvips.Image")

        started = time.perf_counter()
        try:
            parent = os.path.dirname(item.output)
            if parent:
                os.makedirs(parent, exist_ok=True)
            result_image.write_to_file(item.output, **save_options)
        except Exception as exc:
            raise BatchStageError("write") from exc
        _stage_duration(stage_durations, "write", started)
        return item.output, stage_durations
    finally:
        # Drop image references as soon as this item is finished.  In CPython
        # this immediately releases the ffi.gc handle rather than waiting for
        # the whole batch to complete.
        result_image = None
        image = None


def _execute_callback(
        item: BatchItem, operation: Any, operation_args: Sequence[Any],
        operation_kwargs: Mapping[str, Any],
        save_options: Mapping[str, Any]) -> Tuple[str, Dict[str, float]]:
    stage_durations: Dict[str, float] = {}
    started = time.perf_counter()
    try:
        value = operation(item, *operation_args, **operation_kwargs)
    except BatchStageError:
        raise
    except Exception as exc:
        raise BatchStageError("process") from exc
    _stage_duration(stage_durations, "process", started)

    output = item.output
    if isinstance(value, MappingABC):
        output = value.get("output", output)
    elif isinstance(value, (str, Path)):
        output = str(value)

    if isinstance(value, pyvips.Image):
        if not output:
            raise BatchStageError(
                "write", "callback returned an Image but no output path")
        started = time.perf_counter()
        try:
            parent = os.path.dirname(output)
            if parent:
                os.makedirs(parent, exist_ok=True)
            value.write_to_file(output, **save_options)
        except Exception as exc:
            raise BatchStageError("write") from exc
        _stage_duration(stage_durations, "write", started)
        value = None
    else:
        value = None

    if not output:
        raise BatchStageError(
            "write", "callback did not produce an output path")

    return output, stage_durations


def _execute_item(item: BatchItem, operation: Any,
                  operation_args: Sequence[Any],
                  operation_kwargs: Mapping[str, Any],
                  load_options: Mapping[str, Any],
                  save_options: Mapping[str, Any]) -> Tuple[
                      str, Dict[str, float]]:
    if callable(operation):
        return _execute_callback(
            item, operation, operation_args, operation_kwargs, save_options)
    return _execute_string_operation(
        item, str(operation), operation_args, operation_kwargs, load_options,
        save_options)


def _run_item(item: BatchItem, operation: Any, operation_args: Sequence[Any],
              operation_kwargs: Mapping[str, Any],
              load_options: Mapping[str, Any],
              save_options: Mapping[str, Any], retries: int,
              retry_interval: float) -> _ItemOutcome:
    wall_started = time.perf_counter()
    processing_duration = 0.0
    stage_durations: Dict[str, float] = {}
    last_error: Optional[BaseException] = None
    last_stage = "process"
    retry_count = 0
    attempts = 0

    for attempt in range(retries + 1):
        attempts += 1
        attempt_started = time.perf_counter()
        try:
            output, attempt_stages = _execute_item(
                item, operation, operation_args, operation_kwargs,
                load_options, save_options)
            processing_duration += time.perf_counter() - attempt_started
            for stage, duration in attempt_stages.items():
                stage_durations[stage] = (
                    stage_durations.get(stage, 0.0) + duration)
            return _ItemOutcome(
                status=_SUCCESS,
                output=output,
                stage=None,
                exception_type=None,
                exception_module=None,
                error_message=None,
                error_detail=None,
                attempts=attempts,
                retries=retry_count,
                duration=processing_duration,
                wall_duration=time.perf_counter() - wall_started,
                stage_durations=stage_durations,
            )
        except Exception as exc:
            processing_duration += time.perf_counter() - attempt_started
            last_error = exc
            if isinstance(exc, BatchStageError):
                last_stage = exc.stage
                cause = exc.__cause__
                if cause is not None:
                    last_error = cause
            else:
                last_stage = "process"

            if attempt < retries:
                retry_count += 1
                if retry_interval > 0:
                    time.sleep(retry_interval)

    error = last_error or RuntimeError("batch item failed")
    detail = None
    if isinstance(error, pyvips.Error):
        detail = error.detail
        message = error.message
    else:
        message = str(error)

    return _ItemOutcome(
        status=_FAILURE,
        output=item.output,
        stage=last_stage,
        exception_type=type(error).__name__,
        exception_module=type(error).__module__,
        error_message=message,
        error_detail=detail,
        attempts=attempts,
        retries=retry_count,
        duration=processing_duration,
        wall_duration=time.perf_counter() - wall_started,
        stage_durations=stage_durations,
    )


def _checkpoint_output_is_available(record: Mapping[str, Any],
                                    item: BatchItem) -> bool:
    output = record.get("output")
    if not output:
        return True
    if record.get("output") != item.output:
        return False
    return os.path.exists(str(output))


def _percentile(values: Sequence[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = int(round((len(ordered) - 1) * percentile))
    return ordered[rank]


def _duration_stats(durations: Sequence[float]) -> Dict[str, Any]:
    if not durations:
        return {
            "count": 0,
            "total_seconds": 0.0,
            "min_seconds": 0.0,
            "max_seconds": 0.0,
            "mean_seconds": 0.0,
            "p50_seconds": 0.0,
            "p90_seconds": 0.0,
            "p95_seconds": 0.0,
            "p99_seconds": 0.0,
        }

    total = sum(durations)
    return {
        "count": len(durations),
        "total_seconds": total,
        "min_seconds": min(durations),
        "max_seconds": max(durations),
        "mean_seconds": total / len(durations),
        "p50_seconds": _percentile(durations, 0.50),
        "p90_seconds": _percentile(durations, 0.90),
        "p95_seconds": _percentile(durations, 0.95),
        "p99_seconds": _percentile(durations, 0.99),
    }


def _format_duration(seconds: float) -> str:
    return "{:.3f}s".format(seconds)


def _human_summary(summary: BatchSummary) -> str:
    lines = [
        "pyvips batch summary",
        "====================",
        "batch: {}".format(summary.batch_id),
        "run: {}".format(summary.run_id),
        "status: {}".format(summary.status),
        "total: {}  success: {}  failure: {}  skipped: {}".format(
            summary.total_count, summary.success_count,
            summary.failure_count, summary.skipped_count),
        "processed: {}  attempts: {}  retries: {}".format(
            summary.processed_count, summary.attempts_count,
            summary.retry_count),
        "elapsed: {}".format(_format_duration(summary.elapsed_seconds)),
        "duration: min={} mean={} p50={} p95={} p99={} max={}".format(
            _format_duration(summary.duration_stats.get("min_seconds", 0.0)),
            _format_duration(summary.duration_stats.get("mean_seconds", 0.0)),
            _format_duration(summary.duration_stats.get("p50_seconds", 0.0)),
            _format_duration(summary.duration_stats.get("p95_seconds", 0.0)),
            _format_duration(summary.duration_stats.get("p99_seconds", 0.0)),
            _format_duration(summary.duration_stats.get("max_seconds", 0.0))),
    ]

    if summary.failure_types:
        lines.append("failures by type:")
        for failure_type, count in sorted(summary.failure_types.items()):
            lines.append("  {}: {}".format(failure_type, count))
    else:
        lines.append("failures by type: none")

    if summary.failure_stages:
        lines.append("failures by stage:")
        for stage, count in sorted(summary.failure_stages.items()):
            lines.append("  {}: {}".format(stage, count))

    failures = summary.failed_items()
    first_failure = next(failures, None)
    if first_failure is not None:
        lines.append("failed items:")
        for item in chain((first_failure,), failures):
            lines.append(
                "  {} ({}) stage={} type={} attempts={}: {}".format(
                    item.filename, item.item_id, item.stage,
                    item.exception_type, item.attempts,
                    item.error_message))

    if summary.slowest_items:
        lines.append("slowest items:")
        for item in summary.slowest_items:
            lines.append("  {} ({}) {}".format(
                item["filename"], item["item_id"],
                _format_duration(item["duration_seconds"])))

    if summary.warnings:
        lines.append("warnings:")
        for warning in summary.warnings:
            lines.append("  {}".format(warning))

    return "\n".join(lines) + "\n"


def _write_atomic(path: str, content: str) -> None:
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    fd, temporary = tempfile.mkstemp(
        prefix=".tmp-", suffix=Path(path).suffix, dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(content)
            stream.flush()
            try:
                os.fsync(stream.fileno())
            except OSError:
                pass
        os.replace(temporary, path)
    except Exception:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def _write_summaries(summary: BatchSummary, summary_dir: str,
                     run_id: str) -> None:
    json_path = None
    try:
        json_path = _path_for_run(
            summary_dir, "summary", run_id, ".json")
    except Exception as exc:
        summary.warnings.append(
            "could not prepare JSON summary path: {}".format(exc))

    text_path = None
    try:
        text_path = _path_for_run(
            summary_dir, "summary", run_id, ".txt")
    except Exception as exc:
        summary.warnings.append(
            "could not prepare text summary path: {}".format(exc))

    summary.machine_summary_path = json_path
    summary.human_summary_path = text_path
    if text_path:
        try:
            _write_atomic(text_path, _human_summary(summary))
        except Exception as exc:
            summary.warnings.append(
                "could not write text summary {}: {}".format(
                    text_path, exc))
            summary.human_summary_path = None
            try:
                os.unlink(text_path)
            except OSError:
                pass

    if json_path:
        try:
            _write_atomic(
                json_path,
                json.dumps(summary.to_dict(), ensure_ascii=False, indent=2) +
                "\n")
        except Exception as exc:
            summary.warnings.append(
                "could not write JSON summary {}: {}".format(
                    json_path, exc))
            summary.machine_summary_path = None
            try:
                os.unlink(json_path)
            except OSError:
                pass

def process_batch(
        inputs: Union[str, Path, Mapping[str, Any], Iterable[Any]],
        operation: Union[str, Any],
        *,
        output_dir: Optional[Union[str, Path]] = None,
        output_suffix: Optional[str] = None,
        operation_args: Sequence[Any] = (),
        operation_kwargs: Optional[Mapping[str, Any]] = None,
        load_options: Optional[Mapping[str, Any]] = None,
        save_options: Optional[Mapping[str, Any]] = None,
        retries: int = 0,
        retry_interval: float = 0.0,
        state_dir: Optional[Union[str, Path]] = None,
        checkpoint_path: Optional[Union[str, Path]] = None,
        summary_dir: Optional[Union[str, Path]] = None,
        batch_id: Optional[str] = None,
        run_id: Optional[str] = None,
        resume: bool = True,
        retry_failed: bool = False,
        on_result: Optional[Any] = None) -> BatchSummary:
    """Process a batch without aborting when individual items fail.

    ``inputs`` may be a list/tuple of items, an iterable, or a JSON, JSONL,
    CSV, or newline-delimited manifest path.  An item can be a path string,
    ``BatchItem``, ``(input, output)`` pair, ``(id, input, output)`` triple,
    or a mapping with ``input`` and optional ``id``/``output`` keys.

    ``operation`` is either a callable receiving a :class:`BatchItem`, or a
    named ``Image`` operation.  The special names ``convert``/``copy`` load
    and save an image, ``thumbnail`` calls ``Image.thumbnail``, and other
    names are called as methods on the loaded image.  A callable may return a
    ``pyvips.Image`` (the batch writes it), an output path, ``None`` if it
    already wrote the output, or a mapping containing an ``output`` key.

    Every final item result is appended to a JSONL file immediately.  The
    checkpoint is updated after each item and a successful item is skipped on
    a later resume.  All failures are reported in the returned
    :class:`BatchSummary`; processing errors are not re-raised.
    """

    if not isinstance(retries, int) or retries < 0:
        raise ValueError("retries must be a non-negative integer")
    if retry_interval < 0:
        raise ValueError("retry_interval must be non-negative")
    if not callable(operation) and not isinstance(operation, str):
        raise TypeError("operation must be a callable or an operation name")
    if on_result is not None and not callable(on_result):
        raise TypeError("on_result must be callable")

    started = time.perf_counter()
    operation_kwargs = dict(operation_kwargs or {})
    load_options = dict(load_options or {})
    save_options = dict(save_options or {})
    output_dir_value = None if output_dir is None else str(output_dir)
    operation_args = tuple(operation_args)

    prepared = _prepare_items(
        inputs, operation, operation_args, operation_kwargs, load_options,
        save_options, output_dir_value, output_suffix, batch_id)

    base_dir = os.getcwd()
    if state_dir is None:
        if output_dir_value:
            state_dir = os.path.join(output_dir_value, ".pyvips-batch")
        else:
            state_dir = os.path.join(base_dir, ".pyvips-batch")
    state_dir = str(state_dir)

    batch_component = _safe_component(prepared.batch_id)
    batch_dir = os.path.join(state_dir, "batch-" + batch_component)
    os.makedirs(batch_dir, exist_ok=True)

    if checkpoint_path is None:
        checkpoint_path = os.path.join(batch_dir, "checkpoint.jsonl")
    checkpoint_path = str(checkpoint_path)
    checkpoint_dir = os.path.dirname(checkpoint_path) or "."
    os.makedirs(checkpoint_dir, exist_ok=True)

    if summary_dir is None:
        summary_dir = batch_dir
    summary_dir = str(summary_dir)

    if run_id is None:
        run_id = "{}-{}".format(
            datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ"),
            uuid.uuid4().hex[:8])
    run_id = _safe_component(str(run_id), prefix="run")

    result_count = 0
    success_count = 0
    failure_count = 0
    skipped_count = 0
    processed_count = 0
    retry_count = 0
    attempts_count = 0
    failure_types: Counter = Counter()
    failure_stages: Counter = Counter()
    durations: List[float] = []
    slowest: List[Tuple[float, BatchItemResult]] = []
    summary_warnings: List[str] = []

    if retry_failed:
        resume = True
    checkpoint_state = _load_checkpoint(
        checkpoint_path, prepared, resume)
    summary_warnings.extend(checkpoint_state.warnings)

    results_path = _path_for_run(
        batch_dir, "results", run_id, ".jsonl")
    checkpoint_stream = None
    results_stream = None

    try:
        checkpoint_stream = open(
            checkpoint_path, "a+" if checkpoint_state.resumed else "w",
            encoding="utf-8")
        if not checkpoint_state.resumed:
            _append_json_line(checkpoint_stream, {
                "type": "header",
                "schema_version": _CHECKPOINT_SCHEMA_VERSION,
                "batch_id": prepared.batch_id,
                "input_fingerprint": prepared.fingerprint,
                "item_count": prepared.count,
                "created_at": _utc_now(),
            })

        results_stream = open(results_path, "w", encoding="utf-8")

        for item in _iter_prepared(prepared.path):
            prior = checkpoint_state.records.get(item.item_id)
            if (resume and prior is not None and
                    prior.get("status") == _SUCCESS and
                    _checkpoint_output_is_available(prior, item)):
                result = BatchItemResult(
                    item_id=item.item_id,
                    index=item.index,
                    input=item.input,
                    filename=item.filename,
                    output=prior.get("output", item.output),
                    status=_SUCCESS,
                    attempts=0,
                    retries=0,
                    resumed=True,
                    started_at=_utc_now(),
                    finished_at=_utc_now(),
                    metadata=item.metadata,
                )
                skipped_count += 1
                success_count += 1
            else:
                started_at = _utc_now()
                outcome = _run_item(
                    item, operation, operation_args, operation_kwargs,
                    load_options, save_options, retries, retry_interval)
                finished_at = _utc_now()
                result = BatchItemResult(
                    item_id=item.item_id,
                    index=item.index,
                    input=item.input,
                    filename=item.filename,
                    output=outcome.output,
                    status=outcome.status,
                    stage=outcome.stage,
                    exception_type=outcome.exception_type,
                    exception_module=outcome.exception_module,
                    error_message=outcome.error_message,
                    error_detail=outcome.error_detail,
                    attempts=outcome.attempts,
                    retries=outcome.retries,
                    duration_seconds=outcome.duration,
                    wall_duration_seconds=outcome.wall_duration,
                    stage_durations_seconds=outcome.stage_durations,
                    resumed=False,
                    started_at=started_at,
                    finished_at=finished_at,
                    metadata=item.metadata,
                )

                processed_count += 1
                attempts_count += outcome.attempts
                retry_count += outcome.retries
                durations.append(outcome.duration)
                slowest.append((outcome.duration, result))
                slowest = sorted(
                    slowest, key=lambda entry: entry[0],
                    reverse=True)[:10]

                if outcome.status == _SUCCESS:
                    success_count += 1
                else:
                    failure_count += 1
                    failure_types[outcome.exception_type or "Exception"] += 1
                    failure_stages[outcome.stage or "unknown"] += 1

            result_count += 1
            results_stream.write(_json_dumps(result.to_dict()) + "\n")
            results_stream.flush()

            if not result.resumed:
                try:
                    _append_json_line(checkpoint_stream, {
                        "type": "item",
                        **result.to_dict(),
                    })
                except OSError as exc:
                    message = (
                        "could not update checkpoint {}: {}".format(
                            checkpoint_path, exc))
                    if message not in summary_warnings:
                        summary_warnings.append(message)
                        warnings.warn(message, RuntimeWarning, stacklevel=2)

            if on_result is not None:
                try:
                    on_result(result)
                except Exception as exc:
                    message = "on_result callback failed: {}".format(exc)
                    if message not in summary_warnings:
                        summary_warnings.append(message)

    finally:
        if results_stream is not None:
            results_stream.close()
        if checkpoint_stream is not None:
            checkpoint_stream.close()
        try:
            os.unlink(prepared.path)
        except OSError:
            pass

    elapsed = time.perf_counter() - started
    if failure_count == 0:
        status = "success"
    elif success_count == 0:
        status = "failure"
    else:
        status = "partial_failure"

    summary = BatchSummary(
        batch_id=prepared.batch_id,
        run_id=run_id,
        status=status,
        total_count=prepared.count,
        success_count=success_count,
        failure_count=failure_count,
        skipped_count=skipped_count,
        processed_count=processed_count,
        retry_count=retry_count,
        attempts_count=attempts_count,
        failure_types=dict(sorted(failure_types.items())),
        failure_stages=dict(sorted(failure_stages.items())),
        duration_stats=_duration_stats(durations),
        slowest_items=[
            {
                "item_id": result.item_id,
                "filename": result.filename,
                "duration_seconds": duration,
                "status": result.status,
            }
            for duration, result in slowest
        ],
        elapsed_seconds=elapsed,
        checkpoint_path=checkpoint_path,
        results_path=results_path,
        results=BatchResultStream(results_path, result_count),
        resumed=checkpoint_state.resumed,
        checkpoint_recovered=checkpoint_state.recovered,
        warnings=summary_warnings,
    )

    # Results and checkpoint are already durable.  Summary failures are
    # reported as warnings and never change the completed item results.
    _write_summaries(summary, summary_dir, run_id)

    return summary


# Friendly aliases for callers which prefer a verb-first name.
batch_process = process_batch
run_batch = process_batch


class BatchProcessor:
    """Reusable batch configuration wrapper around :func:`process_batch`."""

    def __init__(self, operation: Union[str, Any], **options: Any):
        self.operation = operation
        self.options = dict(options)

    def run(self, inputs: Union[str, Path, Mapping[str, Any],
                               Iterable[Any]], **options: Any) -> BatchSummary:
        """Run configured batch options, allowing per-call overrides."""

        merged = dict(self.options)
        merged.update(options)
        return process_batch(inputs, self.operation, **merged)


__all__ = [
    "BatchItem",
    "BatchItemResult",
    "BatchProcessor",
    "BatchResultStream",
    "BatchStageError",
    "BatchSummary",
    "batch_process",
    "process_batch",
    "run_batch",
]

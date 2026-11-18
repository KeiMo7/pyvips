# vim: set fileencoding=utf-8 :

"""Deterministic, resumable batch colour-space conversion.

This module is deliberately a thin layer over the existing libvips
``colourspace`` and ``icc_transform`` operations.  It adds no colour
management dependency of its own.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import struct
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence, Union

import pyvips


SCHEMA_VERSION = 1

PROFILE_MISSING = "missing"
PROFILE_COMPLETE = "complete"
PROFILE_CORRUPT = "corrupt"

STATUS_CONVERTED = "converted"
STATUS_ASSUMED = "assumed"
STATUS_SKIPPED = "skipped"
STATUS_DUPLICATE = "duplicate"
STATUS_FAILED = "failed"

_SUCCESS_STATUSES = {
    STATUS_CONVERTED,
    STATUS_ASSUMED,
    STATUS_SKIPPED,
    STATUS_DUPLICATE,
}

_SPACE_ALIASES = {
    "bw": "b-w",
    "black-white": "b-w",
    "gray": "b-w",
    "greyscale": "b-w",
    "grayscale": "b-w",
    "grey": "b-w",
    "cmyk-colour": "cmyk",
    "cmyk-color": "cmyk",
    "rgb": "rgb",
    "srgb": "srgb",
}

_ICC_SPACES = {
    b"RGB ": "rgb",
    b"GRAY": "b-w",
    b"CMYK": "cmyk",
    b"Lab ": "lab",
    b"XYZ ": "xyz",
    b"HSV ": "hsv",
}


class ColorBatchError(ValueError):
    """Base class for batch colour conversion errors."""


class ColorBatchDisabledError(ColorBatchError):
    """Raised when the opt-in batch entry point has not been enabled."""


class ColorBatchLedgerError(ColorBatchError):
    """Raised when an existing conclusion ledger is malformed."""


@dataclass(frozen=True)
class BatchItem:
    """One original input in a colour-conversion batch.

    ``assumption`` is a libvips interpretation name such as ``"srgb"`` or
    ``"cmyk"``.  It may also point at an ICC profile to use for an assumed
    input profile.  An item-level assumption always wins over an entry in the
    batch-level ``assumptions`` mapping.
    """

    image_id: str
    path: Union[str, Path]
    assumption: Optional[Union[str, Path]] = None
    output_path: Optional[Union[str, Path]] = None


@dataclass(frozen=True)
class ProfileInspection:
    """Result of inspecting embedded ICC profile bytes."""

    status: str
    source_space: Optional[str] = None
    colour_space_signature: Optional[str] = None
    description: Optional[str] = None
    declared_size: Optional[int] = None
    reason: Optional[str] = None

    @property
    def usable(self) -> bool:
        return self.status == PROFILE_COMPLETE and self.source_space is not None

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "source_space": self.source_space,
            "colour_space_signature": self.colour_space_signature,
            "description": self.description,
            "declared_size": self.declared_size,
            "reason": self.reason,
        }


@dataclass
class ColorBatchResult:
    """Conclusions for every original input, grouped for callers."""

    records: list[dict[str, Any]] = field(default_factory=list)
    elapsed_seconds: float = 0.0
    interrupted: bool = False
    ledger_path: Optional[str] = None

    @property
    def completed(self) -> list[dict[str, Any]]:
        return [
            record for record in self.records
            if record["status"] in _SUCCESS_STATUSES
            and not record.get("incomplete", False)
        ]

    @property
    def failed(self) -> list[dict[str, Any]]:
        return [
            record for record in self.records
            if record["status"] == STATUS_FAILED
            or record.get("incomplete", False)
        ]

    @property
    def duplicates(self) -> list[dict[str, Any]]:
        return [
            record for record in self.records
            if record["status"] == STATUS_DUPLICATE
        ]

    @property
    def incomplete(self) -> list[dict[str, Any]]:
        return [
            record for record in self.records
            if record.get("incomplete", False)
        ]

    def conclusion_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for record in self.records:
            status = record["status"]
            counts[status] = counts.get(status, 0) + 1
        return dict(sorted(counts.items()))

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "elapsed_seconds": self.elapsed_seconds,
            "interrupted": self.interrupted,
            "ledger_path": self.ledger_path,
            "conclusion_counts": self.conclusion_counts(),
            "records": self.records,
        }


@dataclass(frozen=True)
class _ResolvedAssumption:
    space: Optional[str] = None
    profile_path: Optional[str] = None
    requested: Optional[str] = None
    error: Optional[str] = None


@dataclass(frozen=True)
class _Plan:
    status: str
    source_space: Optional[str]
    profile: ProfileInspection
    assumption: Optional[str]
    assumption_profile: Optional[str]
    use_embedded_profile: bool
    use_assumption_profile: bool
    skipped: bool
    degraded: bool
    error: Optional[str] = None
    error_code: Optional[str] = None

    @property
    def signature(self) -> str:
        return "|".join([
            self.status,
            self.source_space or "",
            self.profile.status,
            self.assumption or "",
            self.assumption_profile or "",
            "embedded" if self.use_embedded_profile else "",
            "assumed-profile" if self.use_assumption_profile else "",
            "skipped" if self.skipped else "",
            "degraded" if self.degraded else "",
        ])


def _normalise_space(value: Any) -> Optional[str]:
    if value is None:
        return None

    value = str(value).strip()
    if not value:
        return None

    alias = _SPACE_ALIASES.get(value.lower())
    return alias if alias is not None else value.lower()


def _read_uint32(data: bytes, offset: int) -> Optional[int]:
    if offset < 0 or offset + 4 > len(data):
        return None
    return struct.unpack_from(">I", data, offset)[0]


def _profile_description(data: bytes,
                          tag_records: Mapping[bytes, tuple[int, int]]
                          ) -> Optional[str]:
    try:
        if b"desc" in tag_records:
            offset, size = tag_records[b"desc"]
            block = data[offset:offset + size]
            if len(block) >= 12 and block[:4] == b"desc":
                count = struct.unpack_from(">I", block, 8)[0]
                text = block[12:12 + max(0, count - 1)]
                return text.decode("latin-1", errors="replace").strip("\x00 ")

        if b"mluc" in tag_records:
            offset, size = tag_records[b"mluc"]
            block = data[offset:offset + size]
            if len(block) < 28 or block[:4] != b"mluc":
                return None

            record_count = struct.unpack_from(">I", block, 8)[0]
            if record_count == 0:
                return None

            string_offset, string_length = struct.unpack_from(
                ">II", block, 20)
            text = block[string_offset:string_offset + string_length]
            return text.decode("utf-16-be", errors="replace").strip("\x00 ")
    except (IndexError, struct.error, UnicodeError):
        return None

    return None


def inspect_icc_profile(profile: Optional[bytes]) -> ProfileInspection:
    """Inspect ICC profile bytes without adding an ICC library.

    A profile is complete only when its header, declared length and tag table
    are internally consistent.  Header colour-space information is still
    returned for damaged profiles when it can be read, allowing the batch
    layer to make a conservative, explicit fallback decision.
    """

    if not profile:
        return ProfileInspection(
            status=PROFILE_MISSING,
            reason="no embedded ICC profile",
        )

    data = bytes(profile)
    colour_signature: Optional[str] = None
    source_space: Optional[str] = None

    if len(data) >= 20:
        raw_space = data[16:20]
        colour_signature = raw_space.decode("latin-1", errors="replace")
        source_space = _ICC_SPACES.get(raw_space)

    if len(data) < 128:
        return ProfileInspection(
            status=PROFILE_CORRUPT,
            source_space=source_space,
            colour_space_signature=colour_signature,
            reason="ICC header is shorter than 128 bytes",
        )

    declared_size = _read_uint32(data, 0)
    if declared_size is None or declared_size < 128:
        return ProfileInspection(
            status=PROFILE_CORRUPT,
            source_space=source_space,
            colour_space_signature=colour_signature,
            declared_size=declared_size,
            reason="invalid declared ICC profile size",
        )

    if declared_size > len(data):
        return ProfileInspection(
            status=PROFILE_CORRUPT,
            source_space=source_space,
            colour_space_signature=colour_signature,
            declared_size=declared_size,
            reason="ICC profile is truncated",
        )

    if data[36:40] != b"acsp":
        return ProfileInspection(
            status=PROFILE_CORRUPT,
            source_space=source_space,
            colour_space_signature=colour_signature,
            declared_size=declared_size,
            reason="missing acsp ICC signature",
        )

    tag_count = _read_uint32(data, 128)
    if tag_count is None or tag_count > 4096:
        return ProfileInspection(
            status=PROFILE_CORRUPT,
            source_space=source_space,
            colour_space_signature=colour_signature,
            declared_size=declared_size,
            reason="invalid ICC tag count",
        )

    table_end = 132 + 12 * tag_count
    if table_end > declared_size:
        return ProfileInspection(
            status=PROFILE_CORRUPT,
            source_space=source_space,
            colour_space_signature=colour_signature,
            declared_size=declared_size,
            reason="ICC tag table extends past declared profile size",
        )

    tag_records: dict[bytes, tuple[int, int]] = {}
    for index in range(tag_count):
        record_offset = 132 + 12 * index
        signature = data[record_offset:record_offset + 4]
        tag_offset = _read_uint32(data, record_offset + 4)
        tag_size = _read_uint32(data, record_offset + 8)

        if tag_offset is None or tag_size is None:
            return ProfileInspection(
                status=PROFILE_CORRUPT,
                source_space=source_space,
                colour_space_signature=colour_signature,
                declared_size=declared_size,
                reason="truncated ICC tag table",
            )

        if tag_size == 0 or tag_offset < table_end:
            return ProfileInspection(
                status=PROFILE_CORRUPT,
                source_space=source_space,
                colour_space_signature=colour_signature,
                declared_size=declared_size,
                reason=f"invalid ICC tag {signature!r}",
            )

        if tag_offset + tag_size > declared_size:
            return ProfileInspection(
                status=PROFILE_CORRUPT,
                source_space=source_space,
                colour_space_signature=colour_signature,
                declared_size=declared_size,
                reason=f"ICC tag {signature!r} extends past profile size",
            )

        tag_records[signature] = (tag_offset, tag_size)

    description = _profile_description(data, tag_records)
    if source_space == "rgb" and description:
        lowered = description.lower()
        if "srgb" in lowered:
            source_space = "srgb"

    if source_space is None:
        return ProfileInspection(
            status=PROFILE_CORRUPT,
            source_space=None,
            colour_space_signature=colour_signature,
            description=description,
            declared_size=declared_size,
            reason="unsupported or unreadable ICC colour-space signature",
        )

    return ProfileInspection(
        status=PROFILE_COMPLETE,
        source_space=source_space,
        colour_space_signature=colour_signature,
        description=description,
        declared_size=declared_size,
    )


def _profile_bytes(image: pyvips.Image) -> Optional[bytes]:
    try:
        if image.get_typeof("icc-profile-data") == 0:
            return None
        profile = image.get("icc-profile-data")
    except pyvips.Error:
        return None

    if isinstance(profile, bytes):
        return profile
    return bytes(profile)


def _resolve_assumption(
        value: Any) -> _ResolvedAssumption:
    if value is None:
        return _ResolvedAssumption()

    requested = str(value)
    if isinstance(value, Mapping):
        profile_value = value.get("profile") or value.get("icc_profile")
        space_value = value.get("space") or value.get("colourspace")
        requested = str(profile_value or space_value or value)
        if profile_value is not None:
            value = profile_value
        else:
            value = space_value

    if value is None:
        return _ResolvedAssumption(requested=requested)

    candidate = Path(os.fspath(value))
    if candidate.suffix.lower() in {".icc", ".icm"}:
        if not candidate.is_file():
            return _ResolvedAssumption(
                requested=requested,
                error=f"assumption profile does not exist: {candidate}",
            )

        try:
            inspection = inspect_icc_profile(candidate.read_bytes())
        except OSError as error:
            return _ResolvedAssumption(
                requested=requested,
                error=str(error),
            )

        if inspection.source_space is None:
            return _ResolvedAssumption(
                requested=requested,
                error=inspection.reason or "unable to read assumption profile",
            )

        if inspection.status != PROFILE_COMPLETE:
            return _ResolvedAssumption(
                requested=requested,
                space=inspection.source_space,
            )

        return _ResolvedAssumption(
            space=inspection.source_space,
            profile_path=str(candidate.resolve()),
            requested=requested,
        )

    space = _normalise_space(value)
    if space is None:
        return _ResolvedAssumption(requested=requested)
    return _ResolvedAssumption(space=space, requested=requested)


def _assumption_for_item(
        item: BatchItem,
        assumptions: Optional[Mapping[Any, Any]],
        default_assumption: Any) -> Any:
    if item.assumption is not None:
        return item.assumption

    if assumptions:
        item_path = Path(item.path)
        keys = [
            item.image_id,
            str(item.path),
            os.path.abspath(os.fspath(item.path)),
            item_path,
            item_path.resolve(),
            item_path.name,
        ]
        for key in keys:
            if key in assumptions:
                return assumptions[key]

    return default_assumption


def _normalise_item(item: Any, index: int) -> BatchItem:
    if isinstance(item, BatchItem):
        return item

    if isinstance(item, Mapping):
        image_id = item.get("image_id", item.get("id"))
        path = item.get("path", item.get("input_path"))
        if image_id is None or path is None:
            raise ColorBatchError(
                f"batch item {index} needs image_id and path")
        return BatchItem(
            image_id=str(image_id),
            path=path,
            assumption=item.get("assumption"),
            output_path=item.get("output_path", item.get("output")),
        )

    if isinstance(item, (str, os.PathLike)):
        path = item
        return BatchItem(image_id=Path(path).name, path=path)

    if isinstance(item, Sequence) and not isinstance(item, (str, bytes)):
        if len(item) == 2:
            return BatchItem(image_id=str(item[0]), path=item[1])

    raise ColorBatchError(
        f"batch item {index} must be BatchItem, mapping, path, or "
        "(image_id, path)")


def _normalise_items(items: Iterable[Any]) -> list[BatchItem]:
    if isinstance(items, Mapping):
        items = [
            {"image_id": image_id, "path": path}
            for image_id, path in items.items()
        ]

    result = [_normalise_item(item, index)
              for index, item in enumerate(items)]

    seen: set[str] = set()
    for item in result:
        if item.image_id in seen:
            raise ColorBatchError(
                f"duplicate image_id {item.image_id!r}; IDs must be unique")
        seen.add(item.image_id)

    return result


def _normalise_assumptions(
        assumptions: Optional[Any]) -> Optional[Mapping[Any, Any]]:
    if assumptions is None:
        return None
    if isinstance(assumptions, Mapping):
        return assumptions

    result: dict[Any, Any] = {}
    for entry in assumptions:
        if not isinstance(entry, Mapping):
            raise ColorBatchError(
                "assumptions must be a mapping or a sequence of mappings")
        image_id = entry.get("image_id", entry.get("id"))
        if image_id is None:
            raise ColorBatchError(
                "each assumption entry needs image_id")
        value = entry.get(
            "assumption",
            entry.get("space", entry.get("colourspace")),
        )
        result[str(image_id)] = value
    return result


def _content_hash(path: Union[str, Path]) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as source:
        while True:
            block = source.read(1024 * 1024)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def _target_profile_info(
        target_profile: Optional[Union[str, Path]]
        ) -> Optional[tuple[str, ProfileInspection]]:
    if target_profile is None:
        return None

    path = Path(target_profile).resolve()
    if not path.is_file():
        raise ColorBatchError(f"target profile does not exist: {path}")

    inspection = inspect_icc_profile(path.read_bytes())
    if inspection.status != PROFILE_COMPLETE:
        raise ColorBatchError(
            f"target profile is not complete: {inspection.reason}")
    if inspection.source_space is None:
        raise ColorBatchError(
            "target profile colour space cannot be determined")

    return str(path), inspection


def _make_plan(
        image: pyvips.Image,
        assumption: Any,
        target_space: Optional[str],
        target_profile: Optional[str],
        target_profile_info: Optional[ProfileInspection],
        target_profile_bytes: Optional[bytes]) -> _Plan:
    profile = inspect_icc_profile(_profile_bytes(image))
    resolved = _resolve_assumption(assumption)
    assumption_declared = assumption is not None

    if target_space is None and target_profile_info is not None:
        target_space = target_profile_info.source_space

    def missing_assumption(fallback_profile: ProfileInspection) -> _Plan:
        if not assumption_declared:
            return _Plan(
                status=STATUS_FAILED,
                source_space=profile.source_space,
                profile=profile,
                assumption=None,
                assumption_profile=None,
                use_embedded_profile=False,
                use_assumption_profile=False,
                skipped=False,
                degraded=False,
                error=(
                    "embedded ICC profile is unavailable and no per-image "
                    "source colour-space assumption was declared"
                ),
                error_code="missing_assumption",
            )

        if resolved.error or resolved.space is None:
            return _Plan(
                status=STATUS_FAILED,
                source_space=profile.source_space,
                profile=profile,
                assumption=resolved.space,
                assumption_profile=resolved.profile_path,
                use_embedded_profile=False,
                use_assumption_profile=False,
                skipped=False,
                degraded=False,
                error=(
                    f"invalid assumption for image: {resolved.error or 'empty'}"
                ),
                error_code="invalid_assumption",
            )

        if _normalise_space(target_space) == resolved.space:
            return _Plan(
                status=STATUS_SKIPPED,
                source_space=resolved.space,
                profile=fallback_profile,
                assumption=resolved.space,
                assumption_profile=resolved.profile_path,
                use_embedded_profile=False,
                use_assumption_profile=False,
                skipped=True,
                degraded=False,
            )

        return _Plan(
            status=STATUS_ASSUMED,
            source_space=resolved.space,
            profile=fallback_profile,
            assumption=resolved.space,
            assumption_profile=resolved.profile_path,
            use_embedded_profile=False,
            use_assumption_profile=resolved.profile_path is not None,
            skipped=False,
            degraded=(
                fallback_profile.status == PROFILE_CORRUPT
                or (
                    target_profile is not None
                    and resolved.profile_path is None
                )
            ),
        )

    if profile.status == PROFILE_COMPLETE and profile.usable:
        if target_profile is not None and target_profile_bytes is not None:
            source_bytes = _profile_bytes(image)
            if source_bytes == target_profile_bytes:
                return _Plan(
                    status=STATUS_SKIPPED,
                    source_space=profile.source_space,
                    profile=profile,
                    assumption=None,
                    assumption_profile=None,
                    use_embedded_profile=False,
                    use_assumption_profile=False,
                    skipped=True,
                    degraded=False,
                )

            return _Plan(
                status=STATUS_CONVERTED,
                source_space=profile.source_space,
                profile=profile,
                assumption=None,
                assumption_profile=None,
                use_embedded_profile=True,
                use_assumption_profile=False,
                skipped=False,
                degraded=False,
            )

        if _normalise_space(target_space) == profile.source_space:
            return _Plan(
                status=STATUS_SKIPPED,
                source_space=profile.source_space,
                profile=profile,
                assumption=None,
                assumption_profile=None,
                use_embedded_profile=False,
                use_assumption_profile=False,
                skipped=True,
                degraded=False,
            )

        return _Plan(
            status=STATUS_CONVERTED,
            source_space=profile.source_space,
            profile=profile,
            assumption=None,
            assumption_profile=None,
            use_embedded_profile=False,
            use_assumption_profile=False,
            skipped=False,
            degraded=False,
        )

    if profile.status == PROFILE_CORRUPT and profile.source_space is not None:
        if _normalise_space(target_space) == profile.source_space:
            return _Plan(
                status=STATUS_SKIPPED,
                source_space=profile.source_space,
                profile=profile,
                assumption=None,
                assumption_profile=None,
                use_embedded_profile=False,
                use_assumption_profile=False,
                skipped=True,
                degraded=True,
            )

        return _Plan(
            status=STATUS_CONVERTED,
            source_space=profile.source_space,
            profile=profile,
            assumption=None,
            assumption_profile=None,
            use_embedded_profile=False,
            use_assumption_profile=False,
            skipped=False,
            degraded=True,
        )

    return missing_assumption(profile)


def _safe_output_name(image_id: str, source_path: Union[str, Path]) -> str:
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", str(image_id)).strip("._")
    if not name:
        name = "image"

    if not Path(name).suffix:
        name += Path(source_path).suffix
    return name


def _output_path_for(
        item: BatchItem,
        output_dir: Optional[Union[str, Path]]) -> Optional[str]:
    if item.output_path is not None:
        return str(Path(item.output_path))
    if output_dir is None:
        return None
    return str(Path(output_dir) / _safe_output_name(item.image_id, item.path))


def _artifact_path(output_path: str, marker: str) -> Path:
    output = Path(output_path)
    return output.with_name(
        f"{output.name}.{marker}{output.suffix}" if output.suffix else
        f"{output.name}.{marker}"
    )


def _touch_incomplete(output_path: str) -> str:
    marker = _artifact_path(output_path, "incomplete")
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.touch()
    return str(marker)


def _atomic_write_image(
        image: pyvips.Image,
        output_path: Optional[str]) -> Optional[str]:
    if output_path is None:
        return None

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    part = _artifact_path(output_path, "part")
    incomplete = _artifact_path(output_path, "incomplete")

    if part.exists():
        part.unlink()
    if incomplete.exists():
        incomplete.unlink()

    try:
        image.write_to_file(str(part))
        os.replace(part, output)
    except BaseException:
        if part.exists():
            if incomplete.exists():
                incomplete.unlink()
            os.replace(part, incomplete)
        else:
            incomplete.touch()
        raise

    return str(output)


def _atomic_copy(
        source: str,
        output_path: Optional[str]) -> Optional[str]:
    if output_path is None:
        return None

    source_path = Path(source)
    output = Path(output_path)
    if source_path.resolve() == output.resolve():
        return str(output)

    output.parent.mkdir(parents=True, exist_ok=True)
    part = _artifact_path(str(output), "part")
    incomplete = _artifact_path(str(output), "incomplete")
    if part.exists():
        part.unlink()
    if incomplete.exists():
        incomplete.unlink()
    try:
        shutil.copyfile(source_path, part)
        os.replace(part, output)
    except BaseException:
        if part.exists():
            os.replace(part, incomplete)
        else:
            incomplete.touch()
        raise
    return str(output)


def _convert_image(
        image: pyvips.Image,
        plan: _Plan,
        target_space: Optional[str],
        target_profile: Optional[str],
        intent: str,
        black_point_compensation: bool) -> pyvips.Image:
    if plan.use_embedded_profile:
        if target_profile is None:
            raise ColorBatchError(
                "embedded-profile conversion requires target_profile")
        return image.icc_transform(
            target_profile,
            embedded=True,
            intent=intent,
            black_point_compensation=black_point_compensation,
        )

    if plan.use_assumption_profile:
        if target_profile is None:
            raise ColorBatchError(
                "assumption-profile conversion requires target_profile")
        return image.icc_transform(
            target_profile,
            embedded=False,
            input_profile=plan.assumption_profile,
            intent=intent,
            black_point_compensation=black_point_compensation,
        )

    if target_space is None:
        raise ColorBatchError("target colour space cannot be determined")
    if plan.source_space is None:
        raise ColorBatchError("source colour space cannot be determined")

    return image.colourspace(target_space, source_space=plan.source_space)


def _record_key(
        batch_id: str,
        image_id: str,
        content_hash: str,
        plan: _Plan,
        target_key: str) -> str:
    payload = "\0".join([
        batch_id,
        image_id,
        content_hash,
        plan.signature,
        target_key,
    ])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _base_record(
        item: BatchItem,
        batch_id: str,
        record_key: str,
        content_hash: str,
        plan: _Plan,
        target_space: Optional[str],
        target_profile: Optional[str],
        output_path: Optional[str]) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "record_key": record_key,
        "batch_id": batch_id,
        "image_id": item.image_id,
        "input_path": str(Path(item.path)),
        "content_sha256": content_hash,
        "status": plan.status,
        "source_space": plan.source_space,
        "source_space_detected": plan.profile.source_space,
        "source_space_effective": plan.source_space,
        "source_detected": plan.profile.source_space is not None,
        "profile_colour_space": plan.profile.colour_space_signature,
        "profile_status": plan.profile.status,
        "profile_error": plan.profile.reason,
        "profile_description": plan.profile.description,
        "assumption": plan.assumption,
        "assumption_profile": plan.assumption_profile,
        "used_assumption": plan.assumption is not None,
        "target_space": target_space,
        "target_profile": target_profile,
        "skipped": plan.skipped,
        "degraded": plan.degraded,
        "incomplete": False,
        "duplicate_of": None,
        "output_path": output_path,
        "duration_ms": 0.0,
        "error": plan.error,
        "error_code": plan.error_code,
    }


class _Ledger:
    def __init__(self, path: Optional[Union[str, Path]]):
        self.path = str(Path(path)) if path is not None else None
        self.records: list[dict[str, Any]] = []
        self.by_key: dict[str, dict[str, Any]] = {}
        self._load_and_repair()

    def _load_and_repair(self) -> None:
        if self.path is None or not Path(self.path).exists():
            return

        ledger_path = Path(self.path)
        data = ledger_path.read_bytes()
        repaired = False

        if data and not data.endswith(b"\n"):
            last_newline = data.rfind(b"\n")
            if last_newline < 0:
                data = b""
            else:
                data = data[:last_newline + 1]
            repaired = True

        parsed: list[dict[str, Any]] = []
        for line_number, line in enumerate(data.splitlines(), start=1):
            if not line.strip():
                continue
            try:
                record = json.loads(line.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise ColorBatchLedgerError(
                    f"invalid JSON in ledger {ledger_path} "
                    f"at line {line_number}: {error}"
                ) from error
            if not isinstance(record, dict) or "record_key" not in record:
                raise ColorBatchLedgerError(
                    f"invalid record in ledger {ledger_path} "
                    f"at line {line_number}"
                )
            parsed.append(record)

        self.records = parsed
        for record in parsed:
            key = str(record["record_key"])
            if key in self.by_key:
                repaired = True
            self.by_key[key] = record

        if repaired:
            self._rewrite()

    def _rewrite(self) -> None:
        if self.path is None:
            return

        path = Path(self.path)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.repair.tmp")
        with open(temporary, "w", encoding="utf-8", newline="\n") as target:
            for record in self.by_key.values():
                target.write(json.dumps(
                    record,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ))
                target.write("\n")
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary, path)

    def get(self, key: str) -> Optional[dict[str, Any]]:
        return self.by_key.get(key)

    def append(self, record: dict[str, Any]) -> None:
        key = str(record["record_key"])
        if key in self.by_key:
            return

        self.by_key[key] = record
        self.records.append(record)
        if self.path is None:
            return

        path = Path(self.path)
        path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(
            record,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ) + "\n"

        with open(path, "ab+") as target:
            target.seek(0)
            data = target.read()
            if data and not data.endswith(b"\n"):
                last_newline = data.rfind(b"\n")
                target.truncate(last_newline + 1 if last_newline >= 0 else 0)

            target.seek(0, os.SEEK_END)
            target.write(line.encode("utf-8"))
            target.flush()
            os.fsync(target.fileno())


def _default_batch_id(items: Sequence[BatchItem], target_key: str) -> str:
    digest = hashlib.sha256()
    for item in items:
        digest.update(str(item.image_id).encode("utf-8"))
        digest.update(b"\0")
        digest.update(os.path.abspath(os.fspath(item.path)).encode("utf-8"))
        digest.update(b"\0")
    digest.update(target_key.encode("utf-8"))
    return "batch-" + digest.hexdigest()[:16]


def _set_timing(
        record: dict[str, Any],
        started_at: float) -> dict[str, Any]:
    record["duration_ms"] = round(
        (time.perf_counter() - started_at) * 1000.0, 3)
    return record


def _fail_record(
        record: dict[str, Any],
        error: BaseException,
        error_code: str,
        incomplete_path: Optional[str] = None) -> dict[str, Any]:
    record["status"] = STATUS_FAILED
    record["skipped"] = False
    record["incomplete"] = incomplete_path is not None
    record["error"] = str(error)
    record["error_code"] = error_code
    if incomplete_path is not None:
        record["output_path"] = incomplete_path
    return record


def process_batch(
        items: Iterable[Any],
        *,
        target_space: Optional[Union[str, Any]] = None,
        target_profile: Optional[Union[str, Path]] = None,
        assumptions: Optional[Any] = None,
        default_assumption: Any = None,
        enabled: bool = False,
        output_dir: Optional[Union[str, Path]] = None,
        ledger_path: Optional[Union[str, Path]] = None,
        batch_id: Optional[str] = None,
        intent: str = "relative",
        black_point_compensation: bool = False,
        resume: bool = True) -> ColorBatchResult:
    """Convert a mixed batch while recording every input's conclusion.

    This is a new opt-in entry point: callers must pass ``enabled=True``.
    Existing :class:`pyvips.Image` conversion methods are unchanged.

    Args:
        items: Paths, ``BatchItem`` objects, mappings with ``image_id`` and
            ``path``, or ``(image_id, path)`` pairs.
        target_space: Destination libvips interpretation, for example
            ``"srgb"``.
        target_profile: Optional destination ICC profile.  When supplied,
            complete embedded source profiles are converted with
            ``icc_transform``.
        assumptions: Mapping from image ID, path, basename or absolute path
            to a per-image source colour-space assumption.
        default_assumption: Fallback assumption used only when no per-image
            assumption is present.
        enabled: Feature switch.  The batch entry point is closed by default.
        output_dir: Optional directory for converted files.
        ledger_path: Optional JSON Lines conclusion ledger.  Existing
            complete records are reused on subsequent runs.
        batch_id: Optional stable batch identifier.  A deterministic ID is
            derived from inputs and target when omitted.
        intent: ICC rendering intent, passed through to libvips.
        black_point_compensation: Passed through to libvips.
        resume: Reuse complete records already present in ``ledger_path``.

    Returns:
        :class:`ColorBatchResult` containing one record per original input.
    """

    if not enabled:
        raise ColorBatchDisabledError(
            "colour batch conversion is disabled; pass enabled=True")

    started_at = time.perf_counter()
    normalised_items = _normalise_items(items)
    normalised_assumptions = _normalise_assumptions(assumptions)
    target_profile_data = _target_profile_info(target_profile)
    target_profile_path: Optional[str] = None
    target_profile_info: Optional[ProfileInspection] = None
    target_profile_bytes: Optional[bytes] = None
    if target_profile_data is not None:
        target_profile_path, target_profile_info = target_profile_data
        target_profile_bytes = Path(target_profile_path).read_bytes()

    normalised_target_space = _normalise_space(target_space)
    if normalised_target_space is None and target_profile_info is not None:
        normalised_target_space = target_profile_info.source_space
    if normalised_target_space is None and target_profile_path is None:
        raise ColorBatchError(
            "either target_space or target_profile must be supplied")

    target_key = "\0".join([
        target_profile_path or "",
        normalised_target_space or "",
    ])
    resolved_batch_id = batch_id or _default_batch_id(
        normalised_items, target_key)

    ledger = _Ledger(ledger_path) if ledger_path is not None else _Ledger(None)
    result = ColorBatchResult(ledger_path=ledger.path)

    canonical_by_dedup: dict[str, dict[str, Any]] = {}
    if resume:
        for record in ledger.records:
            if record.get("status") in _SUCCESS_STATUSES:
                canonical_by_dedup.setdefault(
                    str(record.get("dedup_key", "")), record)

    for item in normalised_items:
        item_started = time.perf_counter()
        output_path = _output_path_for(item, output_dir)

        try:
            content_hash = _content_hash(item.path)
            image = pyvips.Image.new_from_file(str(item.path))
            assumption = _assumption_for_item(
                item, normalised_assumptions, default_assumption)
            plan = _make_plan(
                image,
                assumption,
                normalised_target_space,
                target_profile_path,
                target_profile_info,
                target_profile_bytes,
            )
            record_key = _record_key(
                resolved_batch_id,
                item.image_id,
                content_hash,
                plan,
                target_key,
            )

            existing = ledger.get(record_key) if resume else None
            if existing is not None:
                record = dict(existing)
                record["resumed"] = True
                result.records.append(record)
                continue

            dedup_key = hashlib.sha256(
                f"{content_hash}\0{plan.signature}\0{target_key}".encode(
                    "utf-8")
            ).hexdigest()

            base = _base_record(
                item,
                resolved_batch_id,
                record_key,
                content_hash,
                plan,
                normalised_target_space,
                target_profile_path,
                output_path,
            )
            base["dedup_key"] = dedup_key

            if plan.status == STATUS_FAILED:
                record = _set_timing(base, item_started)
                ledger.append(record)
                result.records.append(record)
                continue

            canonical = canonical_by_dedup.get(dedup_key)
            if canonical is not None:
                base["status"] = STATUS_DUPLICATE
                base["skipped"] = True
                base["duplicate_of"] = canonical.get("image_id")

                canonical_output = canonical.get("output_path")
                if canonical_output and output_path:
                    try:
                        _atomic_copy(canonical_output, output_path)
                    except KeyboardInterrupt:
                        incomplete_path = _touch_incomplete(output_path)
                        result.interrupted = True
                        base["interrupted"] = True
                        record = _fail_record(
                            base,
                            KeyboardInterrupt("batch interrupted"),
                            "interrupted",
                            incomplete_path,
                        )
                        record = _set_timing(record, item_started)
                        result.records.append(record)
                        break
                    except BaseException as error:
                        record = _fail_record(
                            base, error, "duplicate_output_failed")
                        record = _set_timing(record, item_started)
                        ledger.append(record)
                        result.records.append(record)
                        continue

                if canonical_output:
                    base["output_path"] = output_path or canonical_output

                record = _set_timing(base, item_started)
                ledger.append(record)
                result.records.append(record)
                continue

            if plan.skipped:
                try:
                    _atomic_copy(str(item.path), output_path)
                except KeyboardInterrupt:
                    incomplete_path = None
                    if output_path is not None:
                        incomplete_path = _touch_incomplete(output_path)
                    result.interrupted = True
                    base["interrupted"] = True
                    record = _fail_record(
                        base,
                        KeyboardInterrupt("batch interrupted"),
                        "interrupted",
                        incomplete_path,
                    )
                    record = _set_timing(record, item_started)
                    result.records.append(record)
                    break
                except BaseException as error:
                    record = _fail_record(
                        base, error, "skip_output_failed")
                    record = _set_timing(record, item_started)
                    ledger.append(record)
                    result.records.append(record)
                    continue
            else:
                try:
                    converted = _convert_image(
                        image,
                        plan,
                        normalised_target_space,
                        target_profile_path,
                        intent,
                        black_point_compensation,
                    )
                    _atomic_write_image(converted, output_path)
                except KeyboardInterrupt:
                    incomplete_path = None
                    if output_path is not None:
                        incomplete_path = _touch_incomplete(output_path)
                    result.interrupted = True
                    base["interrupted"] = True
                    record = _fail_record(
                        base,
                        KeyboardInterrupt("batch interrupted"),
                        "interrupted",
                        incomplete_path,
                    )
                    record = _set_timing(record, item_started)
                    result.records.append(record)
                    break
                except BaseException as error:
                    incomplete_path = None
                    if output_path is not None:
                        incomplete_path = _touch_incomplete(output_path)
                    record = _fail_record(
                        base, error, "conversion_failed", incomplete_path)
                    record = _set_timing(record, item_started)
                    ledger.append(record)
                    result.records.append(record)
                    continue

            base["output_path"] = output_path
            record = _set_timing(base, item_started)
            ledger.append(record)
            result.records.append(record)
            canonical_by_dedup[dedup_key] = record
        except KeyboardInterrupt:
            result.interrupted = True
            break
        except BaseException as error:
            fallback_plan = _Plan(
                status=STATUS_FAILED,
                source_space=None,
                profile=ProfileInspection(
                    status=PROFILE_MISSING,
                    reason="input could not be inspected",
                ),
                assumption=None,
                assumption_profile=None,
                use_embedded_profile=False,
                use_assumption_profile=False,
                skipped=False,
                degraded=False,
                error=str(error),
                error_code="input_error",
            )
            record_key = _record_key(
                resolved_batch_id,
                item.image_id,
                "",
                fallback_plan,
                target_key,
            )
            existing = ledger.get(record_key) if resume else None
            if existing is not None:
                record = dict(existing)
                record["resumed"] = True
            else:
                base = _base_record(
                    item,
                    resolved_batch_id,
                    record_key,
                    "",
                    fallback_plan,
                    normalised_target_space,
                    target_profile_path,
                    output_path,
                )
                record = _fail_record(base, error, "input_error")
                record = _set_timing(record, item_started)
                ledger.append(record)
            result.records.append(record)

    result.elapsed_seconds = round(time.perf_counter() - started_at, 6)
    return result


def convert_batch(*args: Any, **kwargs: Any) -> ColorBatchResult:
    """Alias for :func:`process_batch`."""

    return process_batch(*args, **kwargs)


def colourspace_batch(*args: Any, **kwargs: Any) -> ColorBatchResult:
    """Alias for :func:`process_batch`."""

    return process_batch(*args, **kwargs)


def color_space_batch(*args: Any, **kwargs: Any) -> ColorBatchResult:
    """Alias for :func:`process_batch`."""

    return process_batch(*args, **kwargs)


__all__ = [
    "BatchItem",
    "ColorBatchDisabledError",
    "ColorBatchError",
    "ColorBatchLedgerError",
    "ColorBatchResult",
    "ProfileInspection",
    "color_space_batch",
    "colourspace_batch",
    "convert_batch",
    "inspect_icc_profile",
    "process_batch",
]

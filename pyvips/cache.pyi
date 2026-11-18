from typing import Any, Callable

from pyvips.vimage import Image

class CacheStats:
    full_hits: int
    partial_hits: int
    misses: int
    bypasses: int
    corruptions: int
    read_errors: int
    write_errors: int
    capacity_evictions: int
    expired_evictions: int
    writes: int

    @property
    def requests(self) -> int: ...
    @property
    def hit_rate(self) -> float: ...
    @property
    def partial_hit_rate(self) -> float: ...
    @property
    def reuse_rate(self) -> float: ...

class CacheKey:
    key: str
    source_fingerprint: str
    chain_digest: str
    operations: tuple[tuple[str, str], ...]
    source_id: Any

class RenderCache:
    directory: str
    enabled: bool
    max_bytes: int | None
    max_entries: int | None
    max_age: float | None
    stats: CacheStats

    def __init__(
        self,
        directory: str,
        *,
        max_bytes: int | None = ...,
        max_entries: int | None = ...,
        max_age: float | None = ...,
        enabled: bool = ...,
    ) -> None: ...
    def set_max_bytes(
        self, max_bytes: int | None, *, reset_stats: bool = ...
    ) -> None: ...
    def set_max_entries(
        self, max_entries: int | None, *, reset_stats: bool = ...
    ) -> None: ...
    def set_max_age(
        self, max_age: float | None, *, reset_stats: bool = ...
    ) -> None: ...
    def reset_stats(self) -> None: ...
    def compute_key(
        self,
        source: Image,
        pipeline: Callable[[Image], Image],
        *,
        source_id: Any = ...,
    ) -> CacheKey: ...
    def render(
        self,
        source: Image,
        pipeline: Callable[[Image], Image],
        *,
        source_id: Any = ...,
    ) -> Image: ...
    def get_or_render(
        self,
        source: Image,
        pipeline: Callable[[Image], Image],
        *,
        source_id: Any = ...,
    ) -> Image: ...
    def enforce_capacity(self) -> bool: ...
    def evict_expired(self, now: float | None = ...) -> int: ...
    def cleanup(self) -> int: ...
    def get_entry_info(self, key: str) -> dict[str, Any] | None: ...

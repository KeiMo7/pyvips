"""A content-addressed cache for complete pyvips render pipelines.

The cache is deliberately opt-in.  ``RenderCache(enabled=False)``, which is
the default, only calls the supplied pipeline and does not touch the cache
directory or install any operation tracking.

A request is identified by:

* a fingerprint of the source image content (never its filename), and
* a digest of the lazy libvips operation graph that produced the result.

Operation arguments are canonicalised before hashing, so equivalent spellings
such as ``1`` / ``1.0``, lists / tuples and keyword arguments in a different
order produce the same key.
"""

import base64
import concurrent.futures
import contextvars
import contextlib
import errno
import hashlib
import json
import logging
import math
import os
import re
import tempfile
import threading
import time
from dataclasses import dataclass

import pyvips
from pyvips.voperation import Introspect, Operation, _set_operation_observer

logger = logging.getLogger(__name__)

_SCHEMA_VERSION = 1
_ROOT_HASH_PREFIX = b'pyvips-render-cache\x001\x00'
_CORE_FIELDS = {
    'width', 'height', 'bands', 'format', 'coding', 'interpretation',
    'xoffset', 'yoffset', 'xres', 'yres', 'filename',
}
_COPY_FIELDS = (
    'coding', 'interpretation', 'xoffset', 'yoffset', 'xres', 'yres',
)
_STAT_KEYS = (
    'full_hits', 'partial_hits', 'misses', 'bypasses', 'corruptions',
    'read_errors', 'write_errors', 'capacity_evictions',
    'expired_evictions', 'writes',
)
_active_recorder = contextvars.ContextVar('pyvips_cache_recorder',
                                          default=None)
_observer_lock = threading.Lock()
_observer_installed = False
_defaults_lock = threading.Lock()
_operation_defaults = {}


@dataclass(frozen=True)
class CacheStats:
    """Immutable snapshot of cache statistics."""

    full_hits: int = 0
    partial_hits: int = 0
    misses: int = 0
    bypasses: int = 0
    corruptions: int = 0
    read_errors: int = 0
    write_errors: int = 0
    capacity_evictions: int = 0
    expired_evictions: int = 0
    writes: int = 0

    @property
    def requests(self):
        return self.full_hits + self.partial_hits + self.misses

    @property
    def hit_rate(self):
        """Fraction of requests which reused the complete result."""
        if self.requests == 0:
            return 0.0
        return self.full_hits / self.requests

    @property
    def partial_hit_rate(self):
        if self.requests == 0:
            return 0.0
        return self.partial_hits / self.requests

    @property
    def reuse_rate(self):
        """Fraction of requests which reused all or part of a result."""
        if self.requests == 0:
            return 0.0
        return (self.full_hits + self.partial_hits) / self.requests


@dataclass(frozen=True)
class CacheKey:
    """A deterministic key plus information useful when debugging misses."""

    key: str
    source_fingerprint: str
    chain_digest: str
    operations: tuple
    source_id: object = None


@dataclass
class _Payload:
    entry: dict
    data: bytes


@dataclass
class _Node:
    operation: str
    digest: str
    dependencies: list


@dataclass
class _PendingCall:
    node: _Node


class _UncacheableArgument(Exception):
    pass


def _update_hash(digest, value):
    if value is None:
        digest.update(b'n')
    elif isinstance(value, bool):
        digest.update(b'b1' if value else b'b0')
    elif isinstance(value, int):
        data = str(value).encode('ascii')
        digest.update(b'i')
        digest.update(str(len(data)).encode('ascii'))
        digest.update(b':')
        digest.update(data)
    elif isinstance(value, float):
        # Make 1 and 1.0 equivalent, and avoid locale-dependent formatting.
        if math.isfinite(value) and value.is_integer():
            _update_hash(digest, int(value))
        else:
            data = value.hex().encode('ascii')
            digest.update(b'f')
            digest.update(str(len(data)).encode('ascii'))
            digest.update(b':')
            digest.update(data)
    elif isinstance(value, complex):
        _update_hash(digest, ('complex', value.real, value.imag))
    elif isinstance(value, str):
        data = value.encode('utf-8')
        digest.update(b's')
        digest.update(str(len(data)).encode('ascii'))
        digest.update(b':')
        digest.update(data)
    elif isinstance(value, (bytes, bytearray, memoryview)):
        data = bytes(value)
        digest.update(b'x')
        digest.update(str(len(data)).encode('ascii'))
        digest.update(b':')
        digest.update(data)
    elif isinstance(value, os.PathLike):
        _update_hash(digest, os.fspath(value))
    elif isinstance(value, (list, tuple)):
        digest.update(b'l')
        digest.update(str(len(value)).encode('ascii'))
        digest.update(b':')
        for item in value:
            _update_hash(digest, item)
    elif isinstance(value, dict):
        items = list(value.items())
        items.sort(key=lambda item: _canonical_item_key(item[0]))
        digest.update(b'd')
        digest.update(str(len(items)).encode('ascii'))
        digest.update(b':')
        for key, item in items:
            _update_hash(digest, key)
            _update_hash(digest, item)
    else:
        raise _UncacheableArgument(
            f'unsupported cache key value {type(value).__name__}')


def _canonical_item_key(value):
    digest = hashlib.sha256()
    _update_hash(digest, value)
    return digest.digest()


def _hash_parts(parts):
    digest = hashlib.sha256()
    digest.update(_ROOT_HASH_PREFIX)
    for part in parts:
        _update_hash(digest, part)
    return digest.hexdigest()


def _sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


def _sha256_file(filename):
    digest = hashlib.sha256()
    with open(filename, 'rb') as stream:
        while True:
            block = stream.read(1024 * 1024)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def _runtime_fingerprint():
    return (
        'pyvips=' + pyvips.__version__ +
        ';libvips=' + '.'.join(
            str(pyvips.version(i)) for i in range(3)
        ) +
        ';cache-schema=' + str(_SCHEMA_VERSION)
    )


def _pointer_key(image):
    if hasattr(pyvips, 'ffi'):
        return int(pyvips.ffi.cast('uintptr_t', image.pointer))
    return int(image.pointer)


def _split_option_text(text):
    if isinstance(text, bytes):
        text = text.decode('utf-8')
    if not isinstance(text, str):
        raise _UncacheableArgument('string_options must be text')

    text = text.strip()
    if text.startswith('[') and text.endswith(']'):
        text = text[1:-1]

    parts = []
    current = []
    quote = None
    escaped = False
    for char in text:
        if escaped:
            current.append(char)
            escaped = False
        elif char == '\\':
            escaped = True
        elif quote is not None:
            current.append(char)
            if char == quote:
                quote = None
        elif char in ('"', "'"):
            current.append(char)
            quote = char
        elif char == ',':
            parts.append(''.join(current).strip())
            current = []
        else:
            current.append(char)
    if current or text:
        parts.append(''.join(current).strip())

    return [part for part in parts if part]


def _unquote_option(value):
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
        return value[1:-1]
    return value


_INT_RE = re.compile(r'^[+-]?[0-9]+$')
_FLOAT_RE = re.compile(
    r'^[+-]?(?:[0-9]+\.[0-9]*|\.[0-9]+|[0-9]+)(?:[eE][+-]?[0-9]+)?$')


def _coerce_option_value(value):
    value = _unquote_option(value)
    lower = value.lower()
    if lower == 'true':
        return True
    if lower == 'false':
        return False
    if _INT_RE.match(value):
        try:
            return int(value)
        except ValueError:
            pass
    if _FLOAT_RE.match(value):
        try:
            return float(value)
        except ValueError:
            pass
    return value


def _parse_string_options(text):
    options = {}
    for part in _split_option_text(text):
        if '=' in part:
            name, value = part.split('=', 1)
            options[name.strip()] = _coerce_option_value(value.strip())
        else:
            options[part] = True
    return options


def _get_operation_defaults(operation_name):
    with _defaults_lock:
        if operation_name in _operation_defaults:
            return _operation_defaults[operation_name]

        defaults = {}
        intro = Introspect.get(operation_name)
        operation = Operation.new_from_name(operation_name)
        for name in intro.optional_input:
            try:
                value = operation.get(name)
                # Values which cannot be represented deterministically are
                # simply not materialised as defaults.
                digest = hashlib.sha256()
                _update_hash(digest, value)
            except Exception:
                continue
            defaults[name] = value

        _operation_defaults[operation_name] = defaults
        return defaults


def _encode_metadata_value(value):
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return {'__float__': 'nan'}
        if math.isinf(value):
            return {'__float__': 'inf' if value > 0 else '-inf'}
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {
            '__bytes__': base64.b64encode(bytes(value)).decode('ascii'),
        }
    if isinstance(value, (list, tuple)):
        return [_encode_metadata_value(item) for item in value]
    raise _UncacheableArgument(
        f'unsupported metadata value {type(value).__name__}')


def _decode_metadata_value(value):
    if isinstance(value, list):
        return [_decode_metadata_value(item) for item in value]
    if isinstance(value, dict):
        if '__bytes__' in value:
            return base64.b64decode(value['__bytes__'])
        if '__float__' in value:
            special = value['__float__']
            if special == 'nan':
                return float('nan')
            return float('inf') if special == 'inf' else float('-inf')
    return value


def _entry_source_id(source_id):
    if source_id is None or isinstance(source_id, (bool, int, float, str)):
        return source_id
    if isinstance(source_id, (bytes, bytearray, memoryview)):
        return {
            '__bytes__': base64.b64encode(bytes(source_id)).decode('ascii'),
        }
    return repr(source_id)


def _encode_metadata(image):
    metadata = {}
    complete = True
    for field in sorted(image.get_fields()):
        if field in _CORE_FIELDS:
            continue
        try:
            gtype = image.get_typeof(field)
            if gtype == 0:
                continue
            type_name = pyvips.type_name(gtype)
            if not type_name:
                raise _UncacheableArgument('unknown metadata gtype')
            value = image.get(field)
            metadata[field] = {
                'type': type_name,
                'value': _encode_metadata_value(value),
            }
        except Exception:
            complete = False
    return metadata, complete


def _image_descriptor(image):
    metadata, metadata_complete = _encode_metadata(image)
    descriptor = {
        'width': image.width,
        'height': image.height,
        'bands': image.bands,
        'format': image.format,
        'coding': image.coding,
        'interpretation': image.interpretation,
        'xoffset': image.xoffset,
        'yoffset': image.yoffset,
        'xres': image.xres,
        'yres': image.yres,
        'metadata': metadata,
        'metadata_complete': metadata_complete,
    }
    return descriptor, metadata_complete


def _source_data_digest(image, source_id):
    if source_id is not None:
        digest = hashlib.sha256()
        digest.update(b'caller-source-id\x00')
        _update_hash(digest, source_id)
        return 'id:' + digest.hexdigest(), True

    filename = None
    if image.get_typeof('filename') != 0:
        try:
            filename = image.get('filename')
        except Exception:
            filename = None

    if (isinstance(filename, str) and filename and
            os.path.isfile(filename)):
        try:
            return 'file:' + _sha256_file(filename), True
        except OSError:
            pass

    data = bytes(image.write_to_memory())
    return 'pixels:' + _sha256_bytes(data), True


def _image_fingerprint(image, source_id=None):
    descriptor, complete = _image_descriptor(image)
    data_digest, data_complete = _source_data_digest(image, source_id)
    fingerprint = _hash_parts((
        'image-fingerprint',
        data_digest,
        descriptor,
    ))
    return fingerprint, complete and data_complete


def _canonical_argument(recorder, name, value):
    if isinstance(value, pyvips.Image):
        token, node = recorder.token_for_image(value)
        dependencies = []
        if node is not None:
            dependencies.append(node)
        return ('image', token), dependencies

    if isinstance(value, (list, tuple)):
        items = []
        dependencies = []
        for item in value:
            canonical, item_deps = _canonical_argument(
                recorder, name, item)
            items.append(canonical)
            dependencies.extend(item_deps)
        return ('sequence', items), dependencies

    if isinstance(value, dict):
        items = []
        dependencies = []
        for key in sorted(value, key=lambda item: str(item)):
            canonical, item_deps = _canonical_argument(
                recorder, name, value[key])
            items.append((key, canonical))
            dependencies.extend(item_deps)
        return ('mapping', items), dependencies

    if (isinstance(value, (str, os.PathLike)) and
            name in ('filename', 'input_profile', 'output_profile',
                     'profile')):
        text = os.fspath(value)
        if text and os.path.isfile(text):
            try:
                return ('file-content', _sha256_file(text)), []
            except OSError:
                pass
        return ('path-text', text), []

    digest = hashlib.sha256()
    _update_hash(digest, value)
    return ('value', digest.hexdigest()), []


def _normalize_operation_call(recorder, operation_name, args, kwargs):
    intro = Introspect.get(operation_name)
    if len(args) != len(intro.required_input):
        # Let the real operation report the error.
        raise _UncacheableArgument('unexpected required argument count')

    dependencies = []
    required = []
    values = {}
    for name, value in zip(intro.required_input, args):
        canonical, deps = _canonical_argument(recorder, name, value)
        required.append((name, canonical))
        dependencies.extend(deps)
        values[name] = value

    string_options = kwargs.get('string_options', '')
    option_values = _parse_string_options(string_options)
    for name, value in option_values.items():
        canonical, deps = _canonical_argument(recorder, name, value)
        dependencies.extend(deps)
        values[name] = canonical

    for name, value in kwargs.items():
        if name == 'string_options':
            continue
        canonical, deps = _canonical_argument(recorder, name, value)
        dependencies.extend(deps)
        values[name] = canonical

    for name, value in _get_operation_defaults(operation_name).items():
        if name not in values:
            canonical, deps = _canonical_argument(recorder, name, value)
            dependencies.extend(deps)
            values[name] = canonical

    optional = []
    for name in intro.optional_input:
        if name in values:
            optional.append((name, values[name]))
    for name in intro.optional_output:
        if name in kwargs:
            canonical, deps = _canonical_argument(
                recorder, name, kwargs[name])
            dependencies.extend(deps)
            optional.append((name, canonical))

    optional.sort(key=lambda item: item[0])
    return {
        'operation': operation_name,
        'required': required,
        'optional': optional,
    }, dependencies


def _walk_images(value, path=()):
    if isinstance(value, pyvips.Image):
        yield path, value
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            yield from _walk_images(item, path + (('index', index),))
    elif isinstance(value, dict):
        for key in sorted(value, key=lambda item: str(item)):
            yield from _walk_images(value[key], path + (('key', key),))


class _Recorder:
    def __init__(self, source, source_id=None):
        self.cacheable = True
        self.source_id = source_id
        self.tokens = {}
        self.nodes = {}

        source_fingerprint, complete = _image_fingerprint(
            source, source_id=source_id)
        if not complete:
            self.cacheable = False
        self.source_fingerprint = source_fingerprint
        self.tokens[_pointer_key(source)] = (source_fingerprint, None)

    def token_for_image(self, image):
        pointer = _pointer_key(image)
        if pointer in self.tokens:
            return self.tokens[pointer]

        fingerprint, complete = _image_fingerprint(image)
        if not complete:
            self.cacheable = False
        token = _hash_parts(('external-image', fingerprint))
        self.tokens[pointer] = (token, None)
        return token, None

    def before_call(self, operation_name, args, kwargs):
        if not self.cacheable:
            return None

        try:
            normalized, dependencies = _normalize_operation_call(
                self, operation_name, args, kwargs)
            digest = _hash_parts(('operation', normalized))
            node = _Node(operation_name, digest, dependencies)
            return _PendingCall(node)
        except Exception:
            self.cacheable = False
            return None

    def after_call(self, pending, result):
        if not self.cacheable:
            return

        base = pending.node.digest
        for path, image in _walk_images(result):
            token = _hash_parts(('output', base, path))
            pointer = _pointer_key(image)
            self.tokens[pointer] = (token, pending.node)
            node = _Node(
                pending.node.operation, token, pending.node.dependencies)
            self.nodes[token] = node

    def finish(self, result):
        if not self.cacheable:
            raise _UncacheableArgument('pipeline used unsupported values')
        if not isinstance(result, pyvips.Image):
            raise _UncacheableArgument('pipeline did not return an Image')

        token_info = self.tokens.get(_pointer_key(result))
        if token_info is None:
            raise _UncacheableArgument('result is not an observed operation')
        token, final_node = token_info

        descriptor, complete = _image_descriptor(result)
        if not complete:
            raise _UncacheableArgument('result has unsupported metadata')

        digest = _hash_parts((
            'request', _runtime_fingerprint(), token, descriptor))
        operations = self._operation_summary(final_node)
        return CacheKey(
            key=digest,
            source_fingerprint=self.source_fingerprint,
            chain_digest=token,
            operations=operations,
            source_id=self.source_id,
        )

    def _operation_summary(self, final_node):
        if final_node is None:
            return ()

        result = []
        seen = set()

        def visit(node):
            for dependency in node.dependencies:
                visit(dependency)
            if node.digest not in seen:
                seen.add(node.digest)
                result.append((node.operation, node.digest))

        visit(final_node)
        return tuple(result)


class _Observer:
    def before_call(self, operation_name, args, kwargs):
        recorder = _active_recorder.get()
        if recorder is None:
            return None
        return recorder.before_call(operation_name, args, kwargs)

    def after_call(self, pending, result):
        recorder = _active_recorder.get()
        if recorder is None:
            return
        recorder.after_call(pending, result)


_OBSERVER = _Observer()


def _install_observer():
    global _observer_installed
    if _observer_installed:
        return
    with _observer_lock:
        if not _observer_installed:
            _set_operation_observer(_OBSERVER)
            _observer_installed = True


def _is_no_space(error):
    if isinstance(error, OSError) and error.errno == errno.ENOSPC:
        return True
    text = str(error).lower()
    return ('no space left' in text or 'enospc' in text or
            'disk full' in text)


class RenderCache:
    """Cache complete, lazy pyvips render pipelines.

    Args:
        directory: Directory used for cache entries.
        max_bytes: Maximum size of result data and metadata. ``None`` means no
            size limit.
        max_entries: Maximum number of entries. ``None`` means no count limit.
        max_age: Maximum entry age in seconds. ``None`` disables time expiry.
        enabled: Enable caching. The default is ``False``.

    The pipeline must be lazy. It should build and return a pyvips image
    without forcing evaluation. The first request evaluates and caches the
    result; later requests reconstruct it from an in-memory buffer.
    """

    def __init__(self, directory, *, max_bytes=512 * 1024 * 1024,
                 max_entries=None, max_age=None, enabled=False):
        if max_bytes is not None and max_bytes < 0:
            raise ValueError('max_bytes must be non-negative or None')
        if max_entries is not None and max_entries < 0:
            raise ValueError('max_entries must be non-negative or None')
        if max_age is not None and max_age < 0:
            raise ValueError('max_age must be non-negative or None')

        self.directory = os.fspath(directory)
        self._enabled = bool(enabled)
        self._max_bytes = max_bytes
        self._max_entries = max_entries
        self._max_age = max_age
        self._state_lock = threading.RLock()
        self._pins = {}
        self._deleting = set()
        self._inflight = {}
        self._stats = {name: 0 for name in _STAT_KEYS}

    @property
    def enabled(self):
        return self._enabled

    @enabled.setter
    def enabled(self, value):
        self._enabled = bool(value)

    @property
    def max_bytes(self):
        return self._max_bytes

    @property
    def max_entries(self):
        return self._max_entries

    @property
    def max_age(self):
        return self._max_age

    def set_max_bytes(self, max_bytes, *, reset_stats=True):
        """Change the byte limit and optionally start a fresh stats period."""
        if max_bytes is not None and max_bytes < 0:
            raise ValueError('max_bytes must be non-negative or None')
        with self._state_lock:
            self._max_bytes = max_bytes
            if reset_stats:
                self._reset_stats_locked()
        self.enforce_capacity()

    def set_max_entries(self, max_entries, *, reset_stats=True):
        """Change the entry limit and optionally reset statistics."""
        if max_entries is not None and max_entries < 0:
            raise ValueError('max_entries must be non-negative or None')
        with self._state_lock:
            self._max_entries = max_entries
            if reset_stats:
                self._reset_stats_locked()
        self.enforce_capacity()

    def set_max_age(self, max_age, *, reset_stats=True):
        """Change the age limit and optionally reset statistics."""
        if max_age is not None and max_age < 0:
            raise ValueError('max_age must be non-negative or None')
        with self._state_lock:
            self._max_age = max_age
            if reset_stats:
                self._reset_stats_locked()
        self.evict_expired()

    @property
    def stats(self):
        with self._state_lock:
            return CacheStats(**self._stats)

    def reset_stats(self):
        """Reset all statistics without changing cached entries."""
        with self._state_lock:
            self._reset_stats_locked()

    def _reset_stats_locked(self):
        for key in self._stats:
            self._stats[key] = 0

    def _bump(self, name, count=1):
        with self._state_lock:
            self._stats[name] += count

    def compute_key(self, source, pipeline, *, source_id=None):
        """Build the deterministic key for a source and lazy pipeline."""
        if not callable(pipeline):
            raise TypeError('pipeline must be callable')
        _install_observer()
        recorder = _Recorder(source, source_id=source_id)
        token = _active_recorder.set(recorder)
        try:
            result = pipeline(source)
        finally:
            _active_recorder.reset(token)
        return recorder.finish(result)

    def render(self, source, pipeline, *, source_id=None):
        """Return a cached image, computing it once when necessary.

        ``pipeline`` is called with ``source`` and must return a lazy
        :class:`pyvips.Image`.
        """
        if not self._enabled:
            return pipeline(source)
        if not callable(pipeline):
            raise TypeError('pipeline must be callable')
        if not self._ensure_directory():
            self._bump('bypasses')
            return pipeline(source)

        _install_observer()
        try:
            recorder = _Recorder(source, source_id=source_id)
        except Exception:
            logger.debug('render cache could not fingerprint source',
                         exc_info=True)
            self._bump('bypasses')
            return pipeline(source)

        token = _active_recorder.set(recorder)
        try:
            result = pipeline(source)
        finally:
            _active_recorder.reset(token)

        try:
            request = recorder.finish(result)
        except Exception:
            logger.debug('render cache bypassed an uncacheable request',
                         exc_info=True)
            self._bump('bypasses')
            return result

        return self._render_cached(request, result)

    get_or_render = render

    def _render_cached(self, request, result):
        payload = self._lookup(request.key)
        if payload is not None:
            materialized = self._materialize(payload)
            if materialized is not None:
                image, partial = materialized
                self._record_reuse(partial)
                return image

        with self._state_lock:
            future = self._inflight.get(request.key)
            leader = future is None
            if leader:
                future = concurrent.futures.Future()
                self._inflight[request.key] = future

        if not leader:
            try:
                shared = future.result()
            except Exception:
                shared = None
            if shared is not None:
                materialized = self._materialize(shared)
                if materialized is not None:
                    image, partial = materialized
                    self._record_reuse(partial)
                    return image
            self._bump('misses')
            return result

        payload = None
        try:
            with self._entry_lock(request.key):
                payload = self._lookup(request.key)
                if payload is not None:
                    materialized = self._materialize(payload)
                    if materialized is not None:
                        image, partial = materialized
                        future.set_result(payload)
                        self._record_reuse(partial)
                        return image

                payload = self._write_entry(request, result)
                future.set_result(payload)
                if payload is not None:
                    materialized = self._materialize(payload)
                    if materialized is not None:
                        # Returning the in-memory reconstruction also makes
                        # non-seekable sources safe to evaluate after the
                        # cache has consumed their lazy graph.
                        self._bump('misses')
                        return materialized[0]
        except Exception:
            logger.debug('render cache write path failed', exc_info=True)
            self._bump('write_errors')
            if not future.done():
                future.set_result(None)
        finally:
            with self._state_lock:
                if self._inflight.get(request.key) is future:
                    del self._inflight[request.key]

        self._bump('misses')
        return result

    def _record_reuse(self, partial):
        self._bump('partial_hits' if partial else 'full_hits')

    def _ensure_directory(self):
        try:
            os.makedirs(self.directory, exist_ok=True)
            return True
        except OSError:
            logger.debug('render cache directory is unavailable',
                         exc_info=True)
            return False

    def _entry_paths(self, key):
        return (
            os.path.join(self.directory, key + '.json'),
            os.path.join(self.directory, key + '.bin'),
            os.path.join(self.directory, key + '.lock'),
        )

    @contextlib.contextmanager
    def _pin(self, key):
        with self._state_lock:
            if key in self._deleting:
                yield False
                return
            self._pins[key] = self._pins.get(key, 0) + 1
        try:
            yield True
        finally:
            with self._state_lock:
                count = self._pins.get(key, 0) - 1
                if count > 0:
                    self._pins[key] = count
                else:
                    self._pins.pop(key, None)

    @contextlib.contextmanager
    def _entry_lock(self, key):
        _, _, lock_path = self._entry_paths(key)
        descriptor = None
        fcntl = None
        try:
            descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
            try:
                import fcntl as fcntl_module
                fcntl = fcntl_module
                fcntl.flock(descriptor, fcntl.LOCK_EX)
            except (ImportError, OSError):
                fcntl = None
        except OSError:
            descriptor = None

        try:
            yield
        finally:
            if descriptor is not None:
                if fcntl is not None:
                    try:
                        fcntl.flock(descriptor, fcntl.LOCK_UN)
                    except OSError:
                        pass
                try:
                    os.close(descriptor)
                except OSError:
                    pass

    def _lookup(self, key):
        bad_entry = False
        payload = None

        with self._pin(key) as pinned:
            if not pinned:
                return None

            metadata_path, data_path, _ = self._entry_paths(key)
            if os.path.exists(metadata_path):
                try:
                    with open(metadata_path, 'r',
                              encoding='utf-8') as stream:
                        entry = json.load(stream)

                    if (entry.get('schema') != _SCHEMA_VERSION or
                            entry.get('key') != key):
                        raise ValueError('cached entry header mismatch')

                    if self._is_expired(metadata_path):
                        self._bump('expired_evictions')
                        bad_entry = True
                        entry = None

                    if entry is not None:
                        result = entry.get('result', {})
                        expected = result.get('sha256')
                        expected_size = result.get('bytes')
                        if not isinstance(expected, str):
                            raise ValueError('cached result has no checksum')

                        with open(data_path, 'rb') as stream:
                            data = stream.read()
                        if (expected_size is not None and
                                len(data) != expected_size):
                            raise ValueError(
                                'cached result size mismatch')
                        if _sha256_bytes(data) != expected:
                            raise ValueError(
                                'cached result checksum mismatch')

                        payload = _Payload(entry, data)
                except OSError:
                    self._bump('read_errors')
                    bad_entry = True
                except (ValueError, TypeError, json.JSONDecodeError):
                    self._bump('corruptions')
                    bad_entry = True

                if payload is not None:
                    self._touch(metadata_path, data_path)

        if bad_entry:
            self._remove_entry(key, allow_inflight=True)
        return payload

    def _is_expired(self, metadata_path):
        if self._max_age is None:
            return False
        try:
            age = time.time() - os.path.getmtime(metadata_path)
        except OSError:
            return False
        return age > self._max_age

    def _touch(self, metadata_path, data_path):
        now = time.time()
        for filename in (metadata_path, data_path):
            try:
                os.utime(filename, (now, now))
            except OSError:
                pass

    def _materialize(self, payload):
        try:
            result = payload.entry['result']
            descriptor = result['image']
            image = pyvips.Image.new_from_memory(
                payload.data,
                descriptor['width'],
                descriptor['height'],
                descriptor['bands'],
                descriptor['format'],
            )

            copy_args = {}
            for name in _COPY_FIELDS:
                if name in descriptor:
                    copy_args[name] = descriptor[name]
            if copy_args:
                image = image.copy(**copy_args)

            missing = []
            for name, encoded in descriptor.get('metadata', {}).items():
                try:
                    gtype = pyvips.type_from_name(encoded['type'])
                    if gtype == 0:
                        raise ValueError('unknown metadata type')
                    image.set_type(
                        gtype, name, _decode_metadata_value(encoded['value']))
                except Exception:
                    missing.append(name)

            return image, bool(missing)
        except Exception:
            self._bump('corruptions')
            logger.debug('cached result could not be reconstructed',
                         exc_info=True)
            key = payload.entry.get('key')
            if key:
                self._remove_entry(key, allow_inflight=True)
            return None

    def _write_entry(self, request, image):
        try:
            descriptor, complete = _image_descriptor(image)
            if not complete:
                return None
            data = bytes(image.write_to_memory())
        except Exception:
            self._bump('write_errors')
            logger.debug('render result could not be serialised',
                         exc_info=True)
            return None

        metadata_path, data_path, _ = self._entry_paths(request.key)
        entry = {
            'schema': _SCHEMA_VERSION,
            'key': request.key,
            'source': {
                'fingerprint': request.source_fingerprint,
                'id': _entry_source_id(request.source_id),
            },
            'chain': {
                'digest': request.chain_digest,
                'operations': [
                    {'operation': name, 'digest': digest}
                    for name, digest in request.operations
                ],
            },
            'created': time.time(),
            'result': {
                'file': os.path.basename(data_path),
                'sha256': _sha256_bytes(data),
                'bytes': len(data),
                'image': descriptor,
            },
        }
        metadata = json.dumps(
            entry, sort_keys=True, separators=(',', ':')).encode('utf-8')
        entry_size = len(data) + len(metadata)

        if self._max_bytes is not None and entry_size > self._max_bytes:
            return _Payload(entry, data)
        if not self._enforce_capacity(
                reserve=entry_size, exclude=request.key):
            return _Payload(entry, data)

        for _ in range(3):
            try:
                self._atomic_write(data_path, data)
                self._atomic_write(metadata_path, metadata)
                self._bump('writes')
                self._enforce_capacity(exclude=request.key)
                return _Payload(entry, data)
            except Exception as error:
                self._remove_files(metadata_path, data_path)
                if not _is_no_space(error):
                    self._bump('write_errors')
                    logger.debug('render cache write failed', exc_info=True)
                    return _Payload(entry, data)
                if not self._evict_one(exclude=request.key):
                    return _Payload(entry, data)

        return _Payload(entry, data)

    def _atomic_write(self, target, data):
        descriptor, temporary = tempfile.mkstemp(
            dir=self.directory, prefix='.tmp-')
        try:
            with os.fdopen(descriptor, 'wb') as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, target)
        except Exception:
            try:
                os.unlink(temporary)
            except OSError:
                pass
            raise

    def _remove_files(self, *paths):
        for path in paths:
            try:
                os.unlink(path)
            except OSError:
                pass

    def _remove_entry(self, key, *, allow_inflight=False):
        with self._state_lock:
            if key in self._pins:
                return False
            if key in self._inflight and not allow_inflight:
                return False
            if key in self._deleting:
                return False
            self._deleting.add(key)
        try:
            metadata_path, data_path, _ = self._entry_paths(key)
            self._remove_files(metadata_path, data_path)
            return True
        finally:
            with self._state_lock:
                self._deleting.discard(key)

    def _evict_one(self, exclude=None):
        entries = self._scan_entries()
        for entry in entries:
            key = entry['key']
            if key == exclude:
                continue
            if self._remove_entry(key):
                self._bump('capacity_evictions')
                return True
        return False

    def _enforce_capacity(self, reserve=0, exclude=None):
        if self._max_bytes is None and self._max_entries is None:
            return True

        while True:
            entries = self._scan_entries()
            entries.sort(key=lambda item: item['mtime'])
            total = sum(item['size'] for item in entries)

            over_bytes = (
                self._max_bytes is not None and
                total + reserve > self._max_bytes
            )
            over_entries = (
                self._max_entries is not None and
                len(entries) >= self._max_entries and reserve > 0
            )
            if not over_bytes and not over_entries:
                return True

            victim = None
            for entry in entries:
                if entry['key'] == exclude:
                    continue
                with self._state_lock:
                    if (entry['key'] in self._pins or
                            entry['key'] in self._inflight or
                            entry['key'] in self._deleting):
                        continue
                victim = entry
                break

            if victim is None:
                return False
            if not self._remove_entry(victim['key']):
                return False
            self._bump('capacity_evictions')

    def _scan_entries(self):
        entries = []
        try:
            names = os.listdir(self.directory)
        except OSError:
            return entries

        for name in names:
            if not name.endswith('.json') or name.startswith('.tmp-'):
                continue
            key = name[:-5]
            metadata_path, data_path, _ = self._entry_paths(key)
            try:
                metadata_stat = os.stat(metadata_path)
                data_stat = os.stat(data_path)
                size = metadata_stat.st_size + data_stat.st_size
                mtime = max(metadata_stat.st_mtime, data_stat.st_mtime)
            except OSError:
                continue
            entries.append({
                'key': key,
                'size': size,
                'mtime': mtime,
            })
        return entries

    def enforce_capacity(self):
        """Immediately enforce the configured capacity limits."""
        return self._enforce_capacity()

    def evict_expired(self, now=None):
        """Immediately remove entries older than ``max_age``."""
        if self._max_age is None:
            return 0
        if now is None:
            now = time.time()

        removed = 0
        for entry in self._scan_entries():
            if now - entry['mtime'] <= self._max_age:
                continue
            if self._remove_entry(entry['key']):
                self._bump('expired_evictions')
                removed += 1
        return removed

    def cleanup(self):
        """Remove expired, over-capacity and orphaned files."""
        removed = self.evict_expired()
        self._enforce_capacity()

        try:
            names = os.listdir(self.directory)
        except OSError:
            return removed
        for name in names:
            path = os.path.join(self.directory, name)
            try:
                stale = time.time() - os.path.getmtime(path) > 60
                if name.startswith('.tmp-') and stale:
                    os.unlink(path)
                elif name.endswith('.lock') and stale:
                    key = name[:-5]
                    metadata_path, data_path, _ = self._entry_paths(key)
                    with self._state_lock:
                        active = (key in self._inflight or
                                  key in self._pins or
                                  key in self._deleting)
                    if (not active and
                            not os.path.exists(metadata_path) and
                            not os.path.exists(data_path)):
                        os.unlink(path)
                elif name.endswith('.json') and stale:
                    key = name[:-5]
                    _, data_path, _ = self._entry_paths(key)
                    if not os.path.exists(data_path):
                        os.unlink(path)
                elif name.endswith('.bin') and stale:
                    key = name[:-4]
                    metadata_path, _, _ = self._entry_paths(key)
                    if not os.path.exists(metadata_path):
                        os.unlink(path)
            except OSError:
                pass
        return removed

    def get_entry_info(self, key):
        """Return a copy of an entry's metadata, or ``None``."""
        metadata_path, _, _ = self._entry_paths(key)
        try:
            with open(metadata_path, 'r', encoding='utf-8') as stream:
                return json.load(stream)
        except (OSError, ValueError, json.JSONDecodeError):
            return None


__all__ = [
    'CacheKey',
    'CacheStats',
    'RenderCache',
]

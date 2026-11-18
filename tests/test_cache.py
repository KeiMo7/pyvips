import errno
import json
import os
import shutil
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pyvips
import pytest

from helpers import JPEG_FILE


def _source():
    image = pyvips.Image.black(96, 64, bands=3)
    return image.copy(interpretation='srgb')


def _pipeline(image):
    return image.resize(0.5).invert()


def _pixels(image):
    return bytes(image.copy_memory().write_to_memory())


class TestRenderCache:
    def test_disabled_is_a_transparent_passthrough(self, tmp_path):
        cache_dir = tmp_path / 'cache'
        cache = pyvips.RenderCache(cache_dir, enabled=False)

        result = cache.render(_source(), _pipeline)

        assert result.width == 48
        assert result.height == 32
        assert not cache_dir.exists()
        assert cache.stats.requests == 0

    def test_full_hit_reuses_result_without_touching_source_pixels(
            self, tmp_path, monkeypatch):
        cache = pyvips.RenderCache(tmp_path, enabled=True)
        source = pyvips.Image.new_from_file(JPEG_FILE)

        calls = 0
        original = pyvips.Image.write_to_memory

        def counted_write(self):
            nonlocal calls
            calls += 1
            return original(self)

        monkeypatch.setattr(pyvips.Image, 'write_to_memory',
                            counted_write)

        first = cache.render(source, _pipeline)
        assert calls == 1
        assert cache.stats.misses == 1
        assert cache.stats.writes == 1

        calls = 0
        second = cache.render(source, _pipeline)

        assert calls == 0
        assert _pixels(first) == _pixels(second)
        assert cache.stats.full_hits == 1
        assert cache.stats.hit_rate == 0.5

    def test_key_ignores_filename_but_tracks_content(self, tmp_path):
        first_dir = tmp_path / 'first'
        second_dir = tmp_path / 'second'
        first_dir.mkdir()
        second_dir.mkdir()
        first = first_dir / 'material.jpg'
        second = second_dir / 'renamed-copy.jpg'
        shutil.copyfile(JPEG_FILE, first)
        shutil.copyfile(JPEG_FILE, second)

        cache = pyvips.RenderCache(tmp_path / 'cache', enabled=True)
        source_a = pyvips.Image.new_from_file(first)
        source_b = pyvips.Image.new_from_file(second)

        key_a = cache.compute_key(source_a, _pipeline)
        key_b = cache.compute_key(source_b, _pipeline)

        assert key_a.key == key_b.key
        assert key_a.source_fingerprint == key_b.source_fingerprint

        changed = pyvips.Image.new_from_file(first).invert()
        key_c = cache.compute_key(changed, _pipeline)
        assert key_c.key != key_a.key

    def test_equivalent_argument_spellings_share_a_key(self, tmp_path):
        cache = pyvips.RenderCache(tmp_path, enabled=True)
        source = _source()

        def plain(image):
            return image.resize(0.5)

        def explicit_defaults(image):
            return image.resize(0.50, kernel='lanczos3')

        def string_option(image):
            return image.resize(0.5, string_options='kernel=lanczos3')

        def reordered(image):
            return image.embed(0, 0, 120, 80, background=0.0,
                               extend='background')

        def reordered_equivalent(image):
            return image.embed(0, 0, 120, 80, extend='background',
                               background=0)

        assert (cache.compute_key(source, plain).key ==
                cache.compute_key(source, explicit_defaults).key)
        assert (cache.compute_key(source, plain).key ==
                cache.compute_key(source, string_option).key)
        assert (cache.compute_key(source, reordered).key ==
                cache.compute_key(source, reordered_equivalent).key)

    def test_material_argument_and_chain_changes_never_hit(self, tmp_path):
        cache = pyvips.RenderCache(tmp_path, enabled=True)
        source = _source()

        first = cache.render(source, lambda image: image.resize(0.50))
        second = cache.render(source, lambda image: image.resize(0.51))
        third = cache.render(source, lambda image: image.resize(0.50).invert())

        assert cache.stats.full_hits == 0
        assert cache.stats.misses == 3
        assert _pixels(first) != _pixels(second)
        assert _pixels(first) != _pixels(third)

    def test_entry_has_source_and_chain_provenance(self, tmp_path):
        cache = pyvips.RenderCache(tmp_path, enabled=True)
        source = _source()
        request = cache.compute_key(source, _pipeline, source_id='asset:42')

        cache.render(source, _pipeline, source_id='asset:42')
        entry = cache.get_entry_info(request.key)

        assert entry['source']['fingerprint'] == request.source_fingerprint
        assert entry['source']['id'] == 'asset:42'
        assert entry['chain']['digest'] == request.chain_digest
        assert [item['operation'] for item in
                entry['chain']['operations']] == ['resize', 'invert']

    def test_missing_and_corrupt_results_fall_back_and_are_removed(
            self, tmp_path):
        cache = pyvips.RenderCache(tmp_path, enabled=True)
        source = _source()
        request = cache.compute_key(source, _pipeline)
        expected = _pixels(cache.render(source, _pipeline))

        data_path = tmp_path / (request.key + '.bin')
        metadata_path = tmp_path / (request.key + '.json')
        data_path.unlink()

        result = cache.render(source, _pipeline)
        assert _pixels(result) == expected
        assert cache.stats.read_errors == 1
        assert data_path.exists()

        data = data_path.read_bytes()
        data_path.write_bytes(bytes([data[0] ^ 0xff]) + data[1:])
        result = cache.render(source, _pipeline)
        assert _pixels(result) == expected
        assert cache.stats.corruptions == 1

        metadata_path.write_text('{not json', encoding='utf-8')
        result = cache.render(source, _pipeline)
        assert _pixels(result) == expected
        assert cache.stats.corruptions >= 2
        assert cache.get_entry_info(request.key) is not None

    def test_concurrent_identical_requests_are_coalesced(self, tmp_path):
        cache = pyvips.RenderCache(tmp_path, enabled=True)
        source = _source()
        workers = 8
        barrier = threading.Barrier(workers)

        def render(_):
            barrier.wait()
            return cache.render(source, _pipeline)

        with ThreadPoolExecutor(max_workers=workers) as executor:
            results = list(executor.map(render, range(workers)))

        assert all(_pixels(image) == _pixels(results[0])
                   for image in results)
        assert cache.stats.writes == 1
        assert cache.stats.misses == 1
        assert cache.stats.full_hits == workers - 1

    def test_capacity_eviction_and_stats_reset(self, tmp_path):
        cache = pyvips.RenderCache(
            tmp_path, enabled=True, max_bytes=30_000)
        source = _source()

        for scale in (0.70, 0.71, 0.72, 0.73, 0.74):
            cache.render(source, lambda image, value=scale:
                         image.resize(value))

        total = sum(
            entry['size']
            for entry in cache._scan_entries()
        )
        assert total <= cache.max_bytes
        assert cache.stats.capacity_evictions > 0
        assert cache.stats.requests == 5

        previous = cache.stats
        cache.set_max_bytes(15_000)
        assert cache.stats.requests == 0
        assert cache.stats.full_hits == 0
        assert cache.stats.misses == 0
        assert cache.stats.capacity_evictions <= previous.capacity_evictions

        cache.render(source, lambda image: image.resize(0.70))
        cache.render(source, lambda image: image.resize(0.70))
        assert cache.stats.requests == 2
        assert cache.stats.full_hits == 1
        assert cache.stats.hit_rate == 0.5

    def test_time_expiry_is_independent_from_capacity(self, tmp_path):
        cache = pyvips.RenderCache(
            tmp_path, enabled=True, max_bytes=10_000_000, max_age=0.05)
        source = _source()

        cache.render(source, _pipeline)
        cache.render(source, _pipeline)
        assert cache.stats.full_hits == 1

        time.sleep(0.08)
        cache.render(source, _pipeline)

        assert cache.stats.expired_evictions == 1
        assert cache.stats.misses == 2
        assert cache.stats.capacity_evictions == 0

    def test_partial_hit_is_not_counted_as_a_full_hit(self, tmp_path):
        cache = pyvips.RenderCache(tmp_path, enabled=True)
        source = _source()
        source.set_type(pyvips.GValue.gstr_type, 'cache-test-note', 'hello')
        request = cache.compute_key(source, _pipeline)
        cache.render(source, _pipeline)

        metadata_path = tmp_path / (request.key + '.json')
        entry = json.loads(metadata_path.read_text(encoding='utf-8'))
        entry['result']['image']['metadata']['cache-test-note']['type'] = \
            'NotARealType'
        metadata_path.write_text(json.dumps(entry), encoding='utf-8')

        cache.render(source, _pipeline)

        assert cache.stats.partial_hits == 1
        assert cache.stats.full_hits == 0
        assert cache.stats.reuse_rate == 0.5
        assert cache.stats.hit_rate == 0.0

    def test_write_failure_does_not_change_the_return_value(
            self, tmp_path, monkeypatch):
        cache = pyvips.RenderCache(tmp_path, enabled=True)
        source = _source()

        def fail(*args, **kwargs):
            raise OSError('permission denied')

        monkeypatch.setattr(cache, '_atomic_write', fail)
        result = cache.render(source, _pipeline)

        assert result.width == 48
        assert _pixels(result) == _pixels(source.resize(0.5).invert())
        assert cache.stats.write_errors == 1
        assert cache.stats.misses == 1

    def test_no_space_evicts_and_retries(self, tmp_path, monkeypatch):
        cache = pyvips.RenderCache(
            tmp_path, enabled=True, max_bytes=10_000_000)
        source = _source()
        cache.render(source, lambda image: image.resize(0.20))

        original = cache._atomic_write
        first_call = [True]

        def no_space_once(target, data):
            if first_call[0]:
                first_call[0] = False
                raise OSError(errno.ENOSPC, 'No space left on device')
            return original(target, data)

        monkeypatch.setattr(cache, '_atomic_write', no_space_once)
        result = cache.render(source, lambda image: image.resize(0.21))

        assert result.width == 20
        assert cache.stats.capacity_evictions == 1
        assert cache.stats.writes == 2

    def test_key_is_stable_across_processes(self, tmp_path):
        code = r'''
import pyvips
image = pyvips.Image.black(31, 17, bands=3)
cache = pyvips.RenderCache('.', enabled=False)
request = cache.compute_key(
    image,
    lambda image: image.resize(0.37, kernel='lanczos3').invert())
print(request.key)
'''
        env = dict(os.environ)
        root = os.path.dirname(os.path.dirname(__file__))
        env['PYTHONPATH'] = root + os.pathsep + env.get('PYTHONPATH', '')
        env['PYTHONHASHSEED'] = '1'
        first = subprocess.check_output(
            [sys.executable, '-c', code], cwd=tmp_path, env=env,
            text=True).strip()
        env['PYTHONHASHSEED'] = '987'
        second = subprocess.check_output(
            [sys.executable, '-c', code], cwd=tmp_path, env=env,
            text=True).strip()

        assert first == second

    def test_max_entries_limit(self, tmp_path):
        cache = pyvips.RenderCache(
            tmp_path, enabled=True, max_bytes=None, max_entries=1)
        source = _source()

        cache.render(source, lambda image: image.resize(0.20))
        cache.render(source, lambda image: image.resize(0.21))

        assert len(cache._scan_entries()) == 1
        assert cache.stats.capacity_evictions == 1

    def test_eviction_does_not_delete_a_pinned_entry(self, tmp_path):
        cache = pyvips.RenderCache(tmp_path, enabled=True)
        source = _source()
        request = cache.compute_key(source, _pipeline)
        cache.render(source, _pipeline)
        data_path = tmp_path / (request.key + '.bin')

        with cache._pin(request.key) as pinned:
            assert pinned
            assert not cache._remove_entry(request.key)

        assert data_path.exists()
        assert cache._remove_entry(request.key)
        assert not data_path.exists()

    def test_cleanup_removes_orphan_temp_and_lock_files(self, tmp_path):
        cache = pyvips.RenderCache(tmp_path, enabled=True)
        orphan_temp = tmp_path / '.tmp-orphan'
        orphan_lock = tmp_path / ('a' * 64 + '.lock')
        orphan_temp.write_bytes(b'x')
        orphan_lock.write_bytes(b'')
        old = time.time() - 120
        os.utime(orphan_temp, (old, old))
        os.utime(orphan_lock, (old, old))

        cache.cleanup()

        assert not orphan_temp.exists()
        assert not orphan_lock.exists()

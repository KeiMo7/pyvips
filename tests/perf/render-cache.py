#!/usr/bin/env python3
"""Compare repeated lazy renders with RenderCache disabled and enabled.

The source is a VipsSourceCustom so every byte read from the JPEG body can be
counted. Header reads are excluded from the decode count. A fixed source_id is
used to keep key generation free of image decoding.
"""

import argparse
import json
import os
import shutil
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, '..', '..'))
sys.path.insert(0, ROOT)

import pyvips  # noqa: E402


IMAGE = os.path.join(HERE, '..', 'images', 'sample.jpg')
HEADER_READS = 6


def run(enabled, count, root, raw):
    cache = pyvips.RenderCache(
        root, enabled=enabled, max_bytes=256 * 1024 * 1024)
    source_reads = 0
    checksum = 0

    started = time.perf_counter()
    for _ in range(count):
        position = [0]
        reads = [0]

        def read(length):
            chunk = raw[position[0]:position[0] + length]
            position[0] += len(chunk)
            reads[0] += 1
            return chunk

        source = pyvips.SourceCustom()
        source.on_read(read)
        image = pyvips.Image.new_from_source(
            source, '', access='sequential')

        def pipeline(image):
            return image.resize(0.35).invert()

        result = cache.render(image, pipeline, source_id='sample.jpg')
        pixels = bytes(result.write_to_memory())
        source_reads += max(0, reads[0] - HEADER_READS)
        checksum += sum(pixels[:100])

    elapsed = time.perf_counter() - started
    stats = cache.stats
    return {
        'enabled': enabled,
        'requests': count,
        'source_decode_reads': source_reads,
        'elapsed_seconds': elapsed,
        'checksum': checksum,
        'full_hits': stats.full_hits,
        'misses': stats.misses,
        'writes': stats.writes,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--requests', type=int, default=20)
    parser.add_argument('-r', '--runs', type=int, default=3)
    args = parser.parse_args()

    with open(IMAGE, 'rb') as stream:
        raw = stream.read()

    results = []
    for _ in range(args.runs):
        for enabled in (False, True):
            root = tempfile.mkdtemp(prefix='pyvips-render-cache-')
            try:
                results.append(run(enabled, args.requests, root, raw))
            finally:
                shutil.rmtree(root, ignore_errors=True)

    print(json.dumps(results, indent=2))


if __name__ == '__main__':
    main()

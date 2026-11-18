pyvips Benchmarks
=================

Prepare
-------

Prepare test-images::

    $ mkdir tmp/
    $ vips colourspace images/sample2.v tmp/t1.v srgb
    $ vips replicate tmp/t1.v tmp/t2.v 20 15
    $ vips extract_area tmp/t2.v tmp/x.tif[tile] 0 0 5000 5000
    $ vips copy tmp/x.tif tmp/x.jpg
    $ vipsheader tmp/x.tif


Run
---

::

    # tune your system to run stable benchmarks
    $ python3 -m pyperf system tune

    $ python3 vips-bench.py -o vips-bench.json
    $ python3 -m pyperf stats vips-bench.json

    $ python3 operation-call.py -o operation-call.json
    $ python3 -m pyperf stats operation-call.json

    # command to test if a difference is significant
    $ python3 -m pyperf compare_to operation-call2.json operation-call.json --table

RenderCache benchmark
---------------------

This compares the same repeated lazy resize/invert pipeline with caching off
and on. It counts source decode reads with ``VipsSourceCustom`` and reports
wall-clock time, hit statistics and a result checksum::

    $ python3 render-cache.py -n 20 -r 3

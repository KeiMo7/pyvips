.. include global.rst

.. py:currentmodule:: pyvips.batch

``batch`` -- bit-depth aware batch processing
=============================================

The :mod:`pyvips.batch` module adds a 10-bit capable pipeline on top of the
plain libvips load / save path.  With no new arguments it is exactly the
old 8-bit behaviour; the 10-bit path is enabled by the input samples or by
an explicit argument.

.. code-block:: python

    import pyvips.batch

    # old behaviour, unchanged: 8-bit in, 8-bit out
    pyvips.batch.process_image('in.jpg', 'out.png')

    # migration example: opt a mixed batch into explicit 10-bit output
    # with ordered dithering on the 8-bit items
    depths = pyvips.batch.process_batch(
        ['a.jpg', 'b.png', 'c.jpg'],
        ['a.png', 'b.png', 'c.png'],
        bit_depth=10, dither=pyvips.batch.DITHER_ORDERED)

Defaults for the new options (``bit_depth=None``, ``dither='none'``,
``pixel_format=None``) reproduce the previous behaviour, so existing
callers need no changes.

Failures raise :class:`BitDepthError` (a subclass of
:class:`pyvips.Error`) and never leave a partial output file behind.
Log and exception messages contain basenames only, never absolute paths.

.. autoclass:: BitDepthError

.. autofunction:: detect_bit_depth

.. autofunction:: convert_bit_depth

.. autofunction:: convert_pixel_format

.. autofunction:: process_image

.. autofunction:: process_batch

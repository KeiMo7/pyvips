# vim: set fileencoding=utf-8 :
# bit-depth aware batch image processing

"""Batch image processing with an explicit 8-bit / 10-bit pipeline.

The default path is the familiar libvips 8-bit one: load, save, no casts.
The 10-bit path is enabled by the input (a ushort image with samples above
the 8-bit range) or by an explicit ``bit_depth`` / ``pixel_format``
argument.  Intermediate buffers always match the target depth; nothing is
silently truncated to 8-bit.

The module keeps no state, so callers may process items concurrently from
threads; each item runs its own load / convert / save cycle and shares
nothing with its neighbours.

"""

import logging
import os
import re
import tempfile

import pyvips
from pyvips import Error

logger = logging.getLogger(__name__)

#: bit depths supported by the pipeline
BIT_DEPTH_8 = 8
BIT_DEPTH_10 = 10

#: dither strategies for bit-depth conversion
DITHER_NONE = 'none'
DITHER_ORDERED = 'ordered'

_DITHERS = (DITHER_NONE, DITHER_ORDERED)

# 10-bit samples are carried in ushort images in the range 0..1023
_MAX_SAMPLE = {BIT_DEPTH_8: 255, BIT_DEPTH_10: 1023}
_SHIFT = BIT_DEPTH_10 - BIT_DEPTH_8

#: explicit pixel format conversions; the encoder default is never used.
#: 'depth' is the pipeline bit depth the format carries (10-bit formats are
#: stored in 16-bit containers)
PIXEL_FORMATS = {
    'rgb24': {'bands': 3, 'depth': BIT_DEPTH_8, 'interpretation': 'srgb'},
    'gray8': {'bands': 1, 'depth': BIT_DEPTH_8, 'interpretation': 'b-w'},
    'rgb48': {'bands': 3, 'depth': BIT_DEPTH_10, 'interpretation': 'rgb16'},
    'gray16': {'bands': 1, 'depth': BIT_DEPTH_10, 'interpretation': 'grey16'},
    'yuv420p10le': {'bands': 3, 'depth': BIT_DEPTH_10,
                    'interpretation': 'multiband'},
}

#: savers and the pipeline bit depths each container can carry
_SAVE_OPS = {
    '.png': ('pngsave', (BIT_DEPTH_8, BIT_DEPTH_10)),
    '.tif': ('tiffsave', (BIT_DEPTH_8, BIT_DEPTH_10)),
    '.tiff': ('tiffsave', (BIT_DEPTH_8, BIT_DEPTH_10)),
    '.jpg': ('jpegsave', (BIT_DEPTH_8,)),
    '.jpeg': ('jpegsave', (BIT_DEPTH_8,)),
    '.webp': ('webpsave', (BIT_DEPTH_8,)),
    '.heif': ('heifsave', (BIT_DEPTH_8, BIT_DEPTH_10)),
    '.avif': ('heifsave', (BIT_DEPTH_8, BIT_DEPTH_10)),
}

# a 2x2 Bayer matrix in the low two bits of a 10-bit sample
_BAYER_2X2 = [[0, 2], [3, 1]]

# anything that looks like an absolute unix path
_ABS_PATH = re.compile(r'(?<![\w.=-])(/[^\s:()"\']+)+')


class BitDepthError(Error):
    """A bit-depth decision or conversion failed.

    A subclass of :class:`pyvips.Error`, so existing ``except pyvips.Error``
    handlers keep working.

    """


def _display_name(path):
    """A path safe for logs and errors: the basename only, never absolute."""
    return os.path.basename(os.fspath(path)) or '<stream>'


def _sanitize(text, paths=()):
    """Strip absolute paths and user data from a message."""
    text = str(text)
    for path in paths:
        if path:
            text = text.replace(os.fspath(path), _display_name(path))
    return _ABS_PATH.sub('<path>', text)


def _ordered_tile(width, height):
    """A deterministic ordered-dither pattern, values 0..3."""
    tile = pyvips.Image.new_from_array(_BAYER_2X2).cast('ushort')
    return tile.replicate((width + 1) // 2, (height + 1) // 2) \
               .crop(0, 0, width, height)


def detect_bit_depth(image, declared=None):
    """Detect the pipeline bit depth of an image.

    uchar images are 8-bit.  ushort images whose samples exceed the 8-bit
    range are 10-bit; anything else fits in 8 bits without loss.  If
    ``declared`` is given and does not match the samples, raise
    :class:`BitDepthError` rather than trusting the declaration.

    """
    if image.format == 'uchar':
        detected = BIT_DEPTH_8
    elif image.format == 'ushort':
        detected = BIT_DEPTH_8 if image.max() <= _MAX_SAMPLE[BIT_DEPTH_8] \
            else BIT_DEPTH_10
    else:
        raise BitDepthError(
            f'unsupported sample format {image.format}',
            detail='only uchar and ushort images can enter the pipeline; '
                   'convert explicitly first')

    if declared is not None and declared != detected:
        raise BitDepthError(
            f'declared bit depth {declared} does not match detected '
            f'{detected}',
            detail='samples in the input exceed the declared range')

    return detected


def convert_bit_depth(image, target, dither=DITHER_NONE):
    """Convert an image to a target bit depth via an explicit path.

    8 to 10 shifts samples left by two bits, optionally adding an ordered
    dither in the low bits to hide steps on gradients.  10 to 8 rounds to
    nearest.  Images already at the target depth pass through untouched.

    """
    if dither not in _DITHERS:
        raise BitDepthError(
            f'unknown dither strategy {dither!r}',
            detail='supported strategies: ' + ', '.join(_DITHERS))

    if target == BIT_DEPTH_8:
        if image.format == 'uchar':
            return image
        if dither == DITHER_ORDERED:
            out = (image + _ordered_tile(image.width, image.height)) \
                >> _SHIFT
        else:
            out = (image + 2) >> _SHIFT
        return out.cast('uchar') \
            .copy(interpretation='b-w' if image.bands < 3 else 'srgb')

    if target == BIT_DEPTH_10:
        if image.format == 'ushort':
            if image.interpretation in ('grey16', 'rgb16'):
                return image
            # keep the samples, fix the metadata so 16-bit savers do not
            # silently write 8-bit
            return image.copy(
                interpretation='grey16' if image.bands < 3 else 'rgb16')
        out = image.cast('ushort') << _SHIFT
        if dither == DITHER_ORDERED:
            out = (out + _ordered_tile(image.width, image.height)) \
                .cast('ushort')
        # 16-bit savers need a 16-bit interpretation to avoid an 8-bit write
        return out.copy(
            interpretation='grey16' if image.bands < 3 else 'rgb16')

    raise BitDepthError(
        f'unsupported target bit depth {target}',
        detail=f'use {BIT_DEPTH_8} or {BIT_DEPTH_10}')


def _to_ycbcr_10bit(image):
    """Explicit full-range BT.601 RGB to YCbCr conversion, 10-bit samples."""
    if image.bands < 3:
        raise BitDepthError(
            'yuv420p10le needs a 3-band input',
            detail='convert the image to RGB before selecting a YUV pixel '
                   'format')
    red, green, blue = image[0], image[1], image[2]
    luma = red * 0.299 + green * 0.587 + blue * 0.114
    chroma_b = (blue - luma) * 0.564 + 512.5
    chroma_r = (red - luma) * 0.713 + 512.5
    return luma.bandjoin([chroma_b, chroma_r]).cast('ushort')


def convert_pixel_format(image, pixel_format):
    """Convert an image to an explicit pixel format.

    Raises :class:`BitDepthError` for unknown formats instead of letting the
    encoder pick a default.

    """
    try:
        spec = PIXEL_FORMATS[pixel_format]
    except KeyError:
        raise BitDepthError(
            f'unknown pixel format {pixel_format!r}',
            detail='supported formats: ' + ', '.join(sorted(PIXEL_FORMATS)))

    out = image
    if out.bands < spec['bands']:
        # broadcast a mono image to colour
        out = out.bandjoin([out] * (spec['bands'] - out.bands))
    elif out.bands > spec['bands']:
        # drop extra bands such as alpha
        out = out[:spec['bands']]

    if pixel_format == 'yuv420p10le':
        out = _to_ycbcr_10bit(out)
    elif spec['bands'] == 1 and image.bands >= 3:
        out = (image[0] * 0.299 + image[1] * 0.587 + image[2] * 0.114) \
            .cast(image.format)

    return out.copy(interpretation=spec['interpretation'])


def _check_intermediate(image, depth):
    """The intermediate buffer must match the pipeline depth exactly."""
    want = 'uchar' if depth == BIT_DEPTH_8 else 'ushort'
    if image.format != want:
        raise BitDepthError(
            f'intermediate buffer format {image.format} does not match the '
            f'{depth}-bit pipeline',
            detail='conversion must produce an explicit sample format '
                   'before save')


def _save_op_for(ext, depth):
    try:
        op_name, depths = _SAVE_OPS[ext]
    except KeyError:
        raise BitDepthError(
            f'no saver registered for extension {ext!r}',
            detail='supported extensions: ' + ', '.join(sorted(_SAVE_OPS)))
    if pyvips.type_find('VipsOperation', op_name) == 0:
        raise BitDepthError(
            f'saver {op_name} is not available in this libvips build',
            detail='install libvips with the matching loader to save this '
                   'format')
    if depth not in depths:
        alternatives = sorted(
            e for e, (op, ds) in _SAVE_OPS.items()
            if depth in ds and pyvips.type_find('VipsOperation', op) != 0)
        raise BitDepthError(
            f'{ext} does not support {depth}-bit output',
            detail='refusing to silently drop to 8-bit; save as one of: '
                   + ', '.join(alternatives))
    return op_name


def _save_atomic(image, target, depth, save_options):
    """Save via a temp file, so a failure never leaves a partial image."""
    ext = os.path.splitext(os.fspath(target))[1].lower()
    op_name = _save_op_for(ext, depth)
    options = dict(save_options)
    if op_name == 'heifsave' and depth == BIT_DEPTH_10:
        # explicit, never the encoder default
        options.setdefault('bitdepth', 10)

    fd, temp_name = tempfile.mkstemp(
        dir=os.path.dirname(os.path.abspath(os.fspath(target))),
        prefix='.tmp-', suffix=ext)
    os.close(fd)
    try:
        getattr(image, op_name)(temp_name, **options)
        os.replace(temp_name, target)
    except BaseException:
        try:
            os.unlink(temp_name)
        except OSError:
            pass
        raise


def process_image(source, target, *, bit_depth=None, dither=DITHER_NONE,
                  pixel_format=None, declared_bit_depth=None,
                  **save_options):
    """Process one image and return the output bit depth.

    With no new arguments this is the plain 8-bit libvips path: load, save,
    no casts.  A 10-bit input enables the 10-bit path by itself; ``bit_depth``
    forces a depth, ``dither`` picks the conversion dither strategy and
    ``pixel_format`` forces an explicit sample layout.

    """
    name = _display_name(target)
    paths = (source, target)
    logger.info('process_image: enter item=%s', name)
    try:
        image = pyvips.Image.new_from_file(source)
        detected = detect_bit_depth(image, declared=declared_bit_depth)

        depth = detected if bit_depth is None else bit_depth
        if pixel_format is not None:
            spec_depth = PIXEL_FORMATS.get(pixel_format, {}).get('depth')
            if spec_depth is not None:
                if bit_depth is not None and bit_depth != spec_depth:
                    raise BitDepthError(
                        f'pixel format {pixel_format!r} carries '
                        f'{spec_depth}-bit samples, not {bit_depth}',
                        detail='drop bit_depth or pick a matching '
                               'pixel_format')
                depth = spec_depth
        logger.info('process_image: depth item=%s detected=%d target=%d',
                    name, detected, depth)

        image = convert_bit_depth(image, depth, dither=dither)
        if pixel_format is not None:
            image = convert_pixel_format(image, pixel_format)
            if pixel_format == 'yuv420p10le':
                ext = os.path.splitext(os.fspath(target))[1].lower()
                if ext not in ('.heif', '.avif'):
                    raise BitDepthError(
                        'yuv420p10le output needs a HEIF/AVIF container',
                        detail='save as .heif or .avif, or pick pixel '
                               'format rgb48 for still-image containers')
        _check_intermediate(image, depth)

        _save_atomic(image, target, depth, save_options)
    except BitDepthError as e:
        logger.error('process_image: failed item=%s error=%s',
                     name, e.message)
        raise
    except Error as e:
        safe = Error(_sanitize(e.message, paths),
                     detail=_sanitize(e.detail, paths))
        logger.error('process_image: failed item=%s error=%s',
                     name, safe.message)
        raise safe from e
    except OSError as e:
        safe = Error(_sanitize(str(e), paths))
        logger.error('process_image: failed item=%s error=%s',
                     name, safe.message)
        raise safe from e

    logger.info('process_image: exit item=%s depth=%d', name, depth)
    return depth


def process_batch(sources, targets, **options):
    """Process a batch of images, returning the output depth of each item.

    Every item decides its own depth from its own samples; a mixed 8-bit /
    10-bit batch is never flattened to the lowest common depth.

    """
    if len(sources) != len(targets):
        raise BitDepthError(
            f'sources and targets differ in length ({len(sources)} != '
            f'{len(targets)})',
            detail='pass one target per source')
    return [process_image(source, target, **options)
            for source, target in zip(sources, targets)]


__all__ = [
    'BIT_DEPTH_8',
    'BIT_DEPTH_10',
    'DITHER_NONE',
    'DITHER_ORDERED',
    'PIXEL_FORMATS',
    'BitDepthError',
    'detect_bit_depth',
    'convert_bit_depth',
    'convert_pixel_format',
    'process_image',
    'process_batch',
]

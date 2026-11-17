# vim: set fileencoding=utf-8 :
# tests for the bit-depth aware batch pipeline

import logging
import os
from concurrent.futures import ThreadPoolExecutor

import pytest

import pyvips
from pyvips import batch


def _gradient_8bit(path, width=256, height=8):
    ramp = pyvips.Image.xyz(width, height)[0].cast('uchar')
    image = ramp.bandjoin([ramp, ramp]).copy(interpretation='srgb')
    image.pngsave(str(path))
    return str(path)


def _gradient_10bit(path, width=1024, height=8):
    # a smooth 0..1023 ramp, one value per column
    ramp = pyvips.Image.xyz(width, height)[0].cast('ushort')
    image = ramp.bandjoin([ramp, ramp]).copy(interpretation='rgb16')
    image.pngsave(str(path))
    return str(path)


def _distinct_values(path):
    image = pyvips.Image.new_from_file(str(path))[0]
    hist = image.hist_find()
    return (hist > 0).sum()


class TestBatch:
    def test_passthrough_10bit(self, tmp_path):
        source = _gradient_10bit(tmp_path / 'in.png')
        target = tmp_path / 'out.png'

        depth = batch.process_image(source, str(target))

        assert depth == 10
        out = pyvips.Image.new_from_file(str(target))
        assert out.format == 'ushort'
        assert out.max() > 255
        # lossless container: samples survive untouched
        original = pyvips.Image.new_from_file(source)
        assert (original - out).abs().max() == 0

    def test_upgrade_8_to_10(self, tmp_path):
        source = _gradient_8bit(tmp_path / 'in.png')
        target = tmp_path / 'out.png'

        depth = batch.process_image(source, str(target), bit_depth=10)

        assert depth == 10
        out = pyvips.Image.new_from_file(str(target))
        assert out.format == 'ushort'
        # exact left shift, no artefacts
        original = pyvips.Image.new_from_file(source).cast('ushort')
        assert (out[0] - (original[0] << 2)).abs().max() == 0

    def test_upgrade_dither_ordered(self, tmp_path):
        source = _gradient_8bit(tmp_path / 'in.png')
        plain = tmp_path / 'plain.png'
        dithered = tmp_path / 'dithered.png'

        batch.process_image(source, str(plain), bit_depth=10)
        batch.process_image(source, str(dithered), bit_depth=10,
                            dither=batch.DITHER_ORDERED)

        # the dither fills the low two bits instead of leaving them zero
        assert _distinct_values(dithered) > 2 * _distinct_values(plain)
        out = pyvips.Image.new_from_file(str(dithered))[0]
        base = pyvips.Image.new_from_file(source)[0].cast('ushort') << 2
        # dither stays within the low two bits of the shifted sample
        assert (out - base).min() >= 0
        assert (out - base).max() <= 3

    def test_mixed_batch(self, tmp_path):
        sources = [
            _gradient_8bit(tmp_path / 'a.png'),
            _gradient_10bit(tmp_path / 'b.png'),
            _gradient_8bit(tmp_path / 'c.png'),
        ]
        targets = [str(tmp_path / name) for name in
                   ('oa.png', 'ob.png', 'oc.png')]

        depths = batch.process_batch(sources, targets)

        # each item is judged on its own samples, not the batch minimum
        assert depths == [8, 10, 8]
        assert pyvips.Image.new_from_file(targets[0]).format == 'uchar'
        assert pyvips.Image.new_from_file(targets[1]).format == 'ushort'
        assert pyvips.Image.new_from_file(targets[2]).format == 'uchar'

    def test_default_8bit_unchanged(self, tmp_path):
        source = _gradient_8bit(tmp_path / 'in.png')
        target = tmp_path / 'out.png'
        reference = tmp_path / 'ref.png'

        depth = batch.process_image(source, str(target))
        pyvips.Image.new_from_file(source).write_to_file(str(reference))

        assert depth == 8
        with open(str(target), 'rb') as f:
            actual = f.read()
        with open(str(reference), 'rb') as f:
            assert actual == f.read()

    def test_banding_gradient(self, tmp_path):
        # a smooth 10-bit ramp must stay smooth end to end
        source = _gradient_10bit(tmp_path / 'in.png')
        target = tmp_path / 'out.png'
        batch.process_image(source, str(target))

        out = pyvips.Image.new_from_file(str(target))[0]
        width = out.width
        diffs = out.crop(1, 0, width - 1, out.height) - \
            out.crop(0, 0, width - 1, out.height)
        assert diffs.abs().max() <= 1
        # and it keeps far more levels than an 8-bit version of the ramp
        target8 = tmp_path / 'out8.png'
        batch.process_image(source, str(target8), bit_depth=8)
        assert _distinct_values(target) > 3 * _distinct_values(target8)

    def test_no_silent_downgrade(self, tmp_path):
        source = _gradient_10bit(tmp_path / 'in.png')

        with pytest.raises(batch.BitDepthError) as excinfo:
            batch.process_image(source, str(tmp_path / 'out.jpg'))

        # the error names usable alternatives
        assert '.png' in excinfo.value.detail
        assert not (tmp_path / 'out.jpg').exists()

    def test_explicit_downconvert_rounds(self, tmp_path):
        source = _gradient_10bit(tmp_path / 'in.png')
        target = tmp_path / 'out.png'

        depth = batch.process_image(source, str(target), bit_depth=8)

        assert depth == 8
        out = pyvips.Image.new_from_file(str(target))
        assert out.format == 'uchar'
        original = pyvips.Image.new_from_file(source)[0]
        expected = ((original + 2) >> 2).cast('uchar')
        assert (out[0] - expected).abs().max() == 0

    def test_declared_depth_mismatch(self, tmp_path):
        source = _gradient_10bit(tmp_path / 'in.png')

        with pytest.raises(batch.BitDepthError):
            batch.process_image(source, str(tmp_path / 'out.png'),
                                declared_bit_depth=8)

    def test_unsupported_sample_format(self):
        image = pyvips.Image.black(4, 4).cast('float')
        with pytest.raises(batch.BitDepthError):
            batch.detect_bit_depth(image)

    def test_failed_save_leaves_no_partial(self, tmp_path):
        source = _gradient_10bit(tmp_path / 'in.png')
        target = tmp_path / 'out.png'

        with pytest.raises(pyvips.Error):
            batch.process_image(source, str(target), no_such_option=1)

        assert not target.exists()
        assert [name for name in os.listdir(str(tmp_path))
                if name.startswith('.tmp-')] == []

    def test_no_path_leak_in_logs_and_errors(self, tmp_path, caplog):
        secret_dir = tmp_path / 'secret-token-7f3a9c'
        secret_dir.mkdir()
        missing = secret_dir / 'missing.png'
        target = secret_dir / 'out.png'

        with caplog.at_level(logging.INFO, logger='pyvips.batch'):
            with pytest.raises(pyvips.Error) as excinfo:
                batch.process_image(str(missing), str(target))

        leaked = str(excinfo.value) + ''.join(
            record.getMessage() for record in caplog.records)
        assert str(secret_dir) not in leaked
        assert 'secret-token-7f3a9c' not in leaked
        assert str(tmp_path) not in leaked

    def test_logs_depth_decision(self, tmp_path, caplog):
        source = _gradient_10bit(tmp_path / 'in.png')

        with caplog.at_level(logging.INFO, logger='pyvips.batch'):
            batch.process_image(source, str(tmp_path / 'out.png'))

        messages = [record.getMessage() for record in caplog.records]
        assert any('enter' in message for message in messages)
        assert any('detected=10' in message for message in messages)
        assert any('exit' in message for message in messages)

    def test_output_size_reasonable(self, tmp_path):
        source = _gradient_10bit(tmp_path / 'in.png')
        target10 = tmp_path / 'out10.png'
        target8 = tmp_path / 'out8.png'
        batch.process_image(source, str(target10))
        batch.process_image(source, str(target8), bit_depth=8)

        size10 = os.path.getsize(str(target10))
        size8 = os.path.getsize(str(target8))
        assert size10 < 4 * size8

    def test_pixel_format_explicit(self, tmp_path):
        source = _gradient_8bit(tmp_path / 'in.png')
        target = tmp_path / 'out.png'

        batch.process_image(source, str(target), pixel_format='rgb24')

        out = pyvips.Image.new_from_file(str(target))
        assert out.bands == 3
        assert out.format == 'uchar'

    def test_pixel_format_unknown(self, tmp_path):
        source = _gradient_8bit(tmp_path / 'in.png')

        with pytest.raises(batch.BitDepthError) as excinfo:
            batch.process_image(source, str(tmp_path / 'out.png'),
                                pixel_format='yuv444p99le')

        assert 'rgb24' in excinfo.value.detail

    def test_pixel_format_depth_conflict(self, tmp_path):
        source = _gradient_8bit(tmp_path / 'in.png')

        with pytest.raises(batch.BitDepthError):
            batch.process_image(source, str(tmp_path / 'out.png'),
                                bit_depth=8, pixel_format='rgb48')

    def test_yuv420p10le_conversion(self):
        image = pyvips.Image.black(4, 4, bands=3).cast('ushort')
        image = (image + 512).cast('ushort').copy(interpretation='rgb16')

        out = batch.convert_pixel_format(image, 'yuv420p10le')

        assert out.format == 'ushort'
        assert out.bands == 3
        # mid grey has neutral chroma and luma at the input level
        assert abs(out[0].avg() - 512) < 1
        assert abs(out[1].avg() - 512) < 1
        assert abs(out[2].avg() - 512) < 1

    def test_yuv420p10le_needs_heif(self, tmp_path):
        source = _gradient_10bit(tmp_path / 'in.png')

        with pytest.raises(batch.BitDepthError) as excinfo:
            batch.process_image(source, str(tmp_path / 'out.png'),
                                pixel_format='yuv420p10le')

        assert '.heif' in excinfo.value.detail

    def test_convert_bit_depth_intermediate_format(self):
        # regression guard: the 10-bit path must never carry uchar buffers
        image = pyvips.Image.black(4, 4, bands=3).copy(interpretation='srgb')
        out = batch.convert_bit_depth(image, 10)
        assert out.format == 'ushort'
        assert out.interpretation == 'rgb16'
        with pytest.raises(batch.BitDepthError):
            batch._check_intermediate(image, 10)

    def test_batch_length_mismatch(self, tmp_path):
        with pytest.raises(batch.BitDepthError):
            batch.process_batch(['a.png'], [])

    def test_unknown_dither(self, tmp_path):
        source = _gradient_8bit(tmp_path / 'in.png')
        with pytest.raises(batch.BitDepthError):
            batch.process_image(source, str(tmp_path / 'out.png'),
                                bit_depth=10, dither='random')

    def test_concurrent_batch(self, tmp_path):
        sources = [
            _gradient_8bit(tmp_path / 'a.png'),
            _gradient_10bit(tmp_path / 'b.png'),
            _gradient_8bit(tmp_path / 'c.png'),
            _gradient_10bit(tmp_path / 'd.png'),
        ]
        targets = [str(tmp_path / name) for name in
                   ('oa.png', 'ob.png', 'oc.png', 'od.png')]

        # items are independent, so threads must not change the outcome
        with ThreadPoolExecutor(max_workers=2) as pool:
            depths = list(pool.map(
                lambda pair: batch.process_image(pair[0], pair[1]),
                zip(sources, targets)))

        assert depths == [8, 10, 8, 10]

    @pytest.mark.skipif(
        pyvips.type_find('VipsOperation', 'heifsave') == 0,
        reason='no heifsave')
    def test_heif_10bit(self, tmp_path):
        source = _gradient_10bit(tmp_path / 'in.png')
        target = tmp_path / 'out.heif'
        try:
            depth = batch.process_image(source, str(target),
                                        compression='av1')
        except pyvips.Error:
            pytest.skip('this libheif cannot write 10-bit')
        assert depth == 10
        out = pyvips.Image.new_from_file(str(target))
        assert out.format == 'ushort'

# vim: set fileencoding=utf-8 :

import hashlib
import json

import pyvips
import pytest

import pyvips.color_batch as color_batch
from helpers import JPEG_FILE, skip_if_no


@skip_if_no("tiffsave")
class TestColorBatch:
    @pytest.fixture(scope="module")
    def images(self, tmp_path_factory):
        tmp_path = tmp_path_factory.mktemp("color-batch-images")
        source = pyvips.Image.new_from_file(JPEG_FILE).crop(0, 0, 64, 64)
        profile = source.get("icc-profile-data")

        complete = tmp_path / "complete.tif"
        source.write_to_file(str(complete), compression="lzw")

        missing = tmp_path / "missing.tif"
        no_profile = source.copy()
        no_profile.remove("icc-profile-data")
        no_profile.write_to_file(str(missing), compression="lzw")

        alpha = tmp_path / "alpha.tif"
        alpha_image = no_profile.bandjoin(128)
        alpha_image.write_to_file(str(alpha), compression="lzw")

        gray = tmp_path / "gray.tif"
        gray_image = source.colourspace("b-w")
        if gray_image.get_typeof("icc-profile-data") != 0:
            gray_image.remove("icc-profile-data")
        gray_image.write_to_file(str(gray), compression="lzw")

        cmyk = tmp_path / "cmyk.tif"
        cmyk_image = source.colourspace("cmyk")
        if cmyk_image.get_typeof("icc-profile-data") != 0:
            cmyk_image.remove("icc-profile-data")
        cmyk_image.write_to_file(str(cmyk), compression="lzw")

        corrupt_partial = tmp_path / "corrupt-partial.tif"
        corrupt_bytes = bytearray(complete.read_bytes())
        profile_offset = corrupt_bytes.find(profile)
        if profile_offset < 0:
            pytest.skip("unable to locate embedded profile in TIFF")
        corrupt_bytes[profile_offset + 36:profile_offset + 40] = b"xxxx"
        corrupt_partial.write_bytes(corrupt_bytes)

        corrupt_unreadable = tmp_path / "corrupt-unreadable.tif"
        corrupt_bytes = bytearray(complete.read_bytes())
        profile_offset = corrupt_bytes.find(profile)
        corrupt_bytes[profile_offset + 16:profile_offset + 20] = b"XXXX"
        corrupt_unreadable.write_bytes(corrupt_bytes)

        return {
            "complete": complete,
            "missing": missing,
            "alpha": alpha,
            "gray": gray,
            "cmyk": cmyk,
            "corrupt_partial": corrupt_partial,
            "corrupt_unreadable": corrupt_unreadable,
            "profile": profile,
            "tmp_path": tmp_path,
        }

    def test_batch_entry_is_disabled_by_default(self, images):
        with pytest.raises(pyvips.ColorBatchDisabledError):
            pyvips.process_batch(
                [images["complete"]],
                target_space="srgb",
            )

    @skip_if_no("icc_transform")
    def test_complete_profile_uses_embedded_icc_transform(self, images):
        target_profile = images["tmp_path"] / "srgb.icc"
        target_profile.write_bytes(pyvips.Image.profile_load("srgb"))

        result = pyvips.process_batch(
            [{
                "image_id": "complete",
                "path": images["complete"],
            }],
            target_profile=target_profile,
            output_dir=images["tmp_path"] / "complete-out",
            enabled=True,
        )

        assert len(result.completed) == 1
        record = result.records[0]
        assert record["status"] == "converted"
        assert record["profile_status"] == "complete"
        assert record["assumption"] is None
        assert record["used_assumption"] is False
        assert record["skipped"] is False
        assert record["degraded"] is False
        assert record["output_path"] is not None

        converted = pyvips.Image.new_from_file(record["output_path"])
        assert converted.interpretation == "srgb"

    def test_missing_profile_requires_an_explicit_assumption(self, images):
        failed = pyvips.process_batch(
            [{
                "image_id": "missing",
                "path": images["missing"],
            }],
            target_space="lab",
            enabled=True,
        )

        assert len(failed.failed) == 1
        record = failed.failed[0]
        assert record["image_id"] == "missing"
        assert record["error_code"] == "missing_assumption"
        assert record["profile_status"] == "missing"

        result = pyvips.process_batch(
            [{
                "image_id": "missing",
                "path": images["missing"],
            }],
            target_space="lab",
            assumptions={"missing": "srgb"},
            output_dir=images["tmp_path"] / "missing-out",
            enabled=True,
        )

        assert len(result.completed) == 1
        record = result.records[0]
        assert record["status"] == "assumed"
        assert record["assumption"] == "srgb"
        assert record["used_assumption"] is True
        assert record["skipped"] is False
        assert record["profile_status"] == "missing"

    def test_corrupt_profile_uses_partial_space_or_falls_back(self, images):
        partial = pyvips.process_batch(
            [{
                "image_id": "partial",
                "path": images["corrupt_partial"],
            }],
            target_space="lab",
            output_dir=images["tmp_path"] / "partial-out",
            enabled=True,
        )

        assert len(partial.completed) == 1
        record = partial.records[0]
        assert record["status"] == "converted"
        assert record["profile_status"] == "corrupt"
        assert record["source_space"] == "rgb"
        assert record["assumption"] is None
        assert record["degraded"] is True

        failed = pyvips.process_batch(
            [{
                "image_id": "unreadable",
                "path": images["corrupt_unreadable"],
            }],
            target_space="lab",
            enabled=True,
        )
        assert failed.records[0]["error_code"] == "missing_assumption"

        assumed = pyvips.process_batch(
            [{
                "image_id": "unreadable",
                "path": images["corrupt_unreadable"],
            }],
            target_space="lab",
            assumptions={"unreadable": "srgb"},
            output_dir=images["tmp_path"] / "unreadable-out",
            enabled=True,
        )
        assert assumed.records[0]["status"] == "assumed"
        assert assumed.records[0]["profile_status"] == "corrupt"
        assert assumed.records[0]["assumption"] == "srgb"

    def test_non_rgb_inputs_and_alpha(self, images):
        output_dir = images["tmp_path"] / "non-rgb-out"
        result = pyvips.process_batch(
            [
                {
                    "image_id": "gray",
                    "path": images["gray"],
                    "assumption": "b-w",
                },
                {
                    "image_id": "alpha",
                    "path": images["alpha"],
                    "assumption": "srgb",
                },
                {
                    "image_id": "cmyk",
                    "path": images["cmyk"],
                    "assumption": "cmyk",
                },
            ],
            target_space="lab",
            output_dir=output_dir,
            enabled=True,
        )

        records = {record["image_id"]: record for record in result.records}
        assert all(record["status"] == "assumed"
                   for record in records.values())

        gray = pyvips.Image.new_from_file(records["gray"]["output_path"])
        assert gray.interpretation == "lab"
        assert gray.bands == 3

        alpha = pyvips.Image.new_from_file(records["alpha"]["output_path"])
        assert alpha.interpretation == "lab"
        assert alpha.hasalpha()
        assert alpha.bands == 4

        cmyk = pyvips.Image.new_from_file(records["cmyk"]["output_path"])
        assert cmyk.interpretation == "lab"
        assert cmyk.bands == 3

    def test_same_source_and_target_skips_conversion(self, images):
        output = images["tmp_path"] / "same-out.tif"
        result = pyvips.process_batch(
            [pyvips.BatchItem(
                "same",
                images["missing"],
                assumption="srgb",
                output_path=output,
            )],
            target_space="srgb",
            enabled=True,
        )

        record = result.records[0]
        assert record["status"] == "skipped"
        assert record["skipped"] is True
        assert record["assumption"] == "srgb"
        assert output.exists()
        assert hashlib.sha256(output.read_bytes()).hexdigest() == \
            hashlib.sha256(images["missing"].read_bytes()).hexdigest()

        complete_output = images["tmp_path"] / "same-complete-out.tif"
        complete_result = pyvips.process_batch(
            [pyvips.BatchItem(
                "same-complete",
                images["complete"],
                output_path=complete_output,
            )],
            target_space="rgb",
            enabled=True,
        )
        assert complete_result.records[0]["status"] == "skipped"
        assert complete_result.records[0]["profile_status"] == "complete"
        assert complete_result.records[0]["skipped"] is True

    def test_item_assumption_overrides_batch_assumption(self, images):
        result = pyvips.process_batch(
            [{
                "image_id": "override",
                "path": images["corrupt_unreadable"],
                "assumption": "b-w",
            }],
            target_space="srgb",
            assumptions={"override": "srgb"},
            enabled=True,
        )

        record = result.records[0]
        assert record["status"] == "assumed"
        assert record["assumption"] == "b-w"
        assert record["skipped"] is False

    def test_missing_input_does_not_abort_the_batch(self, images):
        result = pyvips.process_batch(
            [
                {
                    "image_id": "missing-file",
                    "path": images["tmp_path"] / "does-not-exist.tif",
                },
                {"image_id": "complete", "path": images["complete"]},
            ],
            target_space="lab",
            output_dir=images["tmp_path"] / "mid-failure-out",
            enabled=True,
        )

        records = {record["image_id"]: record for record in result.records}
        assert records["missing-file"]["status"] == "failed"
        assert records["missing-file"]["error_code"] == "input_error"
        assert records["complete"]["status"] == "converted"
        assert len(result.completed) == 1
        assert len(result.failed) == 1

    def test_duplicate_content_is_processed_once_but_recorded_per_input(
            self, images):
        output_dir = images["tmp_path"] / "duplicate-out"
        ledger = images["tmp_path"] / "duplicate.jsonl"
        result = pyvips.process_batch(
            [
                {"image_id": "primary", "path": images["complete"]},
                {"image_id": "copy", "path": images["complete"]},
            ],
            target_space="lab",
            output_dir=output_dir,
            ledger_path=ledger,
            enabled=True,
            batch_id="duplicate-batch",
        )

        assert len(result.records) == 2
        assert len(result.duplicates) == 1
        assert result.records[0]["status"] == "converted"
        assert result.records[1]["status"] == "duplicate"
        assert result.records[1]["duplicate_of"] == "primary"
        assert result.records[0]["content_sha256"] == \
            result.records[1]["content_sha256"]
        assert result.records[1]["output_path"] is not None
        assert len(ledger.read_text().splitlines()) == 2

    def test_interrupted_batch_resumes_without_duplicate_records(
            self, images, monkeypatch):
        output_dir = images["tmp_path"] / "resume-out"
        ledger = images["tmp_path"] / "resume.jsonl"
        items = [
            {"image_id": "complete", "path": images["complete"]},
            {
                "image_id": "gray",
                "path": images["gray"],
                "assumption": "b-w",
            },
            {
                "image_id": "cmyk",
                "path": images["cmyk"],
                "assumption": "cmyk",
            },
        ]

        real_convert = color_batch._convert_image
        calls = {"count": 0}

        def interrupted_convert(*args, **kwargs):
            calls["count"] += 1
            if calls["count"] == 2:
                raise KeyboardInterrupt
            return real_convert(*args, **kwargs)

        monkeypatch.setattr(
            color_batch, "_convert_image", interrupted_convert)

        first = pyvips.process_batch(
            items,
            target_space="lab",
            output_dir=output_dir,
            ledger_path=ledger,
            enabled=True,
            batch_id="resume-batch",
        )

        assert first.interrupted is True
        assert len(first.completed) == 1
        assert len(first.incomplete) == 1
        assert first.incomplete[0]["output_path"].endswith(
            ".incomplete.tif")
        assert len(ledger.read_text().splitlines()) == 1

        monkeypatch.setattr(color_batch, "_convert_image", real_convert)

        second = pyvips.process_batch(
            items,
            target_space="lab",
            output_dir=output_dir,
            ledger_path=ledger,
            enabled=True,
            batch_id="resume-batch",
        )

        assert second.interrupted is False
        assert len(second.completed) == 3
        assert second.records[0]["resumed"] is True

        lines = ledger.read_text().splitlines()
        assert len(lines) == 3
        exported = [json.loads(line) for line in lines]
        assert len({record["record_key"] for record in exported}) == 3
        assert {record["image_id"] for record in exported} == {
            "complete", "gray", "cmyk"}
        assert all(record["output_path"] for record in exported)

        with open(ledger, "rb+") as ledger_file:
            data = ledger_file.read()
            ledger_file.seek(0)
            ledger_file.truncate()
            ledger_file.write(data[:-17])

        third = pyvips.process_batch(
            items,
            target_space="lab",
            output_dir=output_dir,
            ledger_path=ledger,
            enabled=True,
            batch_id="resume-batch",
        )

        assert len(third.completed) == 3
        repaired_lines = ledger.read_text().splitlines()
        assert len(repaired_lines) == 3
        assert json.loads(repaired_lines[-1])["image_id"] == "cmyk"

    def test_mixed_batch_acceptance(self, images):
        output_dir = images["tmp_path"] / "mixed-out"
        ledger = images["tmp_path"] / "mixed.jsonl"
        items = [
            {"image_id": "complete", "path": images["complete"]},
            {
                "image_id": "missing",
                "path": images["missing"],
                "assumption": "srgb",
            },
            {
                "image_id": "corrupt_partial",
                "path": images["corrupt_partial"],
            },
            {
                "image_id": "alpha",
                "path": images["alpha"],
                "assumption": "srgb",
            },
            {
                "image_id": "gray",
                "path": images["gray"],
                "assumption": "b-w",
            },
            {
                "image_id": "cmyk",
                "path": images["cmyk"],
                "assumption": "cmyk",
            },
            {
                "image_id": "unreadable",
                "path": images["corrupt_unreadable"],
            },
            {"image_id": "complete_copy", "path": images["complete"]},
        ]

        result = pyvips.process_batch(
            items,
            target_space="srgb",
            output_dir=output_dir,
            ledger_path=ledger,
            enabled=True,
            batch_id="mixed-batch",
        )

        counts = result.conclusion_counts()
        assert counts == {
            "assumed": 2,
            "converted": 2,
            "duplicate": 1,
            "failed": 1,
            "skipped": 2,
        }
        assert len(result.records) == len(items)
        assert len(ledger.read_text().splitlines()) == len(items)

        per_image = {
            record["image_id"]: record["status"]
            for record in result.records
        }
        print(f"MIXED_BATCH conclusions={json.dumps(per_image, sort_keys=True)}")
        print(
            "MIXED_BATCH distribution="
            f"{counts} total_seconds={result.elapsed_seconds:.6f}"
        )

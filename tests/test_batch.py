import json
import os
import shutil
from functools import partial

import pyvips
import pytest

from helpers import JPEG_FILE, WEBP_FILE


def _write_bad_file(path):
    with open(path, "wb") as stream:
        stream.write(b"not an image")


class TestBatch:
    def test_mixed_items_retry_and_summaries(self, tmp_path):
        bad_file = tmp_path / "bad.jpg"
        _write_bad_file(bad_file)
        missing_file = tmp_path / "missing.jpg"
        calls = {}

        def operation(item):
            calls[item.filename] = calls.get(item.filename, 0) + 1
            if item.filename == "sample.webp" and calls[item.filename] == 1:
                raise RuntimeError("transient")
            try:
                image = pyvips.Image.new_from_file(item.input)
            except Exception as exc:
                raise pyvips.BatchStageError("load") from exc
            return image.resize(0.5)

        inputs = [
            {"id": "stable-a", "input": JPEG_FILE},
            (WEBP_FILE, None),
            str(bad_file),
            {"source": missing_file, "id": "missing"},
        ]
        summary = pyvips.process_batch(
            inputs,
            operation,
            output_dir=tmp_path / "out",
            output_suffix=".jpg",
            retries=1,
            retry_interval=0.001,
            state_dir=tmp_path / "state",
        )

        assert summary.status == "partial_failure"
        assert summary.total_count == 4
        assert summary.success_count == 2
        assert summary.failure_count == 2
        assert summary.processed_count == 4
        assert summary.retry_count == 3
        assert summary.attempts_count == 7
        assert summary.failure_types == {"Error": 2}
        assert summary.failure_stages == {"load": 2}
        assert len(summary.results) == 4

        results = list(summary.results)
        assert [result.status for result in results] == [
            "success", "success", "failure", "failure"]
        assert results[0].item_id == "stable-a"
        assert results[1].retries == 1
        assert all(result.duration_seconds >= 0 for result in results)
        assert summary.duration_stats["count"] == 4
        assert summary.duration_stats["p95_seconds"] >= 0
        assert results[2].filename == "bad.jpg"
        assert results[2].stage == "load"
        assert results[2].exception_type == "Error"

        assert os.path.exists(summary.machine_summary_path)
        assert os.path.exists(summary.human_summary_path)
        assert os.path.exists(summary.checkpoint_path)
        assert os.path.exists(summary.results_path)
        with open(summary.machine_summary_path, encoding="utf-8") as stream:
            machine = json.load(stream)
        assert machine["totals"]["failure"] == 2
        assert machine["failure_types"]["Error"] == 2
        with open(summary.human_summary_path, encoding="utf-8") as stream:
            human = stream.read()
        assert "failures by type:" in human
        assert "Error: 2" in human

    def test_all_failures_return_summary(self, tmp_path):
        def operation(item):
            raise ValueError("bad image: {}".format(item.filename))

        summary = pyvips.process_batch(
            ["one.jpg", "two.jpg"],
            operation,
            output_dir=tmp_path / "out",
            output_suffix=".jpg",
            state_dir=tmp_path / "state",
        )

        assert summary.status == "failure"
        assert summary.success_count == 0
        assert summary.failure_count == 2
        assert summary.failure_types == {"ValueError": 2}

    def test_named_operation_converts_images(self, tmp_path):
        summary = pyvips.process_batch(
            [JPEG_FILE, WEBP_FILE],
            "resize",
            output_dir=tmp_path / "out",
            output_suffix=".jpg",
            operation_args=(0.5,),
            state_dir=tmp_path / "state",
        )

        assert summary.status == "success"
        assert summary.success_count == 2
        for result in summary.results:
            assert result.stage_durations_seconds["load"] >= 0
            assert result.stage_durations_seconds["process"] >= 0
            assert result.stage_durations_seconds["write"] >= 0
            assert os.path.exists(result.output)
            converted = pyvips.Image.new_from_file(result.output)
            assert converted.width > 0

    def test_partial_operation_keeps_resume_stable(self, tmp_path):
        def resize(item, scale):
            return pyvips.Image.new_from_file(item.input).resize(scale)

        first = pyvips.process_batch(
            [JPEG_FILE],
            partial(resize, scale=0.5),
            output_dir=tmp_path / "out",
            output_suffix=".jpg",
            state_dir=tmp_path / "state",
        )
        second = pyvips.process_batch(
            [JPEG_FILE],
            partial(resize, scale=0.5),
            output_dir=tmp_path / "out",
            output_suffix=".jpg",
            state_dir=tmp_path / "state",
        )

        assert second.batch_id == first.batch_id
        assert second.skipped_count == 1
        assert second.processed_count == 0
        assert not second.checkpoint_recovered

    def test_resume_skips_success_and_retries_failure(self, tmp_path):
        bad_file = tmp_path / "bad.jpg"
        shutil.copyfile(JPEG_FILE, bad_file)
        counter = {"bad": True}
        calls = {}

        def operation(item):
            calls[item.filename] = calls.get(item.filename, 0) + 1
            if item.filename == "bad.jpg" and counter["bad"]:
                raise ValueError("temporary data problem")
            return pyvips.Image.new_from_file(item.input).resize(0.5)

        inputs = [JPEG_FILE, WEBP_FILE, str(bad_file)]
        first = pyvips.process_batch(
            inputs,
            operation,
            output_dir=tmp_path / "out",
            output_suffix=".jpg",
            state_dir=tmp_path / "state",
        )
        assert first.failure_count == 1
        assert calls == {"sample.jpg": 1, "sample.webp": 1, "bad.jpg": 1}

        calls.clear()
        counter["bad"] = False
        second = pyvips.process_batch(
            inputs,
            operation,
            output_dir=tmp_path / "out",
            output_suffix=".jpg",
            state_dir=tmp_path / "state",
            resume=True,
            retry_failed=True,
        )

        assert second.batch_id == first.batch_id
        assert second.resumed
        assert second.skipped_count == 2
        assert second.processed_count == 1
        assert second.failure_count == 0
        assert second.success_count == 3
        assert calls == {"bad.jpg": 1}
        assert first.machine_summary_path != second.machine_summary_path
        assert os.path.exists(first.machine_summary_path)
        assert os.path.exists(second.machine_summary_path)
        assert [result.resumed for result in second.results] == [
            True, True, False]

    def test_changed_manifest_causes_full_run(self, tmp_path):
        manifest = tmp_path / "inputs.json"
        manifest.write_text(json.dumps([JPEG_FILE, WEBP_FILE]),
                            encoding="utf-8")

        def operation(item):
            return pyvips.Image.new_from_file(item.input).resize(0.5)

        first = pyvips.process_batch(
            manifest,
            operation,
            output_dir=tmp_path / "out",
            output_suffix=".jpg",
            state_dir=tmp_path / "state",
        )
        assert first.processed_count == 2

        manifest.write_text(json.dumps([JPEG_FILE]), encoding="utf-8")
        with pytest.warns(RuntimeWarning, match="does not match"):
            second = pyvips.process_batch(
                manifest,
                operation,
                output_dir=tmp_path / "out",
                output_suffix=".jpg",
                state_dir=tmp_path / "state",
            )

        assert second.batch_id == first.batch_id
        assert second.checkpoint_recovered
        assert second.processed_count == 1
        assert second.skipped_count == 0
        assert any("full run" in warning for warning in second.warnings)

    def test_corrupt_checkpoint_is_recovered(self, tmp_path):
        manifest = tmp_path / "inputs.json"
        manifest.write_text(json.dumps([JPEG_FILE]), encoding="utf-8")

        def operation(item):
            return pyvips.Image.new_from_file(item.input).resize(0.5)

        first = pyvips.process_batch(
            manifest,
            operation,
            output_dir=tmp_path / "out",
            output_suffix=".jpg",
            state_dir=tmp_path / "state",
        )
        with open(first.checkpoint_path, "w", encoding="utf-8") as stream:
            stream.write("{broken")

        with pytest.warns(RuntimeWarning, match="corrupt"):
            second = pyvips.process_batch(
                manifest,
                operation,
                output_dir=tmp_path / "out",
                output_suffix=".jpg",
                state_dir=tmp_path / "state",
            )

        assert second.checkpoint_recovered
        assert second.processed_count == 1
        assert second.skipped_count == 0
        assert any("corrupt" in warning for warning in second.warnings)

    def test_summary_write_failure_keeps_results(self, tmp_path, monkeypatch):
        def operation(item):
            return pyvips.Image.new_from_file(item.input).resize(0.5)

        def fail_write(path, content):
            raise OSError("summary storage unavailable")

        monkeypatch.setattr(pyvips.batch, "_write_atomic", fail_write)
        summary = pyvips.process_batch(
            [JPEG_FILE],
            operation,
            output_dir=tmp_path / "out",
            output_suffix=".jpg",
            state_dir=tmp_path / "state",
        )

        assert summary.status == "success"
        assert summary.machine_summary_path is None
        assert summary.human_summary_path is None
        assert any(
            "could not write" in warning for warning in summary.warnings)
        assert len(list(summary.results)) == 1
        assert os.path.exists(summary.results_path)

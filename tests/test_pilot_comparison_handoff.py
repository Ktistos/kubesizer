"""Do not interrupt comparison startup or pause before its completed receipt."""

import json

import pytest
import yaml

from scripts import run_stateful_pilot as pilot


def harness(tmp_path, monkeypatch, *, status="PENDING"):
    root = tmp_path / "comparison"
    manifest = root / "family/core/study_manifest.yaml"
    manifest.parent.mkdir(parents=True)
    output = tmp_path / "pilot"
    output.mkdir()
    clock, calls = [0], []

    def save(status):
        manifest.write_text(
            yaml.safe_dump(
                {
                    "runs": [
                        {
                            "id": "r1",
                            "status": status,
                            "artifact_sha256": {"sample": "fixed"},
                        }
                    ]
                }
            )
        )

    save(status)
    monkeypatch.setattr(pilot.time, "monotonic", lambda: clock[0])

    def command(*args, **kwargs):
        calls.append(args)
        if args[1] == "stop":
            assert (
                yaml.safe_load(manifest.read_text())["runs"][0]["status"] == "COMPLETED"
            )
            assert (manifest.parent / "evidence/r1/receipt.yaml").exists()
        return "active\n"

    monkeypatch.setattr(pilot, "command", command)
    return root, manifest, output, clock, calls, save


@pytest.mark.parametrize(
    "initial", ["PENDING", "RUNNING", "partial-yaml", "checkpoint-partial"]
)
def test_waits_for_running_then_completed_receipt(tmp_path, monkeypatch, initial):
    root, manifest, output, clock, calls, save = harness(tmp_path, monkeypatch)
    if initial in ("RUNNING", "checkpoint-partial"):
        save("RUNNING")
    elif initial == "partial-yaml":
        manifest.write_text("runs: [")

    def sleep(_):
        clock[0] += 1
        if clock[0] == 1:
            if initial == "checkpoint-partial":
                manifest.write_text("runs: []\n")
            else:
                save("RUNNING")
        elif clock[0] == 2:
            save("COMPLETED")
        elif clock[0] == 3:
            receipt = manifest.parent / "evidence/r1/receipt.yaml"
            receipt.parent.mkdir(parents=True)
            receipt.write_text("completed: true\n")

    monkeypatch.setattr(pilot.time, "sleep", sleep)
    pilot.wait_and_pause(output, comparison_root=root, startup_timeout=10)
    assert sum(c[1] == "stop" for c in calls) == 1
    pause = json.loads((output / "comparison-pause.json").read_text())
    assert pause["resume_required"]
    assert pause["checkpoint"] == "r1"
    assert pause["completed"] == {"r1": {"sample": "fixed"}}


def test_startup_timeout_never_stops_comparison(tmp_path, monkeypatch):
    root, _, output, clock, calls, _ = harness(tmp_path, monkeypatch)
    monkeypatch.setattr(
        pilot.time, "sleep", lambda _: clock.__setitem__(0, clock[0] + 1)
    )
    with pytest.raises(RuntimeError, match="did not start a treatment"):
        pilot.wait_and_pause(output, comparison_root=root, startup_timeout=2)
    assert all(c[1] != "stop" for c in calls)
    assert not (output / "comparison-pause.json").exists()


def test_ambiguous_running_treatments_fail_without_pausing(tmp_path, monkeypatch):
    root, manifest, output, _, calls, _ = harness(tmp_path, monkeypatch)
    manifest.write_text(
        yaml.safe_dump(
            {
                "runs": [
                    {"id": "r1", "status": "RUNNING"},
                    {"id": "r2", "status": "RUNNING"},
                ]
            }
        )
    )
    with pytest.raises(RuntimeError, match="got 2"):
        pilot.wait_and_pause(output, comparison_root=root)
    assert all(c[1] != "stop" for c in calls)
    assert not (output / "comparison-pause.json").exists()

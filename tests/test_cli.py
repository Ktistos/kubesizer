import json
import subprocess
import sys
from pathlib import Path

import yaml

from kubesizer_cli import main

ROOT = Path(__file__).resolve().parents[1]


def test_cli_sizes_without_native_backend(capsys):
    assert (
        main(["--workload", str(ROOT / "workload.minimum.yaml"), "--json", "--quiet"])
        == 0
    )
    result = json.loads(capsys.readouterr().out)
    assert result["result"]["services"] == {"service": {"recommended_replicas": 1}}
    assert "native_backend" not in sys.modules
    assert "calibration" not in sys.modules


def test_cli_rejects_percentile_slo(tmp_path, capsys):
    data = yaml.safe_load((ROOT / "workload.minimum.yaml").read_text())
    data["request_classes"]["request"]["end_to_end_latency_slos"] = {
        "percentile": {"quantile": 0.95, "maximum_seconds": 1.0}
    }
    path = tmp_path / "workload.yaml"
    path.write_text(yaml.safe_dump(data))
    assert main(["--workload", str(path), "--json", "--quiet"]) == 2
    assert "percentile" in json.loads(capsys.readouterr().out)["error"]["message"]


def test_cli_bounds_failure_is_a_machine_error(capsys):
    assert main(["--max-replicas", "0", "--json"]) == 2
    assert not json.loads(capsys.readouterr().out)["ok"]


def test_cli_does_not_expose_removed_options():
    result = subprocess.run(
        [sys.executable, str(ROOT / "kubesizer_cli.py"), "--help"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert "--resources" not in result.stdout
    assert "--warmup" not in result.stdout
    assert "scalable" not in result.stdout


def test_cli_only_persists_when_requested(tmp_path, capsys):
    path = tmp_path / "recommendation.json"
    assert (
        main(
            [
                "--workload",
                str(ROOT / "workload.minimum.yaml"),
                "--out",
                str(path),
                "--quiet",
            ]
        )
        == 0
    )
    assert json.loads(path.read_text())["ok"]

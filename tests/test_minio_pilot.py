"""Guard the queued pilot's isolation, evidence handling and lifecycle."""

import json
import io
import random
from pathlib import Path

import pytest
import yaml

from scripts import run_minio_pilot as pilot
from scripts import characterize_minio_put as put_phases


def test_put_fragment_requires_unique_rename_with_matching_temporary_id():
    bucket = put_phases.BUCKET
    keys = ["size-1048576/key-01", "size-1048576/key-010"]

    def record(api, path):
        return dict(api=f"storage.{api}", path=path, host="minio1:9000", time="t")

    fragment = record("CreateFile", "/data .minio.sys/tmp temp-1/shard/part.1")
    ambiguous = record("CreateFile", "/data .minio.sys/tmp temp-2/shard/part.1")
    renames = [record("RenameData", f"/data temp-1 other {bucket} {keys[0]}")]
    renames += [
        record("RenameData", f"/data temp-2 other {bucket} {key}") for key in keys
    ]
    cleanup = record("Delete", f"/data {bucket} {keys[1]}/old-version")
    joined = put_phases.correlate(
        [{"key": key} for key in keys], [fragment, ambiguous, cleanup, *renames]
    )
    assert joined[keys[0]] == [fragment, *renames[:2]]
    assert joined[keys[1]] == [cleanup, renames[2]]


def test_put_counters_follow_data_mount_device():
    raw = """nr_throttled 3

MOUNTINFO
1 0 8:42 / / rw - ext4 /dev/other rw
2 1 8:16 / /data rw - ext4 /dev/longhorn/test-pvc rw

DISKSTATS
8 42 other 100 101 102 103 104 105 106 107 108 109 110
8 16 sdb 10 11 12 13 14 15 16 17 18 19 20
"""
    assert put_phases.member_counters(raw) == (
        {"nr_throttled": 3},
        dict(
            device="8:16",
            reads=10,
            read_ms=13,
            writes=14,
            write_ms=17,
            busy_ms=19,
            weighted_ms=20,
        ),
    )


@pytest.mark.parametrize("indent", [None, 2])
def test_trace_parser_accepts_json_lines_and_pretty_printed_objects(indent):
    records = [
        {"api": "storage.ReadVersion", "timing": {"seconds": 0.01}},
        {"api": "storage.StatVol"},
    ]
    stream = io.StringIO("\n\n".join(json.dumps(r, indent=indent) for r in records))
    assert list(pilot.storage_trace_records(stream)) == records


@pytest.mark.parametrize(
    "raw", ['{"api":', "[]", "garbage", "x" * (4 * 1024 * 1024 + 1)]
)
def test_trace_parser_rejects_incomplete_or_invalid_output(raw):
    with pytest.raises(RuntimeError):
        list(pilot.storage_trace_records(io.StringIO(raw)))


def test_layout_has_four_distinct_persistent_members_and_pinned_parity(tmp_path):
    pilot.render(tmp_path)
    resources = list(yaml.safe_load_all((tmp_path / "workload.yaml").read_text()))
    members = [r for r in resources if r["kind"] == "StatefulSet"]
    assert len(members) == 4
    assert (
        len(
            {
                r["spec"]["template"]["spec"]["nodeSelector"]["kubernetes.io/hostname"]
                for r in members
            }
        )
        == 4
    )
    for r in members:
        assert r["spec"]["replicas"] == 1
        assert (
            r["spec"]["volumeClaimTemplates"][0]["spec"]["resources"]["requests"][
                "storage"
            ]
            == "2Gi"
        )
        database = r["spec"]["template"]["spec"]["containers"][0]
        assert "@sha256:" in database["image"]
        assert "http://minio{1...4}:9000/data" in database["args"]
        assert any(e.get("value") == "EC:2" for e in database["env"])
    assert all(r["kind"] != "Secret" for r in resources)
    assert json.loads((tmp_path / "protocol.json").read_text()) == pilot.PLAN


def test_trace_redaction_removes_nested_authorization_and_credentials():
    value = {
        "duration": 0.01,
        "headers": {"Authorization": "sensitive"},
        "request": {
            "credential": "sensitive",
            "path": "/bucket/object",
            "children": [{"Signature": "sensitive", "n": 4}],
        },
    }
    assert pilot.redact(value) == {
        "duration": 0.01,
        "request": {"path": "/bucket/object", "children": [{"n": 4}]},
    }


@pytest.mark.parametrize(
    "value,records", [({"status": "error"}, 0), ({"api": "storage.ReadFile"}, 1)]
)
def test_trace_failure_or_early_exit_is_not_successful_evidence(
    tmp_path, monkeypatch, value, records
):
    class Process:
        stdout = iter([json.dumps(value)])

    def popen(args, **kwargs):
        assert args[-1] == pilot.STORAGE_TRACE_SCRIPT
        assert "admin trace --call storage pilot" in args[-1]
        assert "--all" not in args[-1]
        return Process()

    monkeypatch.setattr(pilot.subprocess, "Popen", popen)
    evidence = pilot.Evidence(tmp_path)
    try:
        evidence.trace()
        evidence.threads[0].join(timeout=2)
        assert not evidence.threads[0].is_alive()
        assert evidence.trace_error
        assert evidence.trace_count == records
    finally:
        for _, log in evidence.processes:
            log.close()


@pytest.mark.parametrize("status", ["COMPLETED", "FAILED"])
def test_queue_waits_for_terminal_pg_cleanup_then_records_outcome(
    tmp_path, monkeypatch, status
):
    previous = tmp_path / "previous"
    previous.mkdir()
    output = tmp_path / "new"
    output.mkdir()
    (previous / "status.json").write_text(json.dumps({"status": status}))
    (previous / "comparison-pause.json").write_text('{"resume_required":false}')
    monkeypatch.setattr(pilot, "PG_ROOT", previous)
    monkeypatch.setattr(pilot, "command", lambda *a, **k: "inactive\n")
    monkeypatch.setattr(pilot, "kube", lambda *a, **k: '{"items":[]}')
    pilot.wait_for_postgres(output, timeout=1)
    assert (
        json.loads((output / "postgres-predecessor.json").read_text())["outcome"][
            "status"
        ]
        == status
    )


def test_queue_cannot_overlap_pg_cleanup(tmp_path, monkeypatch):
    previous = tmp_path / "previous"
    previous.mkdir()
    (previous / "status.json").write_text('{"status":"COMPLETED"}')
    (previous / "comparison-pause.json").write_text('{"resume_required":true}')
    monkeypatch.setattr(pilot, "PG_ROOT", previous)
    monkeypatch.setattr(pilot, "command", lambda *a, **k: "inactive\n")
    clock = iter((0, 0, 2))
    monkeypatch.setattr(pilot.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(pilot.time, "sleep", lambda *_: None)
    monkeypatch.setattr(
        pilot,
        "kube",
        lambda *a, **k: pytest.fail("Must not launch during predecessor cleanup"),
    )
    with pytest.raises(RuntimeError, match="not launched"):
        pilot.wait_for_postgres(tmp_path, timeout=1)


def test_health_gate_preserves_failures_but_latency_is_an_outcome():
    value = dict(
        timed_out=False,
        snapshot_errors=0,
        classes={
            "get": dict(
                completed=100,
                failures=0,
                maximum_scheduling_delay_seconds=0.001,
                mean_seconds=0.02,
            )
        },
    )
    assert pilot.healthy(value)
    value["classes"]["get"]["mean_seconds"] = 0.5
    assert pilot.healthy(value)
    value["classes"]["get"]["failures"] = 1
    assert not pilot.healthy(value)


def test_import_only_reuses_matching_receipt_verified_characterization(tmp_path):
    origin, target = tmp_path / "old", tmp_path / "new"
    origin.mkdir()
    target.mkdir()
    pilot.write(origin / "protocol.json", pilot.PLAN)
    name = "characterization-get_64k-rep0"
    path = origin / "runs" / name
    path.mkdir(parents=True)
    pilot.write(
        path / "summary.json",
        dict(
            run_id=name,
            rate_rps=2,
            seed=pilot.PLAN["seed"],
            warmup_seconds=15,
            measurement_seconds=90,
            timed_out=False,
            snapshot_errors=0,
            classes={
                "get_64k": dict(
                    completed=100,
                    failures=0,
                    maximum_scheduling_delay_seconds=0.001,
                    mean_seconds=0.5,
                )
            },
        ),
    )
    pilot.write(
        path / "receipt.json", {"summary.json": pilot.digest(path / "summary.json")}
    )
    imported = pilot.import_characterizations(origin, target)
    assert list(imported) == [name]
    assert pilot.digest(target / "runs" / name / "summary.json") == pilot.digest(
        path / "summary.json"
    )
    assert not json.loads((target / "characterization-import.json").read_text())[
        "target_runs_imported"
    ]
    (path / "summary.json").write_text("{}")
    with pytest.raises(ValueError, match="receipt mismatch"):
        pilot.import_characterizations(origin, tmp_path / "invalid")


def test_cleanup_scales_only_owned_minio_members_and_resumes_once(
    tmp_path, monkeypatch
):
    (tmp_path / "comparison-pause.json").write_text('{"resume_required":true}')
    calls = []

    def kube(*args, **kwargs):
        if "statefulset" in args and "get" in args:
            name = args[args.index("statefulset") + 1]
            return json.dumps(
                {"metadata": {"name": name, "labels": {"kubesizer.campaign": "mio1"}}}
            )
        if "pods" in args:
            return '{"items":[]}'
        calls.append(args)
        return ""

    monkeypatch.setattr(pilot, "kube", kube)
    monkeypatch.setattr(pilot, "command", lambda *args, **kwargs: calls.append(args))
    pilot.finish(tmp_path)
    pilot.finish(tmp_path)
    assert sum("--replicas=0" in args for args in calls) == 4
    assert sum(args[:2] == ("systemctl", "start") for args in calls) == 1
    assert all(pilot.NAMESPACE in args for args in calls if "--replicas=0" in args)


def test_manual_comparison_hold_still_cleans_pods_and_never_resumes(
    tmp_path, monkeypatch
):
    (tmp_path / "comparison-pause.json").write_text(
        '{"resume_required":false,"cleanup_required":true}'
    )
    calls = []

    def kube(*args, **kwargs):
        if "statefulset" in args and "get" in args:
            return json.dumps({"metadata": {"labels": {"kubesizer.campaign": "mio1"}}})
        if "pods" in args:
            return '{"items":[]}'
        calls.append(args)
        return ""

    monkeypatch.setattr(pilot, "kube", kube)
    monkeypatch.setattr(pilot, "command", lambda *a, **k: calls.append(a))
    pilot.finish(tmp_path)
    pilot.finish(tmp_path)
    assert sum("--replicas=0" in a for a in calls) == 4
    assert all(a[:2] != ("systemctl", "start") for a in calls)


def storage_info():
    return {
        "info": {
            "backend": {
                "backendType": "Erasure",
                "onlineDisks": 4,
                "offlineDisks": 0,
                "standardSCParity": 2,
                "totalSets": [1],
                "totalDrivesPerSet": [4],
            },
            "servers": [
                {"state": "online", "drives": [{"state": "ok"}]} for _ in range(4)
            ],
        }
    }


def test_storage_gate_requires_verified_payloads_and_consecutive_health(
    tmp_path, monkeypatch
):
    checks, clock = [0], [0]
    monkeypatch.setattr(pilot.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        pilot.time, "sleep", lambda t: clock.__setitem__(0, clock[0] + t)
    )

    def info():
        checks[0] += 1
        value = storage_info()
        if checks[0] == 2:
            value["info"]["servers"][0]["drives"][0]["state"] = "offline"
        return value

    monkeypatch.setattr(pilot, "admin_info", info)

    class Client:
        def bucket_exists(self, bucket):
            return True

        def put_object(self, bucket, key, content, size):
            self.content = content.read()

        def get_object(self, bucket, key):
            response = io.BytesIO(self.content)
            response.release_conn = lambda: None
            return response

    pilot.wait_for_storage(tmp_path, [Client()], timeout=30)
    value = json.loads((tmp_path / "storage-readiness.json").read_text())
    assert value["ready"]
    assert len(value["checks"]) == 5
    assert value["checks"][-1]["consecutive_successes"] == 3


def test_storage_gate_rejects_corrupt_probe_instead_of_retrying(tmp_path, monkeypatch):
    monkeypatch.setattr(pilot, "admin_info", storage_info)

    class Client:
        def bucket_exists(self, bucket):
            return True

        def put_object(self, *args):
            pass

        def get_object(self, *args):
            response = io.BytesIO(b"wrong")
            response.release_conn = lambda: None
            return response

    with pytest.raises(ValueError, match="payload mismatch"):
        pilot.wait_for_storage(tmp_path, [Client()])
    assert not json.loads((tmp_path / "storage-readiness.json").read_text())["ready"]


def test_snapshot_keeps_partial_members_when_one_observer_times_out(
    tmp_path, monkeypatch
):
    calls = []

    def open_url(url, timeout):
        calls.append(url)
        if ":29410/" in url:
            raise TimeoutError("observer did not reply")
        return io.BytesIO(
            b"cpu_seconds 1\n" if "/metrics/" in url else b'{"processes":[]}'
        )

    monkeypatch.setattr(pilot, "urlopen", open_url)
    result = pilot.Evidence(tmp_path).snapshot()
    assert len(calls) == 8
    assert set(result["members"]) == set(pilot.MEMBERS)
    assert result["errors"] == [
        dict(member="minio1", source="process", error_type="TimeoutError")
    ]
    assert result["members"]["minio4"]["processes"] == []


def test_final_monitor_timeout_cannot_erase_completed_request_outcomes(tmp_path):
    class Evidence:
        count = 0

        def snapshot(self):
            self.count += 1
            if self.count > 1:
                raise TimeoutError("final observer snapshot failed")
            return dict(epoch=0, members={}, errors=[])

    class S3:
        def put_object(self, *args, **kwargs):
            pass

    result = pilot.collect(
        tmp_path,
        S3(),
        "monitor-timeout",
        200,
        41,
        ["put_64k"],
        warmup=0,
        measurement=0.05,
        evidence=Evidence(),
    )
    assert result["classes"]["put_64k"]["completed"] > 0
    assert result["classes"]["put_64k"]["failures"] == 0
    assert result["snapshot_errors"] == 1
    assert not result["evidence_complete"]
    assert not pilot.healthy(result)
    directory = tmp_path / "runs/monitor-timeout"
    receipt = json.loads((directory / "receipt.json").read_text())
    assert all(pilot.digest(directory / name) == sha for name, sha in receipt.items())
    assert (
        json.loads((directory / "snapshots.json").read_text())["after"]["errors"][0][
            "error_type"
        ]
        == "TimeoutError"
    )


def frozen_fixture(path):
    path.mkdir()
    origin = dict(pilot.PLAN)
    origin.pop("client_transport")
    origin.pop("latency_boundary")
    pilot.write(path / "protocol.json", origin)
    schedule = [(u, r) for u in pilot.PLAN["target_load_fractions"] for r in range(2)]
    random.Random(pilot.PLAN["seed"]).shuffle(schedule)
    for name, value in [
        ("anchor-frozen.json", dict(anchor_rps=24, target_outcomes_used=False)),
        ("target-order-frozen.json", schedule),
        ("characterization-results.json", []),
        ("pilot-results.json", []),
    ]:
        pilot.write(path / name, value)
    pilot.write(
        path / "input-freeze-receipt.json",
        {
            name: pilot.digest(path / name)
            for name in (
                "anchor-frozen.json",
                "target-order-frozen.json",
                "characterization-results.json",
                "pilot-results.json",
            )
        },
    )


def test_transport_continuation_preserves_frozen_bytes_and_excludes_old_targets(
    tmp_path,
):
    source, output = tmp_path / "source", tmp_path / "output"
    frozen_fixture(source)
    output.mkdir()
    pilot.write(source / "target-results.json", [{"old": "retained only at origin"}])
    imported = pilot.import_frozen_inputs(source, output)
    assert imported["anchor"]["anchor_rps"] == 24
    receipt = json.loads((source / "input-freeze-receipt.json").read_text())
    assert all(pilot.digest(output / name) == sha for name, sha in receipt.items())
    assert not (output / "target-results.json").exists()
    assert json.loads((output / "input-amendment.json").read_text())[
        "all_targets_fresh_for_new_boundary"
    ]


def test_continuation_rejects_corrupt_frozen_inputs(tmp_path):
    source, output = tmp_path / "source", tmp_path / "output"
    frozen_fixture(source)
    output.mkdir()
    pilot.write(
        source / "anchor-frozen.json", dict(anchor_rps=48, target_outcomes_used=False)
    )
    with pytest.raises(ValueError, match="receipt mismatch"):
        pilot.import_frozen_inputs(source, output)


def test_s3_load_uses_ingress_and_only_ingress_reaches_public_port(
    tmp_path, monkeypatch
):
    pilot.render(tmp_path)
    docs = list(yaml.safe_load_all((tmp_path / "workload.yaml").read_text()))
    ingress = next(d for d in docs if d["kind"] == "Ingress")
    assert ingress["spec"]["rules"][0]["host"] == pilot.S3_HOST
    assert (
        ingress["spec"]["rules"][0]["http"]["paths"][0]["backend"]["service"]["name"]
        == "minio1"
    )
    policy = next(d for d in docs if d["kind"] == "NetworkPolicy")
    external = policy["spec"]["ingress"][1]
    assert (
        external["from"][0]["namespaceSelector"]["matchLabels"][
            "kubernetes.io/metadata.name"
        ]
        == "ingress-nginx"
    )
    assert external["ports"] == [dict(protocol="TCP", port=9000)]
    # Okeanos ingress uses hostNetwork; namespace selectors alone cannot
    # identify its packets. Only observed ingress node IPs get the S3 port.
    assert {x["ipBlock"]["cidr"] for x in external["from"] if "ipBlock" in x} == set(
        pilot.INGRESS_NODE_CIDRS
    )
    assert "10.233.116.192/32" in pilot.INGRESS_NODE_CIDRS

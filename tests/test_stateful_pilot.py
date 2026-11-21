"""Exercise persistent work and early acknowledgement through actual HTTP."""

import json
import threading
from urllib.request import Request, urlopen

import pytest
import yaml

from performance_models import QueueingNetworkType, System
from scripts.run_stateful_pilot import MEMBERS, PLAN, model, predict, render, write
from scripts.run_stateful_pilot import Evidence, collect, finish
from scripts.stateful_pilot_server import Server, Worker, handler


@pytest.fixture
def cluster(tmp_path):
    workers, servers = [], []
    for name in ("follower1", "follower2", "primary"):
        targets = [f"http://127.0.0.1:{s.server_port}/work" for s in servers]
        worker = Worker(
            name,
            str(tmp_path / f"{name}.sqlite"),
            targets=targets if name == "primary" else (),
            means=(0.001, 0.002),
        )
        worker.rng.expovariate = lambda rate, name=name: (
            0.005 if name == "primary" else 0.02
        )
        server = Server(("127.0.0.1", 0), handler(worker))
        threading.Thread(target=server.serve_forever, daemon=True).start()
        workers.append(worker)
        servers.append(server)
    yield workers, servers
    for worker in workers:
        worker.calls.shutdown(wait=True)
    for server in servers:
        server.shutdown()
        server.server_close()
    for worker in workers:
        worker.db.close()


@pytest.mark.parametrize("mode", ["async", "quorum1", "all"])
def test_write_persists_on_all_members_even_after_early_response(cluster, mode):
    workers, servers = cluster
    request = Request(
        f"http://127.0.0.1:{servers[-1].server_port}/work",
        data=json.dumps(
            dict(id="write-1", key=17, request_class="write", mode=mode)
        ).encode(),
    )
    with urlopen(request, timeout=5) as response:
        assert json.load(response)["success"]
    workers[-1].calls.shutdown(wait=True)
    assert all(
        w.db.execute("SELECT v FROM kv WHERE k=17").fetchone()[0] == "write-1"
        for w in workers
    )
    assert workers[-1].status()["counters"]["pending_calls"] == 0


def test_background_acknowledges_while_branches_are_still_running(cluster):
    workers, servers = cluster
    entered, release = threading.Event(), threading.Event()
    for worker in workers[:2]:
        original = worker.local

        def blocked(data, original=original):
            entered.set()
            assert release.wait(5)
            original(data)

        worker.local = blocked
    try:
        result = workers[-1].work(
            dict(id="async-1", key=1, request_class="write", mode="async")
        )
        assert result["success"]
        assert entered.wait(2)
        assert workers[-1].status()["counters"]["pending_calls"] == 2
        with pytest.raises(ValueError, match="outstanding"):
            workers[-1].reset(22)
    finally:
        release.set()


def test_equal_information_models_change_only_call_policies():
    profile = dict(
        local_samples_seconds={
            name: {c: [0.001, 0.002] for c in ("read", "write")} for name in MEMBERS
        },
        residual_seconds={c: [0.001] for c in ("read", "write")},
        one_way_transport_seconds=[0.001],
    )
    legacy = model(profile, "quorum1", 2, enriched=False)
    enriched = model(profile, "quorum1", 2, enriched=True)
    for c in ("read", "write"):
        assert enriched["request_classes"][c].pop("call_policies")
    assert enriched == legacy
    network = System("pilot", legacy, QueueingNetworkType.OPEN)
    assert tuple(network.get_system_replicas()) == (1, 1, 1)
    assert network.get_replica_constraints() == {n: (1, 1) for n in MEMBERS}


def test_render_limits_deployment_and_permissions_to_pilot_namespace(tmp_path):
    render(tmp_path)
    bootstrap = list(yaml.safe_load_all((tmp_path / "bootstrap.yaml").read_text()))
    resources = list(yaml.safe_load_all((tmp_path / "workload.yaml").read_text()))
    assert all(
        x["kind"] not in ("ClusterRole", "ClusterRoleBinding") for x in bootstrap
    )
    stateful = [x for x in resources if x["kind"] == "StatefulSet"]
    assert len(stateful) == 3
    assert all(x["spec"]["replicas"] == 1 for x in stateful)
    assert all(
        x["metadata"]["namespace"] == "kubesizer-stateful-pilot-sfp1" for x in resources
    )
    assert all(x["spec"]["volumeClaimTemplates"] for x in stateful)


def test_freeze_produces_predictions_without_any_target_outcomes(tmp_path, monkeypatch):
    monkeypatch.setitem(PLAN, "warmup_seconds", 1)
    monkeypatch.setitem(PLAN, "measurement_seconds", 2)
    monkeypatch.setitem(PLAN, "simulation_replications", 2)
    monkeypatch.setitem(PLAN, "modes", ["async", "quorum1"])
    monkeypatch.setitem(PLAN, "load_fractions", [0.25])
    profile = dict(
        local_samples_seconds={
            name: {c: [0.001, 0.002] for c in ("read", "write")} for name in MEMBERS
        },
        residual_seconds={c: [0.001] for c in ("read", "write")},
        one_way_transport_seconds=[0.001],
    )
    write(tmp_path / "characterization.json", profile)
    frozen = predict(tmp_path, profile)
    assert not frozen["target_outcomes_used"]
    assert len(frozen["predictions"]) == 4
    assert all(
        row["simulation"]["status"] == "PREDICTED" for row in frozen["predictions"]
    )
    assert all(
        row["analytical"]["status"] == "ABSTAINED" for row in frozen["predictions"]
    )
    assert (tmp_path / "prediction-receipt.json").exists()


def test_collector_retains_complete_http_cohort_and_late_work(
    cluster, tmp_path, monkeypatch
):
    workers, servers = cluster
    evidence_servers = []
    evidence = Evidence(tmp_path)
    for worker in workers:
        server = Server(("127.0.0.1", 0), handler(worker, evidence=True))
        threading.Thread(target=server.serve_forever, daemon=True).start()
        evidence.urls[worker.name] = f"http://127.0.0.1:{server.server_port}"
        evidence_servers.append(server)
    monkeypatch.setattr(
        "scripts.run_stateful_pilot.INGRESS_IP", f"127.0.0.1:{servers[-1].server_port}"
    )
    try:
        result = collect(
            evidence,
            tmp_path,
            run_id="test",
            mode="async",
            rate=40,
            seed=7,
            warmup=0,
            measurement=0.15,
        )
        assert result["cohort_count"] > 0
        assert not result["child_failures"]
        assert not result["timed_out"]
        assert all(c["failures"] == 0 for c in result["classes"].values())
        assert (tmp_path / "runs/test/receipt.json").exists()
    finally:
        for server in evidence_servers:
            server.shutdown()
            server.server_close()


def test_cleanup_resumes_comparison_only_once_after_all_pods_stop(
    tmp_path, monkeypatch
):
    write(tmp_path / "comparison-pause.json", {"resume_required": True})
    calls = []

    def kubectl(*args, **kwargs):
        if "statefulsets" in args:
            return json.dumps(
                {
                    "items": [
                        {
                            "metadata": {
                                "name": name,
                                "labels": {"kubesizer.campaign": "sfp1"},
                            }
                        }
                        for name in MEMBERS
                    ]
                }
            )
        if "pods" in args:
            return '{"items": []}'
        calls.append(args)
        return ""

    monkeypatch.setattr("scripts.run_stateful_pilot.kube", kubectl)
    monkeypatch.setattr(
        "scripts.run_stateful_pilot.command", lambda *args, **kwargs: calls.append(args)
    )
    finish(tmp_path)
    finish(tmp_path)
    starts = [c for c in calls if c[:2] == ("systemctl", "start")]
    assert len(starts) == 1
    assert len([c for c in calls if "--replicas=0" in c]) == 3


def test_cleanup_never_resumes_comparison_while_pilot_pods_remain(
    tmp_path, monkeypatch
):
    write(tmp_path / "comparison-pause.json", {"resume_required": True})
    calls = []
    clock = iter((0, 1, 181))
    monkeypatch.setattr(
        "scripts.run_stateful_pilot.time.monotonic", lambda: next(clock)
    )
    monkeypatch.setattr("scripts.run_stateful_pilot.time.sleep", lambda *_: None)
    monkeypatch.setattr(
        "scripts.run_stateful_pilot.kube",
        lambda *args, **kwargs: (
            '{"items": []}' if "statefulsets" in args else '{"items": [{}]}'
        ),
    )
    monkeypatch.setattr(
        "scripts.run_stateful_pilot.command", lambda *args, **kwargs: calls.append(args)
    )
    with pytest.raises(RuntimeError, match="pods remain"):
        finish(tmp_path)
    assert not calls
    assert json.loads((tmp_path / "comparison-pause.json").read_text())[
        "resume_required"
    ]

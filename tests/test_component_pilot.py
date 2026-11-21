"""Exercise cmp1 over real threads/HTTP and the mandatory native backend."""

import json
import gzip
import threading
from urllib.request import Request, urlopen

import pytest
import yaml

from performance_models import System, QueueingNetworkType
from scripts import run_component_pilot as pilot
from scripts.component_pilot_server import (
    Engine,
    Gate,
    Rejected,
    Server,
    handler,
    FAMILIES,
)


@pytest.fixture
def engine():
    value = Engine()
    yield value
    value.close()


@pytest.mark.parametrize("family", FAMILIES)
def test_fixture_executes_real_http_work_and_records_phases(engine, family):
    engine.reset(family, 1, 41)
    server = Server(("127.0.0.1", 0), handler(engine))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        request = Request(
            f"http://127.0.0.1:{server.server_port}/work",
            data=json.dumps(dict(id="probe", key="key-0", size_bytes=65536)).encode(),
        )
        with urlopen(request, timeout=5) as response:
            result = json.load(response)
        assert result["response_seconds"] > 0
        assert engine.status()["in_flight"] == 0
        assert any(e["phase"] == "response" for e in engine.events)
        if family == "broker":
            assert result["publisher_ack_seconds"] < result["response_seconds"]
    finally:
        server.shutdown()
        server.server_close()


def test_gate_rejects_overflow_and_releases_capacity():
    gate = Gate(1, 0)
    with gate.enter():
        with pytest.raises(Rejected, match="queue_full"):
            with gate.enter():
                pass
    assert gate.status() == dict(active=0, queued=0, capacity=1)


def test_gate_timeout_does_not_cancel_the_active_owner():
    gate = Gate(1, 2, 0.01)
    with gate.enter():
        with pytest.raises(Rejected, match="queue_timeout"):
            with gate.enter():
                pass
        assert gate.status()["active"] == 1
    assert gate.status()["queued"] == 0


def test_reset_cannot_discard_an_old_attempt(engine):
    entered, release = threading.Event(), threading.Event()

    def attempt():
        entered.set()
        assert release.wait(5)

    future = engine.spawn(attempt)
    assert entered.wait(1)
    try:
        with pytest.raises(ValueError, match="old attempts"):
            engine.reset("external", 1, 41)
    finally:
        release.set()
        future.result(timeout=2)


def fitted(family, capacity):
    return dict(
        family=family,
        capacity=capacity,
        residuals=[0.001, 0.002],
        cache_hit_ratio=0.2,
        phases={
            p: [0.001, 0.002]
            for p in (
                "publish",
                "worker_work",
                "worker_block",
                "pool_hold",
                "cache_lookup",
                "cache_backend",
                "link_work",
                "api_delay",
                "function_delay",
            )
        },
    )


@pytest.mark.parametrize("family", FAMILIES)
@pytest.mark.parametrize("capacity", [1, 2])
@pytest.mark.parametrize("enriched", [True, False])
def test_frozen_models_validate_and_execute_on_native_backend(
    family, capacity, enriched
):
    raw = pilot.model(fitted(family, capacity), 5, "poisson", enriched=enriched)
    network = System("component-pilot", raw, QueueingNetworkType.OPEN)
    configuration = tuple(
        network.get_replica_constraints()[n][0] for n in network.get_services()
    )
    network.configure_system_replicas(configuration)
    result = network.simulate_measurement_replications(
        warmup_duration=0.1, measurement_duration=1, maximum_drain_time=5, seeds=(41,)
    )[0]
    assert result.measured_request_counts == result.completed_measured_request_counts
    assert result.measured_request_counts["r"] > 0


def test_deployment_permissions_and_resources_are_owned(tmp_path):
    pilot.render(tmp_path)
    bootstrap = list(yaml.safe_load_all((tmp_path / "bootstrap.yaml").read_text()))
    workload = list(yaml.safe_load_all((tmp_path / "workload.yaml").read_text()))
    assert all(
        d["kind"] not in {"ClusterRole", "ClusterRoleBinding"} for d in bootstrap
    )
    assert all(d["metadata"]["namespace"] == pilot.NAMESPACE for d in workload)
    members = [d for d in workload if d["kind"] == "StatefulSet"]
    assert len(members) == 1 and members[0]["spec"]["replicas"] == 1
    assert not members[0]["spec"].get("volumeClaimTemplates")


def test_collect_retains_the_arrival_cohort_over_http(engine, tmp_path, monkeypatch):
    main = Server(("127.0.0.1", 0), handler(engine))
    evidence = Server(("127.0.0.1", 0), handler(engine, evidence=True))
    for s in (main, evidence):
        threading.Thread(target=s.serve_forever, daemon=True).start()
    monkeypatch.setattr(pilot.common, "INGRESS_IP", f"127.0.0.1:{main.server_port}")
    monkeypatch.setattr(pilot, "EVIDENCE", f"http://127.0.0.1:{evidence.server_port}")
    observer = pilot.Evidence(tmp_path)
    try:
        result = pilot.collect(
            tmp_path,
            observer,
            run_id="short",
            family="pool",
            capacity=1,
            rate=40,
            process="poisson",
            seed=41,
            warmup=0.01,
            measurement=0.15,
        )
        assert result["count"] > 0
        assert result["final_state"]["in_flight"] == 0
        assert not result["evidence_lost"]
        assert (tmp_path / "runs/short/receipt.json").exists()
    finally:
        for s in (main, evidence):
            s.shutdown()
            s.server_close()


def test_freeze_does_not_consume_target_outcomes(tmp_path, monkeypatch):
    monkeypatch.setitem(pilot.PLAN, "reference_fractions", [0.25])
    monkeypatch.setitem(pilot.PLAN, "arrivals", ["poisson"])
    monkeypatch.setitem(pilot.PLAN, "simulation_replications", 2)
    monkeypatch.setitem(pilot.PLAN, "target_warmup_seconds", 0.1)
    monkeypatch.setitem(pilot.PLAN, "target_measurement_seconds", 1)
    predictions = pilot.freeze_predictions(tmp_path, fitted("pool", 1))
    assert len(predictions) == 2
    assert all(r["simulation"]["status"] == "PREDICTED" for r in predictions)
    assert all(r["simulation"]["certified_feasible"] is False for r in predictions)
    assert not (tmp_path / "target-results.json").exists()


def test_characterization_exports_and_consumes_portable_phase_evidence(tmp_path):
    path = tmp_path / "runs/characterization-pool"
    path.mkdir(parents=True)
    rows = [
        dict(
            id=str(i), tagged=True, latency_seconds=0.011, server_response_seconds=0.01
        )
        for i in range(50)
    ]
    events = [
        dict(request_id=str(i), phase="pool_hold", seconds=0.01) for i in range(50)
    ]
    with gzip.open(path / "requests.jsonl.gz", "wt") as stream:
        for row in rows:
            stream.write(json.dumps(row) + "\n")
    with gzip.open(path / "events.json.gz", "wt") as stream:
        json.dump(dict(events=events), stream)
    run = dict(
        run_id="characterization-pool",
        timed_out=False,
        evidence_lost=False,
        materially_late_fraction=0,
        measurement_start_epoch=1767225600,
        measurement_end_epoch=1767225660,
    )
    fitted = pilot.profile(tmp_path, "pool", 1, [run])
    raw = pilot.model(fitted, 5, "poisson", enriched=True)
    model = System(
        "portable-pilot",
        raw,
        QueueingNetworkType.OPEN,
        workload_base_path=tmp_path / "models",
    )
    provenance = model.get_component_measurement_provenance()
    assert len(provenance["sources"]["phases"]["sha256"]) == 64
    assert provenance["estimates"]["pool_hold"]["value"]["samples"] == [0.01] * 50


def test_cleanup_never_restarts_comparison(tmp_path, monkeypatch):
    pilot.write(tmp_path / "cleanup.json", dict(required=True))
    calls = []

    def kube(*args, **kwargs):
        if "get" in args and "statefulset" in args:
            return json.dumps({"metadata": {"labels": {"kubesizer.campaign": "cmp1"}}})
        if "pods" in args:
            return '{"items":[]}'
        calls.append(args)
        return ""

    monkeypatch.setattr(pilot, "kube", kube)
    monkeypatch.setattr(pilot, "command", lambda *a, **k: calls.append(a))
    pilot.finish(tmp_path)
    pilot.finish(tmp_path)
    assert sum("--replicas=0" in c for c in calls) == 1
    assert all(c[:2] != ("systemctl", "start") for c in calls)


def test_client_boundary_loss_is_retained_but_not_scored_as_model_error(tmp_path):
    prediction = dict(
        cell="pool-c1-poisson-u25",
        variant="enriched",
        simulation=dict(mean_seconds=0.01, failure_rate=0),
        analytical={},
    )
    target = dict(
        cell=prediction["cell"],
        run_id="client-limited",
        count=100,
        failures=1,
        mean_all_terminal_seconds=0.02,
        timed_out=False,
        evidence_lost=False,
        materially_late_fraction=0,
        maximum_scheduling_delay_seconds=0.001,
        missing_service_boundaries=1,
    )
    pilot.score(tmp_path, [prediction], [target])
    row = json.loads((tmp_path / "scores.json").read_text())[0]
    assert not row["eligible"]
    assert row["ape_percent"] is None
    assert row["observed_failure_rate"] == 0.01

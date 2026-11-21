"""Protect real-replication declarations, provenance and fail-closed modeling."""

import copy
import json
from pathlib import Path

import pytest
import yaml

from performance_models import System, QueueingNetworkType
from scripts.run_postgres_pilot import MEMBERS, PLAN, cpu_deltas, model, render
from scripts.postgres_pilot_agent import Gateway


def profile():
    return dict(
        cpu_demands_seconds={"primary": 0.001, "standby1": 0.01, "standby2": 0.012},
        replay_cpu_demands_seconds={"standby1": 0.0001, "standby2": 0.0002},
        local_samples_seconds={
            "primary": [0.005, 0.01],
            "standby1": [0.02, 0.03],
            "standby2": [0.03, 0.04],
        },
        residual_seconds={c: [0.02, 0.03] for c in ("read", "write")},
        replay_witness={
            "samples": [
                {
                    "standbys": {
                        n: {"seconds_since_local_commit": v}
                        for n, v in (("standby1", 0.01), ("standby2", 0.02))
                    }
                }
            ]
        },
    )


@pytest.mark.parametrize("mode", ["async", "quorum1", "all"])
def test_frozen_models_share_evidence_and_keep_all_replay_work(mode):
    legacy = model(profile(), mode, 2, False)
    enriched = model(profile(), mode, 2, True)
    for c in ("read", "write"):
        assert enriched["request_classes"][c].pop("call_policies")
    assert legacy == enriched
    network = System(
        "postgres", model(profile(), mode, 2, True), QueueingNetworkType.OPEN
    )
    assert all(v == (1, 1) for v in network.get_replica_constraints().values())
    rates = network.get_effective_arrival_rates()
    assert rates[network.get_service("standby1").get_id()] > 0.5
    assert rates[network.get_service("standby2").get_id()] > 0.5
    assert network.get_strict_analytical_incompatibilities()
    rep = network.simulate_measurement_replications(
        warmup_duration=1, measurement_duration=5, seeds=(7,)
    )[0]
    assert all(rep.request_class_latencies[c] for c in ("read", "write"))


def test_cpu_delta_does_not_join_reused_or_new_process_ids():
    first = {"members": {n: {"processes": []} for n in MEMBERS}}
    last = copy.deepcopy(first)
    first["members"]["standby1"]["processes"] = [
        dict(pid=17, start_ticks=3, cpu_seconds=1)
    ]
    last["members"]["standby1"]["processes"] = [
        dict(pid=17, start_ticks=3, cpu_seconds=1.2, command="postgres: query"),
        dict(pid=18, start_ticks=4, cpu_seconds=100, command="postgres: startup"),
        dict(pid=19, start_ticks=5, cpu_seconds=1, command="postgres: query"),
    ]
    last["pids"] = {"primary": [], "standby1": [17, 19], "standby2": []}
    assert cpu_deltas(first, last)["standby1"] == pytest.approx(
        dict(query=0.2, replay=0, other=0)
    )


def test_replay_proxy_below_cpu_work_abstains():
    value = profile()
    value["replay_cpu_demands_seconds"]["standby1"] = 1
    with pytest.raises(ValueError, match="cannot be separated"):
        model(value, "quorum1", 2, True)


def test_cpu_quota_is_explicit_and_preserves_the_measured_read_path():
    declaration = {
        n: dict(quota_seconds=0.05, period_seconds=0.1, parallelism=2) for n in MEMBERS
    }
    ordinary = model(profile(), "async", 2, True)
    quota = model(profile(), "async", 2, True, cpu_bandwidths=declaration)
    assert quota["request_classes"] == ordinary["request_classes"]
    times = quota["traffic_model"]["service_times"]
    assert times["standby1"]["read"]["mean"] == pytest.approx(0.02)
    assert (
        times["standby1"]["non_cpu_blocking_time"]
        == ordinary["traffic_model"]["service_times"]["standby1"][
            "non_cpu_blocking_time"
        ]
    )
    assert times["standby1"]["cpu_bandwidth"] == declaration["standby1"]
    assert "cpu_bandwidth" not in ordinary["traffic_model"]["service_times"]["standby1"]


def test_render_uses_separate_persistent_physical_standbys(tmp_path):
    render(tmp_path)
    resources = list(yaml.safe_load_all((tmp_path / "workload.yaml").read_text()))
    databases = [r for r in resources if r["kind"] == "StatefulSet"]
    assert len(databases) == 3
    assert {d["metadata"]["name"] for d in databases} == set(MEMBERS)
    for d in databases:
        assert d["spec"]["replicas"] == 1
        assert d["spec"]["volumeClaimTemplates"]
        pod = d["spec"]["template"]["spec"]
        assert pod["shareProcessNamespace"]
        assert not pod.get("hostPID", False)
        if d["metadata"]["name"] != "primary":
            assert "pg_basebackup" in pod["initContainers"][0]["command"][-1]
    source = next(r for r in resources if r["kind"] == "ConfigMap")["data"]
    assert "host replication replicator" in source["init.sh"]
    assert "CREATE ROLE replicator" in source["init.sh"]
    assert "--hash=sha256:" in source["requirements.txt"]
    assert json.loads((tmp_path / "protocol.json").read_text()) == PLAN
    assert all(r["kind"] != "Secret" for r in resources)


def test_gateway_rejects_inflight_mode_change_before_touching_database():
    gateway = Gateway.__new__(Gateway)
    import threading

    gateway.lock = threading.Lock()
    gateway.counts = {"in_flight": 1}
    with pytest.raises(ValueError, match="active traffic"):
        gateway.configure("all")
    with pytest.raises(ValueError, match="Unknown mode"):
        gateway.configure("unknown")


@pytest.mark.parametrize(
    "layout, expected",
    [(None, [1, 0, 0, 0]), ("one", [0, 0, 0, 0]), ("two", [1, 0, 1, 0])],
)
def test_read_layout_routes_the_same_keys_to_declared_standbys(monkeypatch, layout, expected):
    from scripts import postgres_pilot_agent as agent

    class FakePool:
        def __init__(self, *args, **kwargs):
            pass

        def execute(self, *args):
            return []

    monkeypatch.setenv("PGPASSWORD", "test")
    if layout is None:
        monkeypatch.delenv("PGPILOT_READ_LAYOUT", raising=False)
    else:
        monkeypatch.setenv("PGPILOT_READ_LAYOUT", layout)
    monkeypatch.setattr(agent, "Pool", FakePool)
    gateway = Gateway()
    gateway.mode = "async"
    members = [
        gateway.work({"mode": "async", "request_class": "read", "key": key, "id": key})["member"]
        for key in range(4)
    ]
    assert members == [MEMBERS[1 + index] for index in expected]


def test_single_standby_model_does_not_require_a_second_standby_profile():
    inputs = profile()
    for field in ("cpu_demands_seconds", "replay_cpu_demands_seconds", "local_samples_seconds"):
        del inputs[field]["standby2"]
    del inputs["replay_witness"]["samples"][0]["standbys"]["standby2"]
    resolved = model(inputs, "async", 30, True, read_weights={"standby1": 1.0})
    system = System("one read standby", resolved, QueueingNetworkType.OPEN)
    assert set(system.get_services()) == {"router", "primary", "standby1", "standby1_ack"}
    assert resolved["request_classes"]["write"]["calls"]["primary"] == {"standby1": 1}

"""Check that the two sizing candidates preserve erasure and workload semantics."""

import pytest

from performance_models import QueueingNetworkType, System
from scripts import minio_sizing_lab as lab
from scripts import run_minio_sizing as study


def test_erasure_sets_must_span_four_distinct_workers():
    names = lab.members(8)
    placement = {name: lab.NODES[index % 4] for index, name in enumerate(names)}
    admin = {"info": {
        "backend": {"backendType": "Erasure", "onlineDisks": 8, "offlineDisks": 0,
                    "standardSCParity": 2, "totalSets": [2], "totalDrivesPerSet": [4]},
        "servers": [
            {"endpoint": f"{name}:9000", "state": "online",
             "drives": [{"pool_index": 0, "set_index": index // 4, "state": "ok"}]}
            for index, name in enumerate(names)
        ],
    }}
    assert len(lab.verify_layout(8, admin, placement)["members_by_set"]) == 2
    placement[names[-1]] = placement[names[-2]]
    with pytest.raises(ValueError, match="four independent worker nodes"):
        lab.verify_layout(8, admin, placement)


def test_layout_cost_changes_without_changing_request_classes_or_deadlines():
    profile = {
        "GET": {"per_request_cpu_seconds": 0.01, "per_drive_busy_seconds": 0.001,
                "observed_mean_seconds": 0.09},
        "PUT": {"per_request_cpu_seconds": 0.02, "per_drive_busy_seconds": 0.04,
                "observed_mean_seconds": 0.25},
    }
    phase = [0.03, 0.04, 0.045]
    small, large = [study.workload(size, 8.0, profile, phase) for size in lab.LAYOUTS]
    assert small["request_classes"] == large["request_classes"]
    assert small["traffic_model"] == large["traffic_model"]
    for size, config in zip(lab.LAYOUTS, (small, large), strict=True):
        system = System("trial", config, QueueingNetworkType.OPEN)
        assert system.get_replica_constraints() == {
            "cpu": (size, size), "stripe_io": (size // 4, size // 4),
        }


def test_every_target_layout_pair_uses_the_same_input_seed():
    order = study.target_order()
    assert len(order) == 18
    for rate in study.PLAN["target_equal_per_class_rates"]:
        for pair in range(study.PLAN["target_pairs"]):
            matched = [run for run in order if (run["rate"], run["pair"]) == (rate, pair)]
            assert {run["layout"] for run in matched} == set(lab.LAYOUTS)
            assert matched[0]["seed"] == matched[1]["seed"]


@pytest.mark.parametrize(
    "cpu, source",
    [
        ("cpuacct_usage_ns 1750000000\nnr_throttled 3", "cpuacct.usage"),
        ("usage_usec 1750000\nnr_throttled 3", "cpu.stat"),
    ],
)
def test_container_cpu_counter_units_and_disk_scope(monkeypatch, cpu, source):
    sample = (
        cpu + "\nMOUNTINFO\n30 20 8:0 / /data rw - ext4 /dev/sda rw"
        + "\nDISKSTATS\n8 0 sda 3 0 1 5 9 0 11 13 0 15 17"
        + "\nNETDEV\n eth0: 100 0 0 0 0 0 0 0 200 0"
        + "\nFREE\nFilesystem Blocks Used Available Capacity Mounted\n/dev/sda 1000 100 900 10% /data\n"
    )
    monkeypatch.setattr(lab, "kube", lambda *args, **kwargs: sample)
    value = study.counter("member")
    assert value["cpu_source"] == source
    assert value["cpu"]["usage_usec"] == 1_750_000
    assert value["volume"]["busy_ms"] == 15
    assert value["volume_free_bytes"] == 900 * 1024

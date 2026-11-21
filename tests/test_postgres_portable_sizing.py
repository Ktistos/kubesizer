"""Check matched schedules and the aggregate CPU attribution for the lab trial."""

import pytest

from scripts import run_postgres_portable_sizing as study


def window(cpu_rates, request_class=None, rate=0):
    start = 100
    elapsed = 60
    return {
        "start_epoch": start,
        "end_epoch": start + elapsed,
        "snapshots": [
            {"members": {
                member: {"aggregate_container_cpu": {"epoch": tick, "usage_seconds": 4 + cpu * (tick - start)}}
                for member, cpu in cpu_rates.items()
            }}
            for tick in range(start, start + elapsed + 1, 15)
        ],
        "events": [
            {"type": "response", "request_class": request_class, "epoch": start + ordinal / rate}
            for ordinal in range(rate * elapsed + int(rate > 0))
        ],
    }


def test_aggregate_cpu_subtracts_idle_and_scales_by_aligned_class_completions():
    idle = window({"primary": 0.05, "standby1": 0.01})
    windows = {
        "read-only": window({"standby1": 0.41}, "read", 20),
        "write-only": window({"primary": 0.15, "standby1": 0.03}, "write", 10),
    }
    profile = study.measure_arm("aggregate", windows, idle, {})
    assert profile["cpu_demands_seconds"] == pytest.approx({"primary": 0.01, "standby1": 0.02})
    assert profile["replay_cpu_demands_seconds"] == pytest.approx({"standby1": 0.002})
    assert profile["cpu_measurement_windows"]["read"]["completed"] == {"read": 1200, "write": 0}

    windows["write-only"] = window({"primary": 0.15, "standby1": 0.009}, "write", 10)
    with pytest.raises(ValueError, match="did not identify positive incremental CPU for replay"):
        study.measure_arm("aggregate", windows, idle, {})


def test_rates_reuse_the_same_seed_with_both_layouts_in_every_physical_pair():
    order = study.target_order()
    assert len(order) == 18
    for pair in range(3):
        for rate in study.PLAN["target_rates_rps"]:
            matched = [r for r in order if r["pair"] == pair and r["rate_rps"] == rate]
            assert {sample["layout"] for sample in matched} == {"one", "two"}
            assert matched[0]["seed"] == matched[1]["seed"]

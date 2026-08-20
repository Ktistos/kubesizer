from types import SimpleNamespace

import pytest

from runtime_estimation import (
    LiveWorkflowRuntimeEstimator,
    StageRuntimeEstimate,
    format_live_runtime_snapshot,
)


class ManualClock:
    def __init__(self):
        self.value = 0.0

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += seconds


def estimate():
    return SimpleNamespace(
        stages=tuple(
            StageRuntimeEstimate(name, ordinary, ceiling, "ordinary", "ceiling")
            for name, ordinary, ceiling in (
                ("Stage A — calibration", 10.0, 20.0),
                ("Stage B — replica optimization", 100.0, 200.0),
                ("Stage C — final validation", 30.0, 60.0),
                ("Stage D — node placement", 40.0, 80.0),
            )
        )
    )


def test_live_estimate_combines_observed_rate_with_planning_range():
    clock = ManualClock()
    output = []
    subject = LiveWorkflowRuntimeEstimator(
        estimate(),
        clock=clock,
        output=output.append,
        minimum_update_interval_seconds=0,
    )

    subject.start_stage(
        "replica_optimization",
        total_units=10,
        unit_name="configurations",
    )
    clock.advance(10)
    snapshot = subject.update_stage(completed_units=2)

    assert snapshot.stage_elapsed_seconds == pytest.approx(10.0)
    assert snapshot.stage_remaining_low_seconds == pytest.approx(40.0)
    assert snapshot.stage_remaining_high_seconds == pytest.approx(160.0)
    assert snapshot.overall_remaining_low_seconds == pytest.approx(110.0)
    assert snapshot.overall_remaining_high_seconds == pytest.approx(300.0)
    assert "2/10 configurations" in output[-1]

    completed = subject.complete_stage(detail="selected=EXACT")

    assert completed.completed
    assert completed.completed_units == 2
    assert completed.total_units == 10
    assert completed.overall_remaining_low_seconds == pytest.approx(70.0)
    assert completed.overall_remaining_high_seconds == pytest.approx(140.0)


def test_live_updates_are_throttled_without_losing_latest_snapshot():
    clock = ManualClock()
    output = []
    subject = LiveWorkflowRuntimeEstimator(
        estimate(),
        clock=clock,
        output=output.append,
        minimum_update_interval_seconds=1.0,
    )
    subject.start_stage("calibration")
    assert len(output) == 1

    clock.advance(0.5)
    subject.update_stage(event="pilot_batch_completed")
    assert len(output) == 1
    assert subject.last_snapshot.event == "pilot_batch_completed"

    clock.advance(0.6)
    subject.update_stage(event="pilot_horizon_analyzing")
    assert len(output) == 2


def test_callbacks_translate_replica_and_scalable_placement_progress():
    clock = ManualClock()
    subject = LiveWorkflowRuntimeEstimator(
        estimate(),
        clock=clock,
        output=lambda _message: None,
        minimum_update_interval_seconds=0,
    )
    subject.start_stage("replica_optimization")
    replica_callback = subject.callback("replica_optimization")
    replica_callback(
        "replica_optimization_started",
        {"completed_configurations": 0, "total_configurations": 64},
    )
    replica_callback(
        "replica_configuration_evaluated",
        {"completed_configurations": 3},
    )
    assert subject.last_snapshot.completed_units == 3
    assert subject.last_snapshot.total_units == 64
    subject.complete_stage()

    subject.start_stage("node_placement")
    placement_callback = subject.callback("node_placement")
    placement_callback(
        "placement_search_started",
        {"completed_search_states": 0, "total_search_states": 1_000},
    )
    placement_callback(
        "placement_search_progress",
        {"completed_search_states": 250, "total_search_states": 1_000},
    )
    assert subject.last_snapshot.completed_units == 250
    assert subject.last_snapshot.total_units == 1_000
    assert subject.last_snapshot.unit_name == "search states"


def test_live_snapshot_format_labels_ranges_as_estimates():
    clock = ManualClock()
    subject = LiveWorkflowRuntimeEstimator(
        estimate(),
        clock=clock,
        output=lambda _message: None,
        minimum_update_interval_seconds=0,
    )
    snapshot = subject.start_stage("final_validation", total_units=30)

    report = format_live_runtime_snapshot(snapshot)

    assert "stage remaining" in report
    assert "overall remaining" in report
    assert "0/30" in report

"""Command-line interface for calibrated Kubesizer workflows.

The CLI is intentionally a thin orchestration layer over the repository's
public Python interfaces.  Calibration profiles remain in memory unless the
caller explicitly supplies ``--out``.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import threading
import time
from dataclasses import fields, is_dataclass, replace
from enum import Enum
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]

from calibration import CalibrationOptions
from characterization import (
    build_characterized_system_variants,
    estimate_statistical_stability_floor,
    load_and_prepare_initial_sizing,
)
from optimization import (
    NodePlacementProblem,
    OptimizationMode,
    ReplicaEvaluationMode,
    optimize_node_placement,
)
from performance_models import Cluster, QueueingNetworkType, System
from statistical_workflow import (
    calibrate,
    calibrate_fixed_placement,
    optimize_with_analytical_model,
    optimize_with_calibrated_simulation,
    validate_final_configuration,
)
from runtime_estimation import estimate_workflow_runtime


CLI_VERSION = "0.2.0"
DEFAULT_MAXIMUM_REPLICAS = 10
DEFAULT_WORKERS = min(8, os.cpu_count() or 1)


class CliUsageError(ValueError):
    """Raised for invalid combinations of otherwise valid CLI arguments."""


def _jsonable(value: Any) -> Any:
    """Convert repository result objects into strict JSON-compatible values."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        return "Infinity" if value > 0 else "-Infinity"
    if isinstance(value, Enum):
        return _jsonable(value.value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(item) for item in value]
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        return _jsonable(to_dict())
    if is_dataclass(value):
        return {
            field.name: _jsonable(getattr(value, field.name)) for field in fields(value)
        }
    return str(value)


def _read_yaml(path: Path, label: str) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as stream:
            value = yaml.safe_load(stream)
    except OSError as error:
        raise CliUsageError(f"Cannot read {label} file {path}: {error}") from error
    except yaml.YAMLError as error:
        raise CliUsageError(f"Invalid YAML in {label} file {path}: {error}") from error
    if not isinstance(value, dict):
        raise CliUsageError(f"The {label} file must contain a YAML mapping.")
    return value


def _load_system(
    workload_path: str | Path,
) -> tuple[System, Path]:
    path = Path(workload_path).expanduser().resolve()
    workload = _read_yaml(path, "workload")
    return (
        System(
            path.stem,
            workload,
            QueueingNetworkType.OPEN,
            workload_base_path=path.parent,
        ),
        path,
    )


def _load_workflow_system(args):
    """Load either a native workload or the portable evidence workflow."""
    portable_paths = (args.deployment, args.measurements)
    if any(portable_paths) and not all(portable_paths):
        raise CliUsageError(
            "--deployment and --measurements must be supplied together."
        )
    if not all(portable_paths):
        system, workload_path = _load_system(args.workload)
        return system, workload_path, None, None

    workload_path = Path(args.workload).expanduser().resolve()
    plan = load_and_prepare_initial_sizing(
        args.deployment,
        workload_path,
        args.measurements,
    )
    variants = build_characterized_system_variants(
        plan.inputs,
        plan.characterization,
    )
    if len(variants) != 1:
        names = [variant.name for variant, _system in variants]
        raise CliUsageError(
            "The measurements leave call ordering ambiguous and produce "
            f"multiple simulation variants {names}. Declare call_execution "
            "for every request class before optimization."
        )
    _variant, system = variants[0]
    service_order = tuple(
        service.get_name()
        for service in sorted(
            system.get_services().values(),
            key=lambda item: item.get_id(),
        )
    )
    stability_floor = estimate_statistical_stability_floor(
        plan.inputs,
        plan.characterization,
        analytical_minimum=system.get_minimum_stable_replicas(),
        maximum_configuration=(args.max_replicas,) * len(service_order),
        service_order=service_order,
    )
    stability_floor.assert_confidence_bounded_stable()
    minimum_configuration = stability_floor.configuration
    # The statistically controlled floor is both the hard lower bound and the
    # construction start.  All further replica additions must therefore be
    # justified by the selected latency/SLO evaluator, rather than inherited
    # from a separate utilization-based sizing heuristic.
    initial_configuration = minimum_configuration
    return system, workload_path, initial_configuration, minimum_configuration


def _resources_or_skip(
    resources_path: str | None,
    *,
    skip_placement: bool,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    if skip_placement:
        return None, {
            "status": "SKIPPED",
            "reason": "Node placement was disabled with --skip-placement.",
        }
    if resources_path is None:
        return None, {
            "status": "SKIPPED",
            "reason": "No resources file was supplied.",
        }
    path = Path(resources_path).expanduser().resolve()
    if not path.is_file():
        return None, {
            "status": "SKIPPED",
            "reason": f"Resources file is not available: {path}",
        }
    return _read_yaml(path, "resources"), {
        "status": "PENDING",
        "resources_file": str(path),
    }


def _validate_analytical_placement(system, placement_model):
    """Reject a placed analytical recommendation that no longer meets its SLOs."""
    multipliers = tuple(
        float(value)
        for value in placement_model.get("execution_time_multipliers", {}).values()
    )
    if any(
        not math.isclose(value, 1.0, rel_tol=0.0, abs_tol=1e-12)
        for value in multipliers
    ):
        raise CliUsageError(
            "Analytical replica evaluation cannot certify node execution-time "
            "multipliers. Use --replica-evaluation simulation or supply unit "
            "multipliers."
        )
    system.configure_simulation_placement(**placement_model)
    limits = getattr(
        system,
        "get_request_class_mean_latency_limits",
        lambda: {},
    )()
    if not limits:
        return None
    means = system.get_request_class_mean_latencies()
    violations = tuple(
        sorted(
            (
                request_class,
                float(means[request_class]),
                float(limit),
            )
            for request_class, limit in limits.items()
            if float(means[request_class]) > float(limit)
        )
    )
    if violations:
        details = "; ".join(
            f"{request_class}: mean {mean:.6g}s > SLO {limit:.6g}s"
            for request_class, mean, limit in violations
        )
        raise RuntimeError(
            "The selected node placement invalidates analytical mean-latency "
            f"feasibility ({details}). No recommendation was emitted."
        )
    return None


def _calibration_options(args: argparse.Namespace) -> CalibrationOptions:
    options = CalibrationOptions()
    replacements = {}
    names = {
        "warmup_replications": "warmup_replications",
        "warmup_validation_replications": "warmup_validation_replications",
        "measurement_replications": "measurement_calibration_replications",
        "minimum_replications": "minimum_optimization_replications",
        "maximum_replications": "maximum_optimization_replications",
        "replication_batch_size": "replication_batch_size",
        "bootstrap_resamples": "bootstrap_resample_count",
        "final_validation_replications": "final_validation_replications",
    }
    for argument_name, field_name in names.items():
        value = getattr(args, argument_name, None)
        if value is not None:
            replacements[field_name] = value
    return replace(options, **replacements)


class _ConciseProgressReporter:
    """Render sparse progress with elapsed time and advisory remaining time."""

    _STAGE_ESTIMATE_NAMES = {
        "calibration": "Stage A — calibration",
        "replica_sizing": "Stage B — replica optimization",
        "validation": "Stage C — final validation",
        "placement": "Stage D — node placement",
        "placement_calibration": "Stage E — placement-aware calibration",
    }

    _STAGE_EVENTS = {
        "pilot_batch_started": "calibration",
        "validation_started": "calibration",
        "measurement_trajectories_started": "calibration",
        "calibration_completed": "calibration",
        "replica_optimization_started": "replica_sizing",
        "replica_configuration_evaluated": "replica_sizing",
        "replica_optimization_completed": "replica_sizing",
        "final_validation_started": "validation",
        "final_validation_replications_completed": "validation",
        "final_validation_completed": "validation",
        "placement_search_started": "placement",
        "placement_search_progress": "placement",
        "placement_search_completed": "placement",
        "placement_calibration_started": "placement_calibration",
        "placement_calibration_pilot_started": "placement_calibration",
        "placement_calibration_validation_started": "placement_calibration",
        "placement_calibration_measurement_started": "placement_calibration",
        "placement_calibration_completed": "placement_calibration",
    }

    _COMPLETION_EVENTS = {
        "calibration_completed",
        "replica_optimization_completed",
        "final_validation_completed",
        "placement_search_completed",
        "placement_calibration_completed",
    }

    def __init__(
        self,
        *,
        json_output=False,
        output=None,
        clock=None,
    ):
        self.json_output = bool(json_output)
        self.output = sys.stderr if output is None else output
        self.clock = clock or time.perf_counter
        self._seen = set()
        self._milestones = {}
        self._workflow_started = self.clock()
        self._stage_started = None
        self._active_stage = None
        self._active_text = None
        self._completed_units = None
        self._total_units = None
        self._completed_stages = set()
        self._expected_stages = ()
        self._stage_estimates = {}
        self._runtime_estimate = None
        self._line_open = False
        self._lock = threading.RLock()
        is_terminal = getattr(self.output, "isatty", lambda: False)()
        self._live_terminal = bool(is_terminal and not self.json_output)

    def configure_workflow(self, expected_stages, runtime_estimate=None):
        self._expected_stages = tuple(expected_stages)
        self._runtime_estimate = runtime_estimate
        self._stage_estimates = (
            {}
            if runtime_estimate is None
            else {stage.stage: stage for stage in runtime_estimate.stages}
        )

    def runtime_estimation_started(self):
        with self._lock:
            self._start_stage("planning", "Estimating runtime")
            self._publish(final=False)

    def runtime_estimation_completed(self, estimate):
        with self._lock:
            self._runtime_estimate = estimate
            self._stage_estimates = {stage.stage: stage for stage in estimate.stages}
            low = estimate.total_ordinary_planning_seconds
            ceiling = estimate.total_configured_ceiling_seconds
            self._active_text = "Runtime estimate ready: about " + self._duration(low)
            if ceiling <= max(3_600.0, low * 20.0):
                self._active_text += (
                    " (configured stress-path projection "
                    + self._duration(ceiling)
                    + ")"
                )
            else:
                self._active_text += "; no finite worst-case guarantee"
            self._publish(final=True, include_eta=False)
            self._finish_stage("planning")

    def runtime_estimation_failed(self):
        with self._lock:
            self._active_text = (
                "Runtime estimate unavailable; continuing with elapsed-time progress"
            )
            self._publish(final=True, include_eta=False)
            self._finish_stage("planning")

    def close(self):
        with self._lock:
            if self._line_open:
                self.output.write("\n")
                self.output.flush()
                self._line_open = False

    @staticmethod
    def _duration(seconds):
        if seconds is None or not math.isfinite(seconds) or seconds < 0:
            return "unavailable"
        rounded = int(round(seconds))
        hours, remainder = divmod(rounded, 3600)
        minutes, seconds = divmod(remainder, 60)
        if hours:
            return f"{hours:d}:{minutes:02d}:{seconds:02d}"
        return f"{minutes:02d}:{seconds:02d}"

    @classmethod
    def _duration_range(cls, low, high):
        if low is None or high is None:
            return "unavailable"
        if math.isclose(low, high, rel_tol=0.01, abs_tol=0.5):
            return cls._duration(high)
        return f"{cls._duration(low)}–{cls._duration(high)}"

    @staticmethod
    def _progress_bar(percentage):
        width = 12
        if percentage is None:
            return "[" + "·" * width + "]"
        filled = min(width, max(0, int(round(width * percentage / 100))))
        return "[" + "#" * filled + "-" * (width - filled) + "]"

    def _start_stage(self, stage, text):
        if self._active_stage == stage:
            self._active_text = text
            return
        self._active_stage = stage
        self._active_text = text
        self._stage_started = self.clock()
        self._completed_units = None
        self._total_units = None

    def _finish_stage(self, stage):
        self._completed_stages.add(stage)
        self._active_stage = None
        self._active_text = None
        self._stage_started = None
        self._completed_units = None
        self._total_units = None

    def _update_units(self, event, details):
        if event in {"replica_optimization_started", "replica_configuration_evaluated"}:
            completed = details.get("completed_configurations")
            total = details.get("total_configurations")
            if total is None and self._total_units is None:
                total = details.get("search_space_configurations")
        elif event in {
            "final_validation_started",
            "final_validation_replications_completed",
        }:
            completed = details.get("completed_replications")
            total = details.get("total_replications")
        elif event in {"placement_search_started", "placement_search_progress"}:
            if "total_search_states" in details:
                completed = details.get("completed_search_states")
                total = details.get("total_search_states")
            else:
                completed = details.get("completed_configurations")
                total = details.get("total_configurations")
        else:
            return
        if isinstance(total, int) and total > 0:
            self._total_units = total
        if isinstance(completed, int) and completed >= 0:
            self._completed_units = completed

    def _percentage(self, *, completed=False):
        if completed:
            return 100
        if not self._total_units or self._completed_units is None:
            return None
        return min(100, max(0, int(100 * self._completed_units / self._total_units)))

    def _remaining_range(self, *, completed=False):
        stage = self._active_stage
        if stage in {None, "planning"} or self._stage_started is None:
            return None, None, "overall"
        elapsed = max(0.0, self.clock() - self._stage_started)
        fraction = (
            None
            if not self._total_units or self._completed_units is None
            else min(1.0, self._completed_units / self._total_units)
        )
        estimate_name = self._STAGE_ESTIMATE_NAMES.get(stage)
        estimate = self._stage_estimates.get(estimate_name)
        if estimate is None:
            if completed:
                return 0.0, 0.0, "stage"
            if fraction is None or fraction <= 0:
                return None, None, "stage"
            empirical = elapsed * (1.0 - fraction) / fraction
            return empirical, empirical, "stage"

        if completed:
            current_low = current_high = 0.0
        elif fraction is None:
            current_low = max(0.0, estimate.ordinary_planning_seconds - elapsed)
            current_high = current_low
        else:
            ordinary = estimate.ordinary_planning_seconds * (1.0 - fraction)
            if fraction > 0:
                empirical = elapsed * (1.0 - fraction) / fraction
                current_low = min(ordinary, empirical)
                current_high = max(ordinary, empirical)
            else:
                current_low = current_high = ordinary

        future_low = future_high = 0.0
        if stage in self._expected_stages:
            current_index = self._expected_stages.index(stage)
            for future_stage in self._expected_stages[current_index + 1 :]:
                if future_stage in self._completed_stages:
                    continue
                future_name = self._STAGE_ESTIMATE_NAMES[future_stage]
                future_estimate = self._stage_estimates.get(future_name)
                if future_estimate is None:
                    return current_low, current_high, "stage"
                future_low += future_estimate.ordinary_planning_seconds
                future_high += future_estimate.ordinary_planning_seconds
        return current_low + future_low, current_high + future_high, "overall"

    def _render_text(self, *, completed=False, include_eta=True):
        elapsed = max(0.0, self.clock() - self._workflow_started)
        percentage = self._percentage(completed=completed)
        parts = [self._active_text or "Working"]
        if self._active_stage != "planning":
            bar = self._progress_bar(percentage)
            if percentage is not None:
                bar += f" {percentage}%"
            parts.append(bar)
        parts.append(f"elapsed {self._duration(elapsed)}")
        if include_eta:
            low, high, scope = self._remaining_range(completed=completed)
            parts.append(
                f"estimated {scope} remaining {self._duration_range(low, high)}"
            )
        return " | ".join(parts)

    def _publish(self, *, final, include_eta=True):
        rendered = self._render_text(completed=final, include_eta=include_eta)
        percentage = self._percentage(completed=final)
        if self.json_output:
            low, high, scope = self._remaining_range(completed=final)
            payload = {
                "progress": {
                    "stage": self._active_stage,
                    "message": self._active_text,
                    "elapsed_seconds": max(0.0, self.clock() - self._workflow_started),
                }
            }
            if percentage is not None:
                payload["progress"]["percent"] = percentage
            if include_eta:
                payload["progress"].update(
                    {
                        "estimated_remaining_scope": scope,
                        "estimated_remaining_low_seconds": low,
                        "estimated_remaining_high_seconds": high,
                    }
                )
            print(json.dumps(payload, sort_keys=True), file=self.output)
            return
        if self._live_terminal:
            self.output.write("\r\033[2K" + rendered)
            if final:
                self.output.write("\n")
                self._line_open = False
            else:
                self._line_open = True
            self.output.flush()
            return
        print(rendered, file=self.output)

    @staticmethod
    def _number(value):
        try:
            return f"{float(value):g}"
        except (TypeError, ValueError):
            return "unknown"

    def _coarse_search_progress(self, stage, details):
        if stage == "replica_sizing":
            completed = details.get("completed_configurations")
            total = self._total_units
            label = "Replica search"
        else:
            completed = details.get(
                "completed_search_states",
                details.get("completed_configurations"),
            )
            total = self._total_units
            label = "Placement search"
        if not isinstance(completed, int) or not isinstance(total, int) or total < 20:
            return None
        percentage = min(100, max(0, int(100 * completed / total)))
        milestone = min(75, (percentage // 25) * 25)
        if milestone < 25 or milestone <= self._milestones.get(stage, 0):
            return None
        self._milestones[stage] = milestone
        return stage, label, milestone

    def _message(self, event, details):
        if event == "pilot_batch_started":
            horizon = self._number(details.get("simulation_horizon"))
            return (
                "calibration",
                f"Calibration: testing warm-up horizon {horizon}.",
                None,
            )
        if event == "validation_started":
            return "calibration", "Calibration: validating warm-up.", None
        if event == "measurement_trajectories_started":
            duration = self._number(details.get("simulation_duration"))
            return (
                "calibration",
                f"Calibration: testing measurement duration {duration}.",
                None,
            )
        if event == "calibration_completed":
            return "calibration", "Calibration complete.", None
        if event == "replica_optimization_started":
            mode = str(details.get("selected_mode", "")).lower()
            suffix = f" ({mode})" if mode else ""
            return "replica_sizing", f"Optimizing replicas{suffix}.", None
        if event == "replica_configuration_evaluated":
            return self._coarse_search_progress("replica_sizing", details)
        if event == "replica_optimization_completed":
            return "replica_sizing", "Replica optimization complete.", None
        if event == "final_validation_started":
            return "validation", "Validating replica recommendation.", None
        if event == "final_validation_completed":
            return "validation", "Final validation complete.", None
        if event == "placement_search_started":
            mode = str(details.get("selected_mode", "")).lower()
            suffix = f" ({mode})" if mode else ""
            return "placement", f"Optimizing node placement{suffix}.", None
        if event == "placement_search_progress":
            return self._coarse_search_progress("placement", details)
        if event == "placement_search_completed":
            return "placement", "Node placement complete.", None
        if event == "placement_calibration_started":
            return (
                "placement_calibration",
                "Calibrating the selected placement.",
                None,
            )
        if event == "placement_calibration_pilot_started":
            horizon = self._number(details.get("simulation_horizon"))
            return (
                "placement_calibration",
                f"Placement calibration: testing warm-up horizon {horizon}.",
                None,
            )
        if event == "placement_calibration_validation_started":
            return (
                "placement_calibration",
                "Placement calibration: validating warm-up.",
                None,
            )
        if event == "placement_calibration_measurement_started":
            duration = self._number(details.get("simulation_duration"))
            return (
                "placement_calibration",
                f"Placement calibration: testing measurement duration {duration}.",
                None,
            )
        if event == "placement_calibration_completed":
            return (
                "placement_calibration",
                "Placement-aware calibration complete.",
                None,
            )
        return None

    def __call__(self, event, details):
        details = dict(details or {})
        with self._lock:
            stage = self._STAGE_EVENTS.get(event)
            if stage is not None and self._active_stage != stage:
                self._start_stage(stage, stage.replace("_", " ").title())
            self._update_units(event, details)
            message = self._message(event, details)
            if message is None:
                return
            stage, text, message_percentage = message
            self._active_text = text.rstrip(".")
            key = (stage, text, message_percentage)
            if key in self._seen:
                return
            self._seen.add(key)
            completed = event in self._COMPLETION_EVENTS
            self._publish(final=completed)
            if completed:
                self._finish_stage(stage)


def _progress_callback(args: argparse.Namespace):
    if getattr(args, "quiet", False):
        return None
    reporter = getattr(args, "_progress_reporter", None)
    if reporter is None:
        reporter = _ConciseProgressReporter(
            json_output=getattr(args, "json", False),
        )
        setattr(args, "_progress_reporter", reporter)
    return reporter


def _notify_progress(args, event, details=None):
    callback = _progress_callback(args)
    if callback is not None:
        callback(event, details or {})


def _placement_calibration_progress_callback(args):
    callback = _progress_callback(args)
    if callback is None:
        return None
    event_mapping = {
        "pilot_batch_started": "placement_calibration_pilot_started",
        "validation_started": "placement_calibration_validation_started",
        "measurement_trajectories_started": (
            "placement_calibration_measurement_started"
        ),
    }

    def report(event, details):
        translated = event_mapping.get(event)
        if translated is not None:
            callback(translated, details)

    return report


def _prepare_runtime_reporting(
    system,
    resources,
    args,
    *,
    replica_mode,
    placement_mode,
):
    reporter = _progress_callback(args)
    if reporter is None:
        return
    evaluation_mode = ReplicaEvaluationMode(args.replica_evaluation.upper())
    expected_stages = []
    if evaluation_mode == ReplicaEvaluationMode.SIMULATION:
        expected_stages.append("calibration")
    expected_stages.append("replica_sizing")
    if resources is not None:
        expected_stages.append("placement")
        if evaluation_mode == ReplicaEvaluationMode.SIMULATION:
            expected_stages.append("placement_calibration")
    if evaluation_mode == ReplicaEvaluationMode.SIMULATION:
        expected_stages.append("validation")
    reporter.configure_workflow(expected_stages)

    if evaluation_mode != ReplicaEvaluationMode.SIMULATION:
        return
    reporter.runtime_estimation_started()
    try:
        estimate = estimate_workflow_runtime(
            system,
            resources,
            options=_calibration_options(args),
            maximum_replicas=args.max_replicas,
            workers=args.workers,
            replica_optimization_mode=replica_mode,
            placement_optimization_mode=placement_mode,
        )
    except Exception:
        reporter.runtime_estimation_failed()
    else:
        reporter.runtime_estimation_completed(estimate)


def _service_recommendations(system, configuration, placement=None):
    services = tuple(system.get_services())
    configuration = tuple(configuration)
    if len(configuration) != len(services):
        raise RuntimeError(
            "Optimizer returned a replica vector with the wrong service count."
        )
    recommendations = {
        service_name: {
            "recommended_replicas": replica_count,
            "node_placement": None if placement is None else {},
        }
        for service_name, replica_count in zip(services, configuration)
    }
    if placement is None:
        return recommendations
    for node_name, service_counts in placement.items():
        for service_name, replica_count in service_counts.items():
            if service_name not in recommendations:
                raise RuntimeError(
                    f"Placement contains unknown service {service_name!r}."
                )
            if replica_count:
                recommendations[service_name]["node_placement"][
                    node_name
                ] = replica_count
    for service_name, recommendation in recommendations.items():
        placed_replicas = sum(recommendation["node_placement"].values())
        if placed_replicas != recommendation["recommended_replicas"]:
            raise RuntimeError(
                f"Placement assigns {placed_replicas} replicas for {service_name!r}, "
                f"but sizing recommended {recommendation['recommended_replicas']}."
            )
    return recommendations


def _run_calibration(system, args, *, minimum_configuration=None):
    result = calibrate(
        system,
        maximum_replicas=args.max_replicas,
        options=_calibration_options(args),
        workers=args.workers,
        minimum_configuration=minimum_configuration,
        progress_callback=_progress_callback(args),
    )
    _notify_progress(args, "calibration_completed")
    return result


def _run_replica_workflow(
    system,
    args,
    replica_mode,
    initial_configuration=None,
    minimum_configuration=None,
):
    evaluation_mode = ReplicaEvaluationMode(args.replica_evaluation.upper())
    kind = system.get_request_class_latency_slo_kind()
    if kind == "percentile" and evaluation_mode != ReplicaEvaluationMode.SIMULATION:
        raise CliUsageError(
            "Percentile latency SLOs require --replica-evaluation simulation."
        )
    if (
        evaluation_mode == ReplicaEvaluationMode.ANALYTICAL
        and any(
            value > 0.0
            for value in getattr(
                system,
                "get_request_class_end_to_end_timeouts",
                lambda: {},
            )().values()
        )
    ):
        raise CliUsageError(
            "Request-class end-to-end deadlines require --replica-evaluation "
            "simulation."
        )
    if evaluation_mode == ReplicaEvaluationMode.ANALYTICAL:
        incompatibilities = getattr(
            system,
            "get_strict_analytical_incompatibilities",
            lambda: (),
        )()
        if incompatibilities:
            raise CliUsageError(
                "The workload contains semantics that require "
                "--replica-evaluation simulation: "
                + "; ".join(incompatibilities)
                + "."
            )
    if kind is None and evaluation_mode == ReplicaEvaluationMode.SIMULATION:
        raise CliUsageError("Simulation optimization requires workload latency SLOs.")

    if evaluation_mode == ReplicaEvaluationMode.ANALYTICAL:
        result = optimize_with_analytical_model(
            system,
            maximum_replicas=args.max_replicas,
            mode=replica_mode,
            minimum_configuration=minimum_configuration,
            initial_configuration=initial_configuration,
            progress_callback=_progress_callback(args),
        )
        return result, None, None

    profile = _run_calibration(
        system,
        args,
        minimum_configuration=minimum_configuration,
    )
    result = optimize_with_calibrated_simulation(
        system,
        profile,
        workers=args.workers,
        mode=replica_mode,
        initial_configuration=initial_configuration,
        progress_callback=_progress_callback(args),
    )
    if getattr(args, "_defer_final_validation", False):
        return result, profile, None
    validation = validate_final_configuration(
        system,
        result.configuration,
        profile,
        workers=args.workers,
        progress_callback=_progress_callback(args),
    )
    _notify_progress(args, "final_validation_completed")
    return result, profile, validation


def _command_run(args):
    (
        system,
        _,
        initial_configuration,
        minimum_configuration,
    ) = _load_workflow_system(args)
    if args.replica_evaluation == "simulation":
        compatibility_check = getattr(
            system,
            "require_simulation_compatible_workload",
            None,
        )
        if compatibility_check is not None:
            compatibility_check()
    resources, _placement_status = _resources_or_skip(
        args.resources,
        skip_placement=args.skip_placement,
    )
    replica_mode = OptimizationMode(args.replica_search.upper())
    placement_mode = OptimizationMode(args.placement_search.upper())
    _prepare_runtime_reporting(
        system,
        resources,
        args,
        replica_mode=replica_mode,
        placement_mode=placement_mode,
    )
    setattr(
        args,
        "_defer_final_validation",
        resources is not None and args.replica_evaluation == "simulation",
    )
    result, _profile, validation = _run_replica_workflow(
        system,
        args,
        replica_mode,
        initial_configuration,
        minimum_configuration,
    )
    if validation is not None and not validation.passed:
        reason = getattr(validation, "failure_reason", None)
        raise RuntimeError(
            "The selected replica configuration failed final validation"
            + (f": {reason}" if reason else ".")
        )
    selected_placement = None
    if resources is not None:
        system.configure_system_replicas(result.configuration)
        cluster = Cluster("cli-cluster", resources, system)
        placement_result = optimize_node_placement(
            NodePlacementProblem(cluster),
            mode=placement_mode,
            progress_callback=_progress_callback(args),
        )
        selected_placement = getattr(
            placement_result,
            "placement",
            placement_result.configuration,
        )
        placement_model = cluster.simulation_placement_model(selected_placement)
        if _profile is not None:
            system.configure_simulation_placement(**placement_model)
            _notify_progress(args, "placement_calibration_started")
            _profile = calibrate_fixed_placement(
                system,
                result.configuration,
                options=_calibration_options(args),
                workers=args.workers,
                progress_callback=_placement_calibration_progress_callback(args),
            )
            _notify_progress(args, "placement_calibration_completed")
        else:
            _validate_analytical_placement(system, placement_model)
    if _profile is not None and validation is None:
        validation = validate_final_configuration(
            system,
            result.configuration,
            _profile,
            workers=args.workers,
            progress_callback=_progress_callback(args),
        )
        _notify_progress(args, "final_validation_completed")
        if not validation.passed:
            reason = getattr(validation, "failure_reason", None)
            raise RuntimeError(
                "The selected replica configuration failed final validation"
                + (f": {reason}" if reason else ".")
            )
    return {
        "services": _service_recommendations(
            system,
            result.configuration,
            selected_placement,
        )
    }


def _add_output_arguments(parser):
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit one stable JSON result object on stdout.",
    )
    parser.add_argument(
        "--out",
        help="Also write the result envelope to this file (never used by default).",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress progress messages on stderr.",
    )


def _add_workload_argument(parser):
    parser.add_argument(
        "--workload",
        default="workload.yaml",
        help="Workload YAML path (default: workload.yaml).",
    )
    parser.add_argument(
        "--deployment",
        help=(
            "Kubernetes resource facts for a portable measurement workflow. "
            "Requires --measurements."
        ),
    )
    parser.add_argument(
        "--measurements",
        help=(
            "Versioned measurement-manifest YAML. Requires --deployment; "
            "arrival rates are derived from its evidence."
        ),
    )


def _add_calibration_arguments(parser):
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument(
        "--max-replicas",
        type=int,
        default=DEFAULT_MAXIMUM_REPLICAS,
        help="Per-service replica upper bound.",
    )
    advanced = parser.add_argument_group("statistical calibration overrides")
    advanced.add_argument("--warmup-replications", type=int)
    advanced.add_argument("--warmup-validation-replications", type=int)
    advanced.add_argument("--measurement-replications", type=int)
    advanced.add_argument("--minimum-replications", type=int)
    advanced.add_argument("--maximum-replications", type=int)
    advanced.add_argument("--replication-batch-size", type=int)
    advanced.add_argument("--bootstrap-resamples", type=int)
    advanced.add_argument("--final-validation-replications", type=int)


def _add_full_search_arguments(parser):
    parser.add_argument(
        "--replica-search",
        choices=("auto", "exact", "scalable"),
        default="auto",
        help=(
            "Replica-sizing search method. AUTO selects from the bounded "
            "replica search-space size."
        ),
    )
    parser.add_argument(
        "--placement-search",
        choices=("auto", "exact", "scalable"),
        default="auto",
        help=(
            "Node-placement search method. AUTO selects from the placement "
            "search-space size."
        ),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="kubesizer",
        description=(
            "Run calibration, replica sizing, final validation, and optional "
            "node placement. No result files are created unless --out is supplied."
        ),
    )
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {CLI_VERSION}"
    )
    _add_workload_argument(parser)
    parser.add_argument(
        "--resources",
        help=(
            "Cluster resources YAML. Placement is skipped when omitted or "
            "unavailable."
        ),
    )
    parser.add_argument("--skip-placement", action="store_true")
    _add_calibration_arguments(parser)
    parser.add_argument(
        "--replica-evaluation",
        choices=("analytical", "simulation"),
        default="simulation",
        help=(
            "Evidence used to evaluate replica configurations. Percentile SLOs "
            "require simulation. Node placement is always analytical."
        ),
    )
    _add_full_search_arguments(parser)
    _add_output_arguments(parser)
    return parser


def _human_render(result: Any) -> str:
    return yaml.safe_dump(
        _jsonable(result),
        sort_keys=False,
        allow_unicode=True,
    ).rstrip()


def _write_output(envelope: dict[str, Any], args: argparse.Namespace) -> None:
    serialized = json.dumps(_jsonable(envelope), indent=2, sort_keys=True)
    if args.out is not None:
        path = Path(args.out).expanduser().resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(serialized + "\n", encoding="utf-8")
    if args.json:
        print(serialized)
    else:
        print(_human_render(envelope["result"]))


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if getattr(args, "workers", 1) <= 0:
            raise CliUsageError("--workers must be a positive integer.")
        result = _command_run(args)
        envelope = {
            "ok": True,
            "command": "kubesizer",
            "cli_version": CLI_VERSION,
            "result": _jsonable(result),
        }
        _write_output(envelope, args)
        return 0
    except (CliUsageError, ValueError, TypeError, OSError) as error:
        envelope = {
            "ok": False,
            "command": "kubesizer",
            "cli_version": CLI_VERSION,
            "error": {
                "type": type(error).__name__,
                "message": str(error),
            },
        }
        if getattr(args, "json", False):
            print(json.dumps(envelope, indent=2, sort_keys=True))
        else:
            print(f"kubesizer: {error}", file=sys.stderr)
        return 2
    except Exception as error:  # Preserve a stable machine error envelope.
        envelope = {
            "ok": False,
            "command": "kubesizer",
            "cli_version": CLI_VERSION,
            "error": {
                "type": type(error).__name__,
                "message": str(error),
            },
        }
        if getattr(args, "json", False):
            print(json.dumps(envelope, indent=2, sort_keys=True))
        else:
            print(f"kubesizer failed: {error}", file=sys.stderr)
        return 1
    finally:
        reporter = getattr(args, "_progress_reporter", None)
        if reporter is not None:
            reporter.close()


if __name__ == "__main__":
    raise SystemExit(main())

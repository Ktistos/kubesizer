#!/usr/bin/env python3
"""Reduced software integration smoke, not deployment accuracy evidence."""

import argparse
from dataclasses import replace
import json
from pathlib import Path
import sys

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from calibration.profile import CalibrationOptions
from native_backend.backend import PROTOCOL_VERSION, SIMULATOR_SEMANTICS_VERSION
from performance_models import System, QueueingNetworkType
from metrics.replication_rate import minimum_zero_failure_replications
from statistical_workflow import (
    calibrate,
    optimize_with_calibrated_simulation,
    validate_final_configuration,
)


def verify(output, workload_path=None):
    if output.exists() and any(output.iterdir()):
        raise FileExistsError("Use an empty software-check output directory")
    output.mkdir(parents=True, exist_ok=True)
    workload_path = Path(
        workload_path or ROOT / "examples/stateful/staged-overlap.yaml"
    ).resolve()
    raw = yaml.safe_load(workload_path.read_text())
    system = System(
        "staged-software-integration",
        raw,
        QueueingNetworkType.OPEN,
        workload_base_path=workload_path.parent,
    )
    options = replace(
        CalibrationOptions(),
        warmup_replications=12,
        warmup_validation_replications=12,
        measurement_calibration_replications=12,
        target_samples_per_class=100,
        minimum_eligible_samples_per_class=1200,
        minimum_nonempty_replications_per_class=8,
        minimum_optimization_replications=12,
        maximum_optimization_replications=12,
        replication_batch_size=4,
        final_validation_replications=12,
        bootstrap_resample_count=2000,
    )
    failures = system.get_request_class_failure_rate_slos()
    if failures:
        constraints = len(system.get_request_class_latency_slos()) + len(failures)
        needed = max(
            minimum_zero_failure_replications(
                slo, options.overall_alpha / (2 * constraints)
            )
            for slo in failures.values()
        )
        replications = max(12, needed + 1)
        options = replace(
            options,
            minimum_optimization_replications=replications,
            maximum_optimization_replications=replications,
            final_validation_replications=replications,
        )
    profile = calibrate(
        system,
        maximum_replicas=3,
        options=options,
        workers=2,
        report_directory=output / "calibration",
    )
    result = optimize_with_calibrated_simulation(system, profile, workers=2)
    final = validate_final_configuration(
        system,
        result.configuration,
        profile,
        workers=2,
        report_directory=output / "validation",
    )
    evidence = dict(
        scope="synthetic software smoke with reduced calibration settings; no deployment accuracy claim",
        native_protocol=PROTOCOL_VERSION,
        native_semantics=SIMULATOR_SEMANTICS_VERSION,
        workload=str(workload_path),
        static_stability_model=system.has_static_stability_model(),
        component_measurement_provenance=system.get_component_measurement_provenance(),
        configuration=result.configuration,
        maximum_configuration=profile.maximum_replica_configuration,
        final_classification=final.classification.value,
        fresh_final_replications=final.replication_count,
        passed=final.passed,
    )
    (output / "software-check.json").write_text(json.dumps(evidence, indent=2) + "\n")
    return evidence


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--workload", type=Path)
    args = parser.parse_args()
    result = verify(args.output.resolve(), args.workload)
    print(json.dumps(result, indent=2))
    raise SystemExit(0 if result["passed"] else 1)

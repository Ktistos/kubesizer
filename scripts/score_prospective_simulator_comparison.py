#!/usr/bin/env python3
"""Score frozen cross-simulator predictions without changing the population."""

from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path
import random
import statistics
from typing import Any


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as stream:
        return list(csv.DictReader(stream))


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def boolean(value: str, label: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"true", "1", "yes"}:
        return True
    if normalized in {"false", "0", "no"}:
        return False
    raise ValueError(f"{label}: expected a boolean")


def finite(value: str, label: str, *, positive: bool = False) -> float:
    result = float(value)
    if not math.isfinite(result) or (positive and result <= 0):
        raise ValueError(f"{label}: invalid finite value")
    return result


def _quantile(values: list[float], probability: float) -> float:
    ordered = sorted(values)
    index = (len(ordered) - 1) * probability
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (index - lower)


def bootstrap_interval(rows: list[dict[str, Any]], field: str, *, seed: int,
                       resamples: int) -> tuple[float | None, float | None]:
    """Cluster bootstrap family/load groups, retaining replica candidates."""
    groups: dict[tuple[str, str], list[float]] = {}
    for row in rows:
        groups.setdefault((row["family"], row["load_ratio"]), []).append(float(row[field]))
    if not groups:
        return None, None
    keys = sorted(groups)
    generator = random.Random(seed)
    estimates = []
    for _ in range(resamples):
        selected = [generator.choice(keys) for _ in keys]
        values = [value for key in selected for value in groups[key]]
        estimates.append(statistics.fmean(values))
    return _quantile(estimates, 0.025), _quantile(estimates, 0.975)


def score(matrix: Path, observed_path: Path, predictions_path: Path, *,
          bootstrap_resamples: int = 2000, seed: int = 26103002) -> dict[str, Any]:
    cells = read_csv(matrix / "cells.csv")
    decisions = read_csv(matrix / "decisions.csv")
    eligibility_rows = read_csv(matrix / "engine-eligibility.csv")
    observed_rows = read_csv(observed_path)
    prediction_rows = read_csv(predictions_path)

    cell_index = {row["cell_id"]: row for row in cells}
    if len(cell_index) != len(cells):
        raise ValueError("duplicate frozen cell IDs")
    observed_index = {row["cell_id"]: row for row in observed_rows}
    if len(observed_index) != len(observed_rows) or set(observed_index) != set(cell_index):
        raise ValueError("observed cells must exactly match the frozen population")
    eligibility = {
        (row["track"], row["engine"]): row["status"]
        for row in eligibility_rows
        if row["status"] != "ineligible"
    }

    observations: dict[str, dict[str, Any]] = {}
    for cell_id, cell in cell_index.items():
        row = observed_index[cell_id]
        repetitions = int(row["observed_repetitions"])
        expected = int(cell["expected_repetitions"])
        count, completed = int(row["cohort_count"]), int(row["cohort_completed"])
        healthy = boolean(row["health_passed"], f"{cell_id}/health_passed")
        complete = repetitions == expected and count > 0 and completed == count and healthy
        observations[cell_id] = {
            "complete": complete,
            "stable": boolean(row["stable"], f"{cell_id}/stable") and healthy,
            "mean": finite(row["mean_seconds"], f"{cell_id}/mean", positive=True)
            if complete else None,
            "p95": finite(row["p95_seconds"], f"{cell_id}/p95", positive=True)
            if complete and row.get("p95_seconds") else None,
        }

    predictions: dict[tuple[str, str], dict[str, Any]] = {}
    engines = set()
    for row in prediction_rows:
        cell_id, engine = row["cell_id"], row["engine"]
        if cell_id not in cell_index:
            raise ValueError(f"prediction for unknown cell {cell_id}")
        track = cell_index[cell_id]["track"]
        if (track, engine) not in eligibility:
            raise ValueError(f"{engine} is not eligible for {track}")
        if row["information_condition"] != "portable_low_load_measurements_only":
            continue
        key = (engine, cell_id)
        if key in predictions:
            raise ValueError(f"duplicate prediction {key}")
        engines.add(engine)
        status = row["status"]
        if status not in {"predicted", "abstained"}:
            raise ValueError(f"{key}: invalid prediction status")
        available = status == "predicted"
        predictions[key] = {
            "available": available,
            "stable": boolean(row["stable"], f"{key}/stable") if available else False,
            "mean": finite(row["mean_seconds"], f"{key}/mean", positive=True)
            if available else None,
            "p95": finite(row["p95_seconds"], f"{key}/p95", positive=True)
            if available and row.get("p95_seconds") else None,
        }

    accuracy_rows: list[dict[str, Any]] = []
    cell_scores: list[dict[str, Any]] = []
    for track in sorted({row["track"] for row in cells}):
        track_cells = [row for row in cells if row["track"] == track]
        for cell_semantics in sorted({row["semantic_status"] for row in track_cells}):
            semantic_cells = [row for row in track_cells
                              if row["semantic_status"] == cell_semantics]
            for engine in sorted(engine for candidate_track, engine in eligibility
                                 if candidate_track == track):
                latency_eligible = [row for row in semantic_cells
                                    if observations[row["cell_id"]]["complete"]]
                available = []
                false_stable = 0
                stability_correct = 0
                for cell in semantic_cells:
                    cell_id = cell["cell_id"]
                    observed = observations[cell_id]
                    prediction = predictions.get((engine, cell_id), {
                        "available": False, "stable": False, "mean": None, "p95": None})
                    if prediction["available"]:
                        stability_correct += prediction["stable"] == observed["stable"]
                        false_stable += prediction["stable"] and not observed["stable"]
                    if not (observed["complete"] and prediction["available"]):
                        continue
                    signed = 100 * (prediction["mean"] - observed["mean"]) / observed["mean"]
                    scored = {
                        "track": track,
                        "cell_semantic_status": cell_semantics,
                        "family": cell["family"],
                        "load_ratio": cell["load_ratio"],
                        "cell_id": cell_id,
                        "engine": engine,
                        "ape_percent": abs(signed),
                        "signed_error_percent": signed,
                    }
                    if observed["p95"] is not None and prediction["p95"] is not None:
                        scored["p95_ape_percent"] = abs(
                            100 * (prediction["p95"] - observed["p95"]) / observed["p95"])
                    available.append(scored)
                    cell_scores.append(scored)
                low, high = bootstrap_interval(
                    available, "ape_percent", seed=seed, resamples=bootstrap_resamples)
                accuracy_rows.append({
                    "track": track,
                    "cell_semantic_status": cell_semantics,
                    "engine": engine,
                    "engine_eligibility": eligibility[(track, engine)],
                    "eligible_cells": len(latency_eligible),
                    "predicted_cells": len(available),
                    "coverage_percent": (100 * len(available) / len(latency_eligible)
                                         if latency_eligible else None),
                    "mean_ape_percent": (
                        statistics.fmean(row["ape_percent"] for row in available)
                        if available else None),
                    "median_ape_percent": (
                        statistics.median(row["ape_percent"] for row in available)
                        if available else None),
                    "mean_signed_error_percent": (
                        statistics.fmean(row["signed_error_percent"] for row in available)
                        if available else None),
                    "mean_ape_ci95_low": low,
                    "mean_ape_ci95_high": high,
                    "stability_scored_cells": sum(
                        (engine, row["cell_id"]) in predictions for row in semantic_cells),
                    "stability_correct": stability_correct,
                    "false_stable": false_stable,
                })

    decision_scores = []
    for decision in decisions:
        candidates = json.loads(decision["candidate_cell_ids"])
        observed_candidates = [
            cell_id for cell_id in candidates
            if observations[cell_id]["complete"]
            and observations[cell_id]["stable"]
            and observations[cell_id]["mean"] <= float(decision["slo_seconds"])
        ]
        observed_choice = observed_candidates[0] if observed_candidates else None
        for engine in sorted(engine for track, engine in eligibility
                             if track == decision["track"]):
            predicted_candidates = [
                cell_id for cell_id in candidates
                if predictions.get((engine, cell_id), {}).get("available")
                and predictions[(engine, cell_id)]["stable"]
                and predictions[(engine, cell_id)]["mean"] <= float(decision["slo_seconds"])
            ]
            predicted_choice = predicted_candidates[0] if predicted_candidates else None
            predicted_safe = bool(
                predicted_choice
                and observations[predicted_choice]["complete"]
                and observations[predicted_choice]["stable"]
                and observations[predicted_choice]["mean"] <= float(decision["slo_seconds"])
            )
            observed_replicas = (int(cell_index[observed_choice]["replicas"])
                                 if observed_choice else None)
            predicted_replicas = (int(cell_index[predicted_choice]["replicas"])
                                  if predicted_choice else None)
            decision_scores.append({
                "decision_id": decision["decision_id"],
                "track": decision["track"],
                "engine": engine,
                "comparison_status": "replica_routing_capability_sizing",
                "observed_choice": observed_choice,
                "predicted_choice": predicted_choice,
                "false_safe": bool(predicted_choice) and not predicted_safe,
                "replica_overprovisioning": (
                    predicted_replicas - observed_replicas
                    if predicted_replicas is not None and observed_replicas is not None
                    else None),
            })

    return {
        "accuracy": accuracy_rows,
        "cell_scores": cell_scores,
        "decisions": decision_scores,
        "policy": {
            "information_condition": "portable_low_load_measurements_only",
            "bootstrap_resamples": bootstrap_resamples,
            "bootstrap_unit": "family/load cluster",
            "incomplete_cohort": "latency abstention",
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix", type=Path, required=True)
    parser.add_argument("--observed", type=Path, required=True)
    parser.add_argument("--predictions", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    result = score(arguments.matrix, arguments.observed, arguments.predictions)
    arguments.output.mkdir(parents=True, exist_ok=False)
    (arguments.output / "scores.json").write_text(
        json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n")
    write_csv(arguments.output / "accuracy.csv", result["accuracy"],
              list(result["accuracy"][0]))
    write_csv(arguments.output / "decision-scores.csv", result["decisions"],
              list(result["decisions"][0]))
    print(json.dumps(result["policy"], indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

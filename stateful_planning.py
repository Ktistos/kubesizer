"""Compare fully resolved stateful topologies using either existing evaluator.

A topology candidate supplies its own routing, replication work and evidence.
No database is expanded by multiplying an interchangeable-worker capacity.
"""

import hashlib
import math
from dataclasses import dataclass
from pathlib import Path

import yaml

from performance_models import QueueingNetworkType, System


@dataclass(frozen=True)
class StatefulCandidate:
    name: str
    mechanism: str
    system: System
    base_cost: float
    elastic_replica_costs: dict
    sources: dict

    def validate_sources(self):
        for name, expected in self.sources.items():
            if hashlib.sha256(Path(name).read_bytes()).hexdigest() != expected:
                raise ValueError(f"Candidate source changed after loading: {name}")

    def search_costs(self):
        return tuple(
            self.elastic_replica_costs.get(name, 1.0)
            for name in self.system.get_services()
        )

    def total_cost(self, configuration):
        cost = self.base_cost + sum(
            count * self.elastic_replica_costs.get(name, 0.0)
            for name, count in zip(self.system.get_services(), configuration)
        )
        if not math.isfinite(cost):
            raise ValueError("Stateful candidate total cost is not finite.")
        return cost


def _cost(value, label, *, positive=False):
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
    ):
        raise ValueError(f"{label} must be finite numeric cost.")
    if value < 0 or (positive and value == 0):
        raise ValueError(f"{label} has an invalid cost.")
    return float(value)


def _read(path):
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a mapping.")
    return value


def _context(system):
    """Compare external demand/SLOs while allowing different internal layouts."""
    from native_backend.backend import _serialize_distribution

    value = system.get_workload_spec()
    flow_fields = {
        "calls",
        "call_execution",
        "call_policies",
        "routing",
        "visits",
        "workflow",
    }
    return {
        "classes": {
            name: {key: item for key, item in spec.items() if key not in flow_fields}
            for name, spec in value["request_classes"].items()
        },
        "arrivals": {
            name: {
                entry: {
                    key: item
                    for key, item in _serialize_distribution(spec).items()
                    if key != "library"
                }
                for entry, spec in streams.items()
            }
            for name, streams in system.get_external_arrival_distribution_specs().items()
        },
    }


def load_stateful_candidates(path):
    """Read a finite design space; all alternatives are resolved before scoring."""
    path = Path(path).expanduser().resolve()
    manifest = _read(path)
    if (
        set(manifest) != {"version", "candidates"}
        or type(manifest["version"]) is not int
        or manifest["version"] != 1
    ):
        raise ValueError("Stateful candidate set needs version: 1 and candidates.")
    rows = manifest["candidates"]
    if not isinstance(rows, list) or not rows:
        raise ValueError("Stateful candidates must be a nonempty list.")
    result, names, context = [], set(), None
    for row in rows:
        if not isinstance(row, dict) or set(row) != {
            "name",
            "mechanism",
            "workload",
            "base_cost",
            "elastic_replica_costs",
            "evidence_sources",
        }:
            raise ValueError(
                "Each candidate needs name, mechanism, workload, base_cost, elastic_replica_costs and evidence_sources."
            )
        name = row["name"]
        if not isinstance(name, str) or not name.strip() or name in names:
            raise ValueError("Stateful candidate names must be nonempty and unique.")
        names.add(name)
        if not isinstance(row["mechanism"], str) or not row["mechanism"].strip():
            raise ValueError("mechanism must describe the resolved scaling mechanism.")
        if not isinstance(row["workload"], str) or not row["workload"]:
            raise ValueError("Candidate workload must be a file path.")
        workload_path = (path.parent / row["workload"]).resolve()
        workload = _read(workload_path)
        system = System(
            name,
            workload,
            QueueingNetworkType.OPEN,
            workload_base_path=workload_path.parent,
        )
        costs = row["elastic_replica_costs"]
        if not isinstance(costs, dict) or set(costs) - set(system.get_services()):
            raise ValueError("elastic_replica_costs must reference known services.")
        costs = {key: _cost(value, key, positive=True) for key, value in costs.items()}
        constraints = system.get_replica_constraints()
        for service in system.get_services():
            if service not in costs and (
                service not in constraints
                or constraints[service][0] != constraints[service][1]
            ):
                raise ValueError(
                    f"Candidate {name}: nonelastic resource {service} must have fixed replica bounds."
                )
        evidence = row["evidence_sources"]
        if (
            not isinstance(evidence, list)
            or not evidence
            or any(not isinstance(item, str) or not item for item in evidence)
        ):
            raise ValueError(
                "Each candidate needs nonempty evidence_sources file paths."
            )
        sources = {
            str(source): hashlib.sha256(source.read_bytes()).hexdigest()
            for source in [
                workload_path,
                *((path.parent / item).resolve() for item in evidence),
            ]
        }
        candidate_context = _context(system)
        if context is not None and candidate_context != context:
            raise ValueError(
                "Candidates must have identical external arrival streams, request classes, latency boundaries and SLOs."
            )
        context = candidate_context
        result.append(
            StatefulCandidate(
                name,
                row["mechanism"],
                system,
                _cost(row["base_cost"], "base_cost"),
                costs,
                sources,
            )
        )
    return tuple(result)


def compare_stateful_candidates(candidates, evaluate):
    """Evaluate every alternative; a failed candidate cannot be selected.

    ``evaluate`` runs analytical sizing or calibrated simulation + fresh final
    validation, and returns an optimization result. Unsupported candidates
    remain visible as unresolved. Minimum-cost scope is the declared set only.
    """
    from calibration.profile import experiment_fingerprints

    outcomes, passed = [], []
    for candidate in candidates:
        row = {
            "name": candidate.name,
            "mechanism": candidate.mechanism,
            "sources": candidate.sources,
            "fingerprints": experiment_fingerprints(candidate.system),
        }
        try:
            candidate.validate_sources()
            result = evaluate(candidate)
            candidate.validate_sources()
            if not getattr(result.metrics, "feasible", True):
                raise ValueError(
                    "Candidate evaluation did not establish model feasibility."
                )
            configuration = tuple(result.configuration)
            candidate.system.configure_system_replicas(configuration)
            cost = candidate.total_cost(configuration)
            row.update(
                status="MODEL_ACCEPTED",
                configuration=dict(zip(candidate.system.get_services(), configuration)),
                total_cost=cost,
                evaluation_basis=getattr(
                    result.metrics,
                    "evaluation_basis",
                    "CALIBRATED_SIMULATION_WITH_FINAL_VALIDATION",
                ),
                within_candidate_minimum_proven=bool(
                    getattr(result, "minimality_proven", False)
                ),
                mean_latency_upper_bounds=getattr(result.metrics, "upper_bounds", {}),
                mean_latency_lower_bounds=getattr(result.metrics, "lower_bounds", {}),
            )
            passed.append((cost, candidate.name, row))
        except (ValueError, RuntimeError, ArithmeticError, OSError) as error:
            row.update(status="UNRESOLVED", reason=str(error))
        outcomes.append(row)
    if not passed:
        return {
            "status": "ABSTAINED",
            "selected": None,
            "candidates": outcomes,
            "minimum_cost_proven": False,
        }
    _cost_value, name, selected = min(passed, key=lambda value: (value[0], value[1]))
    return {
        "status": "MODEL_ACCEPTED",
        "selected": name,
        "configuration": selected["configuration"],
        "total_cost": selected["total_cost"],
        "candidates": outcomes,
        "minimum_cost_proven": len(passed) == len(outcomes)
        and all(row["within_candidate_minimum_proven"] for row in outcomes),
        "scope": "declared topology candidates and supplied performance models; not empirical deployment certification",
    }

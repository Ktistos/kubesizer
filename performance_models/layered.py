"""Experimental moment decomposition for acyclic synchronous service calls.

This is not an exact LQN solver. CPU and admission contention are decomposed;
call-count and duration dependencies are explicit approximation assumptions.
Nothing in this module uses target latency observations or random sampling.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from graphlib import CycleError, TopologicalSorter

import numpy as np

from metrics import Metrics
from .analytical import (
    mg1_mean_response_time,
    variability_adjusted_mean_response_time,
)


@dataclass(frozen=True)
class Moments:
    mean: float
    variance: float = 0.0

    @property
    def scv(self):
        return self.variance / self.mean**2 if self.mean > 0.0 else 0.0


def _sum(parts):
    """Independent-duration approximation, not an inferred covariance."""
    return Moments(sum(p.mean for p in parts), sum(p.variance for p in parts))


def _repeat(part, count):
    # Mean counts do not determine count variance. Freeze the minimal-variance
    # floor/ceil count model, rather than fitting a count law to target outcomes.
    fraction = count - math.floor(count)
    return Moments(
        count * part.mean,
        count * part.variance + fraction * (1.0 - fraction) * part.mean**2,
    )


def independent_join_moments(parts):
    """Approximate E[max] by independent moment-matched lognormal CDFs.

    Deterministic branches are handled exactly. Integration is deterministic;
    no trace replay or Monte Carlo is used. Unknown branch dependence remains
    a limitation, and max(means)..sum(means) is recorded separately.
    """
    parts = tuple(parts)
    if not parts:
        return Moments(0.0)
    if len(parts) == 1:
        return parts[0]
    if any(not math.isfinite(p.mean + p.variance) for p in parts):
        return Moments(math.inf, math.inf)
    floor = max((p.mean for p in parts if p.variance == 0.0), default=0.0)
    random = tuple(p for p in parts if p.variance > 0.0 and p.mean > 0.0)
    if not random:
        return Moments(floor)
    parameters = []
    for part in random:
        sigma = math.sqrt(math.log1p(part.scv))
        parameters.append((math.log(part.mean) - sigma**2 / 2.0, sigma))
    low = math.log(floor) if floor else min(mu - 10 * s for mu, s in parameters)
    high = max([low] + [mu + 10 * s for mu, s in parameters])
    if high > 350:
        raise ValueError("Fork-join moments exceed the numerical integration range.")
    grid = np.linspace(low, high, 2049)
    cdf = np.ones_like(grid)
    for mu, sigma in parameters:
        z = (grid - mu) / (sigma * math.sqrt(2.0))
        cdf *= np.array([0.5 * math.erfc(-float(x)) for x in z])
    survival = 1.0 - cdf
    lower_endpoint = math.exp(low)
    mean = lower_endpoint + float(np.trapezoid(survival * np.exp(grid), grid))
    second = lower_endpoint**2 + float(
        np.trapezoid(2 * survival * np.exp(2 * grid), grid)
    )
    mean = min(sum(p.mean for p in parts), max(max(p.mean for p in parts), mean))
    return Moments(mean, max(0.0, second - mean**2))


def _queue_wait(rate, duration, servers, arrival_scv):
    if rate == 0.0 or duration.mean == 0.0:
        return 0.0
    if not math.isfinite(duration.mean) or rate * duration.mean >= servers:
        return math.inf
    if servers == 1 and math.isclose(arrival_scv, 1.0, abs_tol=1e-12):
        response = mg1_mean_response_time(
            rate, 1 / duration.mean, service_scv=duration.scv
        )
    else:
        response = variability_adjusted_mean_response_time(
            rate,
            1 / duration.mean,
            servers,
            arrival_scv=arrival_scv,
            service_scv=duration.scv,
        )
    return max(0.0, response - duration.mean)


def _mixture(parts):
    total = sum(rate for rate, _ in parts)
    if total == 0.0:
        return Moments(0.0)
    active = [(r, p) for r, p in parts if r > 0.0]
    if any(not math.isfinite(p.mean + p.variance) for _, p in active):
        return Moments(math.inf, math.inf)
    mean = sum(r * p.mean for r, p in active) / total
    second = sum(r * (p.variance + p.mean**2) for r, p in active) / total
    return Moments(mean, max(0.0, second - mean**2))


class LayeredAnalyticalSystem:
    """Opt-in adapter using existing workload inputs and exact replica search.

    The pool-delay correction is W(holding demand) - W(CPU-only demand), clipped
    at zero. It preserves CPU-only limits and avoids counting the same FCFS
    queue twice. CPU waiting inside admission and downstream correlations are
    not solved jointly: all enriched results must be labelled approximate.
    """

    def __init__(self, system, *, join_strategy="moment_match"):
        if join_strategy not in {"moment_match", "sum_sensitivity"}:
            raise ValueError("Unknown fork-join strategy.")
        self.system = system
        self.join_strategy = join_strategy
        self._cached = None
        self._validate()

    def __getattr__(self, name):
        return getattr(self.system, name)

    def _validate(self):
        graphs = self.system.get_request_class_call_graphs()
        for name in self.system.get_request_classes():
            if name not in graphs:
                # A single entry with no internal visits needs no call graph.
                rates = self.system.get_request_class_effective_arrival_rates()[name]
                external = self.system.get_configured_external_arrival_rates()[name]
                if any(abs(r - e) > 1e-9 for r, e in zip(rates, external)):
                    raise ValueError(
                        "Layered analysis requires an explicit synchronous calls graph."
                    )
            if (
                self.system.get_request_class_call_execution_modes().get(name)
                == "parallel"
            ):
                for targets in graphs.get(name, {}).values():
                    if any(c != int(c) for c in targets.values()):
                        raise ValueError(
                            "Parallel fractional call counts do not identify fork membership."
                        )
                    if sum(targets.values()) > 128:
                        raise ValueError(
                            "Parallel fan-out exceeds the explicit 128-branch limit."
                        )
        dependencies = {s: set() for s in self.system.get_services()}
        for graph in graphs.values():
            for source, targets in graph.items():
                dependencies[source].update(
                    t for t, count in targets.items() if count > 0
                )
        try:
            self._order = tuple(TopologicalSorter(dependencies).static_order())
        except CycleError as error:
            raise ValueError(
                "Layered analysis requires an acyclic union of call graphs."
            ) from error

    def get_strict_analytical_incompatibilities(self):
        self._validate()
        return ()

    def get_analytical_evaluation_basis(self):
        return "EXPERIMENTAL_LAYERED_MOMENT_APPROXIMATION"

    def configure_system_replicas(self, configuration):
        self.system.configure_system_replicas(configuration)
        self._cached = None

    def _compute(self):
        key = tuple(self.system.get_system_replicas())
        if self._cached is not None and self._cached[0] == key:
            return self._cached[1:]
        classes = self.system.get_request_classes()
        rates = self.system.get_request_class_effective_arrival_rates()
        cpu = self.system.get_request_class_service_time_distribution_specs()
        blocking = (
            self.system.get_request_class_service_blocking_time_distribution_specs()
        )
        graphs = self.system.get_request_class_call_graphs()
        modes = self.system.get_request_class_call_execution_modes()
        arrival_scvs = self.system.get_effective_arrival_scvs()
        limits = self.system.get_service_concurrency_limits_per_replica()
        slots = self.system.get_service_processing_slots_per_replica()
        dispatch = self.system.get_service_replica_queue_models()
        execution = self.system.get_service_execution_models()
        comm = self.system.get_communication_latency_distribution_spec()
        if comm is None:
            round_trip = Moments(0.0)
        else:
            p = comm.cross_node_probability
            choices = [
                (
                    1 - p,
                    Moments(
                        comm.same_node_one_way.mean,
                        comm.same_node_one_way.scv * comm.same_node_one_way.mean**2,
                    ),
                ),
                (
                    p,
                    Moments(
                        comm.cross_node_one_way.mean,
                        comm.cross_node_one_way.scv * comm.cross_node_one_way.mean**2,
                    ),
                ),
            ]
            one_way = _mixture(choices)
            round_trip = Moments(2 * one_way.mean, 2 * one_way.variance)
        responses, diagnostics, joins = {}, {}, {}
        cpu_utilizations, local_means = [0.0] * len(key), [0.0] * len(key)
        for service_name in self._order:
            service = self.system.get_service(service_name)
            sid = service.get_id()
            total_rate = sum(rates[c][sid] for c in classes)
            active = [c for c in classes if rates[c][sid] > 0.0]
            if not active:
                diagnostics[service_name] = {
                    "cpu_utilization": 0.0,
                    "holding_utilization": 0.0,
                }
                continue
            replicas = key[sid]
            independent = dispatch[service_name] != "pooled"
            rate = total_rate / replicas if independent else total_rate
            arrival_scv = arrival_scvs[sid]
            if dispatch[service_name] == "independent_random":
                arrival_scv = 1 + (arrival_scv - 1) / replicas
            elif dispatch[service_name] == "independent_round_robin":
                arrival_scv /= replicas
            cpu_moments = {
                c: Moments(
                    cpu[c][service_name].mean,
                    cpu[c][service_name].scv * cpu[c][service_name].mean ** 2,
                )
                for c in active
            }
            mixture = _mixture([(rates[c][sid], cpu_moments[c]) for c in active])
            servers = slots[service_name] * (1 if independent else replicas)
            rho = rate * mixture.mean / servers
            cpu_utilizations[sid] = rho
            if execution[service_name] == "processor_sharing":
                # M/G/1-PS insensitivity for Poisson input; GI input is a
                # frozen SCV-factor approximation, not an exact PS formula.
                inflation = (
                    math.inf if rho >= 1 else max(0.0, arrival_scv) * rho / (1 - rho)
                )
                waits = {c: cpu_moments[c].mean * inflation for c in active}
            else:
                wait = _queue_wait(rate, mixture, servers, arrival_scv)
                waits = {c: wait for c in active}
            holds, children = {}, {}
            for c in active:
                child_parts = []
                targets = graphs.get(c, {}).get(service_name, {})
                for target, count in targets.items():
                    if count <= 0.0:
                        continue
                    duration = _sum((responses[c, target], round_trip))
                    if modes.get(c) == "parallel":
                        child_parts.extend([duration] * int(count))
                    else:
                        child_parts.append(_repeat(duration, count))
                if modes.get(c) == "parallel" and len(child_parts) > 1:
                    joins[c, service_name] = (
                        max(p.mean for p in child_parts),
                        sum(p.mean for p in child_parts),
                    )
                    child = (
                        independent_join_moments(child_parts)
                        if self.join_strategy == "moment_match"
                        else _sum(child_parts)
                    )
                else:
                    child = _sum(child_parts)
                children[c] = child
                block = blocking.get(c, {}).get(service_name)
                block_moments = (
                    Moments(block.mean, block.scv * block.mean**2)
                    if block
                    else Moments(0.0)
                )
                holds[c] = _sum((cpu_moments[c], block_moments, child))
            holding_mixture = _mixture([(rates[c][sid], holds[c]) for c in active])
            capacity = limits[service_name]
            holding_rho, correction = 0.0, 0.0
            if capacity is not None:
                pool_servers = capacity * (1 if independent else replicas)
                holding_rho = rate * holding_mixture.mean / pool_servers
                pool_wait = _queue_wait(
                    rate, holding_mixture, pool_servers, arrival_scv
                )
                cpu_only_pool_wait = _queue_wait(
                    rate, mixture, pool_servers, arrival_scv
                )
                correction = (
                    math.inf
                    if not math.isfinite(pool_wait)
                    else max(0.0, pool_wait - cpu_only_pool_wait)
                )
            for c in active:
                wait = waits[c] + correction
                responses[c, service_name] = _sum((holds[c], Moments(wait, wait**2)))
            local_means[sid] = (
                holding_mixture.mean
                + sum(rates[c][sid] * waits[c] for c in active) / total_rate
                + correction
            )
            diagnostics[service_name] = {
                "cpu_utilization": rho,
                "holding_utilization": holding_rho,
                "holding_mean_seconds": holding_mixture.mean,
                "admission_wait_correction_seconds": correction,
                "cpu_wait_mean_seconds": sum(rates[c][sid] * waits[c] for c in active)
                / total_rate,
                "queue_model": dispatch[service_name],
            }
        means = {}
        overheads = self.system.get_request_class_fixed_latency_overheads()
        for c in classes:
            entries = [
                (service.get_gamma(c), responses[c, service.get_name()])
                for service in self.system.get_services().values()
                if service.get_gamma(c) > 0
            ]
            means[c] = _mixture(entries).mean + overheads.get(c, 0.0)
        stable = all(math.isfinite(m) for m in means.values()) and all(
            d["cpu_utilization"] < 1 and d["holding_utilization"] < 1
            for d in diagnostics.values()
        )
        metrics = Metrics(cpu_utilizations, local_means, stable)
        details = {
            "evaluation_basis": "EXPERIMENTAL_LAYERED_MOMENT_APPROXIMATION",
            "services": diagnostics,
            "fork_join_branch_mean_ranges": {
                f"{c}/{s}": list(bounds) for (c, s), bounds in joins.items()
            },
            "join_strategy": self.join_strategy,
            "assumptions": [
                "CPU/admission decomposition, not a joint queue solution",
                "independent durations and floor/ceil sequential call counts",
                "moment-matched lognormal independent fork branches",
                "CPU queue residence excluded from admitted holding demand",
                "baseline approximate network arrival SCV propagation",
            ],
        }
        self._cached = (key, metrics, means, details)
        return metrics, means, details

    def solve(self):
        return self._compute()[0]

    def get_request_class_mean_latencies(self):
        return dict(self._compute()[1])

    def get_layered_model_details(self):
        return self._compute()[2]

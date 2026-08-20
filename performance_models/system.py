import copy
import math
from enum import Enum
from .service import Service
from .workload import (
    DistributionSpec,
    SINGLE_TRAFFIC_MODEL_NAME,
    normalize_workload_spec,
    parse_workload_spec,
)
from .analytical import (
    AnalyticalQueueingModel,
    approximate_departure_scv,
    combine_independent_stream_scvs,
    mg1_mean_response_time,
    mmc_mean_response_time,
    select_queueing_model,
    thinned_stream_scv,
    variability_adjusted_mean_response_time,
)
from metrics import Metrics
from native_backend import get_native_backend

import random

import numpy as np


class QueueingNetworkType(str, Enum):
    OPEN = "open"
    CLOSED = "closed"
    HYBRID = "hybrid"


def _rescale_interarrival_spec(specification, target_rate):
    """Preserve distribution shape while changing its mean arrival rate."""
    target_rate = float(target_rate)
    if target_rate <= 0.0:
        raise ValueError("An external arrival distribution needs a positive rate.")
    if math.isclose(
        specification.rate,
        target_rate,
        rel_tol=1e-12,
        abs_tol=0.0,
    ):
        return specification

    target_mean = 1.0 / target_rate
    time_scale = target_mean / specification.mean
    parameters = dict(specification.parameters)
    if "rate" in parameters:
        parameters["rate"] = target_rate
    if specification.distribution == "arrival_trace":
        parameters["block_duration"] = (
            float(parameters["block_duration"]) * time_scale
        )
    samples = specification.samples
    if specification.distribution in {
        "empirical_interarrival",
        "arrival_trace",
    }:
        samples = tuple(value * time_scale for value in samples)
    return DistributionSpec(
        distribution=specification.distribution,
        mean=target_mean,
        scv=specification.scv,
        parameters=tuple(sorted(parameters.items())),
        samples=samples,
    )


class System:

    def __init__(
        self,
        name,
        workload,
        type: QueueingNetworkType,
        *,
        workload_base_path=None,
    ):
        self.__name = name
        structured = isinstance(workload, dict) and "traffic_model" in workload
        self.__traffic_model_name = SINGLE_TRAFFIC_MODEL_NAME if structured else None
        self.__workload_spec = copy.deepcopy(workload)
        self.__workload_base_path = workload_base_path
        self.__traffic_distribution_spec = None
        self.__request_class_latency_slos = {}
        if structured:
            parsed = parse_workload_spec(
                workload,
                base_path=workload_base_path,
            )
            self.__traffic_distribution_spec = parsed.traffic_model
            self.__request_class_latency_slos = dict(
                parsed.request_class_latency_slos
            )
        workload = normalize_workload_spec(
            workload,
            base_path=workload_base_path,
        )
        # type refers to either open or closed system
        self.__type = type
        self.__services = {}
        self.__external_traffic_services = []

        if not isinstance(workload, dict) or not workload:
            raise ValueError("At least one request class is required.")
        self.__request_classes = tuple(workload.keys())

        class_services = {}
        configured_rates = {}
        configured_totals = {}
        explicit_weights = {}
        wrapped_classes = []
        for request_class_name, request_class_spec in workload.items():
            if not isinstance(request_class_name, str) or not request_class_name:
                raise ValueError("Request-class names must be non-empty strings.")
            if not isinstance(request_class_spec, dict):
                raise ValueError(
                    f"Request class '{request_class_name}' must be a mapping."
                )
            wrapped = (
                "services" in request_class_spec
                or "concurrency_weight" in request_class_spec
            )
            wrapped_classes.append(wrapped)
            if wrapped:
                if set(request_class_spec) != {"concurrency_weight", "services"}:
                    raise ValueError(
                        f"Weighted request class '{request_class_name}' must contain "
                        "only concurrency_weight and services."
                    )
                weight = request_class_spec["concurrency_weight"]
                if (
                    isinstance(weight, bool)
                    or not isinstance(weight, (int, float))
                    or not math.isfinite(float(weight))
                    or float(weight) <= 0
                ):
                    raise ValueError(
                        "Every concurrency_weight must be finite and greater than zero."
                    )
                explicit_weights[request_class_name] = float(weight)
                services_spec = request_class_spec["services"]
            else:
                services_spec = request_class_spec
            if not isinstance(services_spec, dict) or not services_spec:
                raise ValueError(
                    f"Request class '{request_class_name}' needs a nonempty services mapping."
                )
            class_services[request_class_name] = services_spec

            rates = {}
            for service_name, service_data in services_spec.items():
                if not isinstance(service_data, dict):
                    raise ValueError(
                        f"Service '{service_name}' in class '{request_class_name}' "
                        "must be a mapping."
                    )
                gamma = service_data.get("gamma", 0.0)
                if (
                    isinstance(gamma, bool)
                    or not isinstance(gamma, (int, float))
                    or not math.isfinite(float(gamma))
                    or float(gamma) < 0
                ):
                    raise ValueError(
                        "External arrival rates must be finite and non-negative."
                    )
                rates[service_name] = float(gamma)
            configured_rates[request_class_name] = rates
            configured_totals[request_class_name] = sum(rates.values())

        if any(wrapped_classes) and not all(wrapped_classes):
            raise ValueError(
                "Do not mix weighted and legacy request-class schemas in one workload."
            )

        total_configured_rate = sum(configured_totals.values())
        if all(wrapped_classes):
            if not math.isclose(
                sum(explicit_weights.values()),
                1.0,
                rel_tol=0.0,
                abs_tol=1e-12,
            ):
                raise ValueError("Request-class concurrency weights must sum to one.")
            for request_class_name in self.__request_classes:
                if configured_totals[request_class_name] <= 0:
                    raise ValueError(
                        f"Weighted request class '{request_class_name}' needs at "
                        "least one positive external gamma."
                    )
            concurrency_weights = explicit_weights
        elif total_configured_rate > 0:
            # Legacy workloads retain their exact arrival rates. Their implicit
            # weights are simply their current shares of total external traffic.
            concurrency_weights = {
                request_class_name: (
                    configured_totals[request_class_name] / total_configured_rate
                )
                for request_class_name in self.__request_classes
            }
        else:
            equal_weight = 1.0 / len(self.__request_classes)
            concurrency_weights = {
                request_class_name: equal_weight
                for request_class_name in self.__request_classes
            }

        self.__request_class_concurrency_weights = dict(concurrency_weights)
        self.__configured_external_arrival_rates = copy.deepcopy(configured_rates)

        effective_rates = {}
        for request_class_name in self.__request_classes:
            configured_class_total = configured_totals[request_class_name]
            if total_configured_rate <= 0 or configured_class_total <= 0:
                effective_rates[request_class_name] = {
                    service_name: 0.0
                    for service_name in configured_rates[request_class_name]
                }
                continue
            target_class_rate = (
                total_configured_rate * concurrency_weights[request_class_name]
            )
            effective_rates[request_class_name] = {
                service_name: (
                    target_class_rate * configured_rate / configured_class_total
                )
                for service_name, configured_rate in configured_rates[
                    request_class_name
                ].items()
            }

        # Build the union of services before registering routes, so targets can
        # be resolved regardless of declaration order or request class.
        for request_class in class_services.values():
            for service_name, service_data in request_class.items():
                service = self.get_service(service_name)
                service_mu = service_data.get("mu", 0)
                if service is None:
                    service = Service(
                        self,
                        service_name,
                        mu=service_mu,
                        service_id=len(self.__services),
                    )
                    self.add_service(service_name, service)
                elif service.get_mu() != service_mu:
                    raise ValueError(
                        f"Service '{service_name}' must use the same mu in "
                        "every request class."
                    )

        for request_class_name, request_class in class_services.items():
            for service_name, service_data in request_class.items():
                service = self.get_service(service_name)
                service.add_gamma(
                    request_class_name,
                    effective_rates[request_class_name][service_name],
                )
                service.register_edges(
                    request_class_name,
                    service_data.get("routes", {}),
                )

        self.__external_traffic_services = [
            service
            for service in self.get_services().values()
            if any(
                service.get_gamma(request_class) > 0
                for request_class in self.get_request_classes()
            )
        ]
        self.__initialize_distribution_metadata()
        self.__poisson_arrival_flags = self.__calculate_poisson_arrival_flags()

    def __initialize_distribution_metadata(self):
        if self.__traffic_distribution_spec is None:
            self.__service_time_specs = {
                service.get_name(): DistributionSpec(
                    distribution="exponential",
                    mean=(
                        1.0 / service.get_mu()
                        if service.get_mu() > 0.0
                        else math.inf
                    ),
                    scv=1.0,
                    parameters=(
                        ("distribution", "exponential"),
                        ("rate", service.get_mu()),
                    ),
                )
                for service in self.get_services().values()
            }
            self.__entry_arrival_specs = {
                request_class: {
                    service.get_name(): DistributionSpec(
                        distribution="exponential",
                        mean=1.0 / service.get_gamma(request_class),
                        scv=1.0,
                        parameters=(
                            ("distribution", "exponential"),
                            ("rate", service.get_gamma(request_class)),
                        ),
                    )
                    for service in self.get_services().values()
                    if service.get_gamma(request_class) > 0.0
                }
                for request_class in self.get_request_classes()
            }
        else:
            traffic_model = self.__traffic_distribution_spec
            self.__service_time_specs = dict(traffic_model.service_times)
            self.__entry_arrival_specs = {
                request_class: {
                    service_name: _rescale_interarrival_spec(
                        process,
                        self.get_service(service_name).get_gamma(request_class),
                    )
                    for service_name, process in traffic_model.entry_services[
                        request_class
                    ].items()
                }
                for request_class in self.get_request_classes()
            }

        self.__service_time_distributions = {
            service_name: process.distribution
            for service_name, process in self.__service_time_specs.items()
        }
        self.__service_time_scvs = {
            service_name: process.scv
            for service_name, process in self.__service_time_specs.items()
        }
        self.__entry_arrival_distributions = {
            request_class: {
                service_name: process.distribution
                for service_name, process in self.__entry_arrival_specs[
                    request_class
                ].items()
            }
            for request_class in self.get_request_classes()
        }
        self.__entry_arrival_scvs = {
            request_class: {
                service_name: process.scv
                for service_name, process in self.__entry_arrival_specs[
                    request_class
                ].items()
            }
            for request_class in self.get_request_classes()
        }

    def __calculate_poisson_arrival_flags(self):
        service_count = len(self.get_services())
        flags = [True] * service_count
        reachable_by_class = {}
        for request_class in self.get_request_classes():
            reachable = {
                service.get_id()
                for service in self.get_services().values()
                if service.get_gamma(request_class) > 0.0
            }
            pending = list(reachable)
            while pending:
                source_id = pending.pop()
                source = self.get_service_by_id(source_id)
                for edge in source.get_adjacency_list(request_class):
                    target = edge.get_target()
                    if target is None or edge.get_routing_prob() <= 0.0:
                        continue
                    target_id = target.get_id()
                    if target_id not in reachable:
                        reachable.add(target_id)
                        pending.append(target_id)
            reachable_by_class[request_class] = reachable
            for service in self.get_services().values():
                if (
                    service.get_gamma(request_class) > 0.0
                    and self.__entry_arrival_distributions[request_class].get(
                        service.get_name(), "exponential"
                    )
                    != "exponential"
                ):
                    flags[service.get_id()] = False

        for _ in range(service_count):
            changed = False
            for request_class, reachable in reachable_by_class.items():
                for source in self.get_services().values():
                    source_id = source.get_id()
                    if source_id not in reachable:
                        continue
                    poisson_departure = (
                        flags[source_id]
                        and self.__service_time_distributions[source.get_name()]
                        == "exponential"
                    )
                    if poisson_departure:
                        continue
                    for edge in source.get_adjacency_list(request_class):
                        target = edge.get_target()
                        if target is None or edge.get_routing_prob() <= 0.0:
                            continue
                        target_id = target.get_id()
                        if flags[target_id]:
                            flags[target_id] = False
                            changed = True
            if not changed:
                return tuple(flags)
        return tuple(flags)

    def __current_queueing_models(self):
        models = [None] * len(self.get_services())
        for service in self.get_services().values():
            service_id = service.get_id()
            models[service_id] = select_queueing_model(
                poisson_arrivals=self.__poisson_arrival_flags[service_id],
                exponential_service=(
                    self.__service_time_distributions[service.get_name()]
                    == "exponential"
                ),
                replicas=service.get_replicas(),
            )
        return tuple(models)

    def __calculate_effective_arrival_scvs(
        self,
        request_class_rates,
        utilizations,
        replicas,
    ):
        service_count = len(self.get_services())
        service_scvs = [0.0] * service_count
        for service in self.get_services().values():
            service_scvs[service.get_id()] = self.__service_time_scvs[
                service.get_name()
            ]

        external_components = [[] for _ in range(service_count)]
        routed_components = []
        for request_class, class_rates in request_class_rates.items():
            for service in self.get_services().values():
                service_id = service.get_id()
                external_rate = service.get_gamma(request_class)
                if external_rate > 0.0:
                    external_components[service_id].append(
                        (
                            external_rate,
                            self.__entry_arrival_scvs[request_class].get(
                                service.get_name(), 1.0
                            ),
                        )
                    )
                source_rate = float(class_rates[service_id])
                if source_rate <= 0.0:
                    continue
                for edge in service.get_adjacency_list(request_class):
                    target = edge.get_target()
                    probability = edge.get_routing_prob()
                    if target is None or probability <= 0.0:
                        continue
                    routed_components.append(
                        (
                            service_id,
                            target.get_id(),
                            source_rate * probability,
                            probability,
                        )
                    )

        arrival_scvs = [1.0] * service_count
        for _ in range(200):
            departure_scvs = [
                approximate_departure_scv(
                    arrival_scvs[service_id],
                    service_scvs[service_id],
                    float(utilizations[service_id]),
                    int(replicas[service_id]),
                )
                for service_id in range(service_count)
            ]
            components = [list(values) for values in external_components]
            for source_id, target_id, flow_rate, probability in routed_components:
                components[target_id].append(
                    (
                        flow_rate,
                        thinned_stream_scv(
                            departure_scvs[source_id], probability
                        ),
                    )
                )
            raw = [
                (
                    1.0
                    if self.__poisson_arrival_flags[service_id]
                    else combine_independent_stream_scvs(
                        components[service_id]
                    )
                )
                for service_id in range(service_count)
            ]
            updated = [
                0.5 * previous + 0.5 * candidate
                for previous, candidate in zip(arrival_scvs, raw)
            ]
            if max(
                abs(candidate - previous)
                for candidate, previous in zip(updated, arrival_scvs)
            ) <= 1e-10:
                return updated
            arrival_scvs = updated
        raise ValueError(
            "Analytical arrival-variability propagation did not converge."
        )

    def __initialize_system_input(self, request_class_name):
        num_of_services = len(self.get_services())
        gammas = [0.0 for _ in range(num_of_services)]
        mus = [0.0 for _ in range(num_of_services)]
        replicas = [0 for _ in range(num_of_services)]

        routing_probability_matrix = [
            [0 for _ in range(num_of_services)] for _ in range(num_of_services)
        ]

        for service_name in self.get_services():
            service = self.get_service(service_name)
            order_index = service.get_id()
            service_replicas = service.get_replicas()
            service_gamma = service.get_gamma(request_class_name)

            service_mu = service.get_mu()
            gammas[order_index] = service_gamma
            mus[order_index] = service_mu
            replicas[order_index] = service_replicas

            adjacency_list = service.get_adjacency_list(request_class_name)

            for edge in adjacency_list:
                target_service = edge.get_target()
                routing_probability = edge.get_routing_prob()

                if target_service is not None:
                    outgoing_service_index = target_service.get_id()

                    routing_probability_matrix[order_index][
                        outgoing_service_index
                    ] = routing_probability

        return {
            "gammas": gammas,
            "mus": mus,
            "replicas": replicas,
            "routing_probability_matrix": routing_probability_matrix,
        }

    def __is_stable(self, utilizations):
        for utilization in utilizations:
            if utilization >= 1.0:
                return False
        return True

    def __calculate_effective_arrival_rates(self, system_input):
        num_of_services = len(self.get_services())
        gammas = np.array(system_input["gammas"], dtype=float)
        routing_probability_matrix = np.array(
            system_input["routing_probability_matrix"], dtype=float
        )
        identity_matrix = np.eye(num_of_services)
        return np.linalg.solve(
            identity_matrix - routing_probability_matrix.T,
            gammas,
        )

    def __calculate_request_class_arrival_rates(self):
        return {
            request_class: self.__calculate_effective_arrival_rates(
                self.__initialize_system_input(request_class)
            )
            for request_class in self.get_request_classes()
        }

    def __calculate_aggregate_arrival_rates(self, request_class_rates):
        aggregate = np.zeros(len(self.get_services()), dtype=float)
        for arrival_rates in request_class_rates.values():
            aggregate += arrival_rates
        return aggregate

    def solve(self):
        """Solve load and automatically selected per-service queueing models."""
        request_class_rates = self.__calculate_request_class_arrival_rates()
        lambdas = self.__calculate_aggregate_arrival_rates(request_class_rates)

        num_of_services = len(self.get_services())
        system_input = self.__initialize_system_input(self.__request_classes[0])
        mus = np.array(system_input["mus"], dtype=float)
        replicas = np.array(system_input["replicas"], dtype=int)

        capacities = replicas * mus
        utilizations = lambdas / capacities
        arrival_scvs = self.__calculate_effective_arrival_scvs(
            request_class_rates,
            utilizations,
            replicas,
        )

        lambdas = lambdas.tolist()
        mus = mus.tolist()
        replicas = replicas.tolist()
        utilizations = utilizations.tolist()
        service_scvs = [0.0] * num_of_services
        for service in self.get_services().values():
            service_scvs[service.get_id()] = self.__service_time_scvs[
                service.get_name()
            ]

        queueing_models = self.__current_queueing_models()
        latencies = []
        for service_id, model in enumerate(queueing_models):
            if model is AnalyticalQueueingModel.MMC:
                latency = mmc_mean_response_time(
                    lambdas[service_id],
                    mus[service_id],
                    replicas[service_id],
                )
            elif model is AnalyticalQueueingModel.MG1:
                latency = mg1_mean_response_time(
                    lambdas[service_id],
                    mus[service_id],
                    service_scv=service_scvs[service_id],
                )
            else:
                latency = variability_adjusted_mean_response_time(
                    lambdas[service_id],
                    mus[service_id],
                    replicas[service_id],
                    arrival_scv=arrival_scvs[service_id],
                    service_scv=service_scvs[service_id],
                )
            latencies.append(latency)

        return Metrics(
            utilizations,
            latencies,
            self.__is_stable(utilizations),
        )

    def simulate(
        self,
        sim_duration,
        analytical_metrics=None,
        metrics_collector=None,
        warmup_duration=None,
        *,
        seed=None,
    ):
        """Run one fixed-horizon replication in the mandatory C++ backend.

        This preserves the existing fixed-horizon API. Requests entering before
        ``warmup_duration`` affect the system but are excluded from request-class
        latency. Use ``simulate_measurement_replications`` for the uncensored
        ``[W, W + M)`` measurement-arrival window and drain semantics.
        """
        if analytical_metrics is None:
            analytical_metrics = self.solve()
        if not analytical_metrics.get_stability():
            return analytical_metrics

        sim_duration = float(sim_duration)
        if not math.isfinite(sim_duration) or sim_duration <= 0:
            raise ValueError("sim_duration must be finite and greater than zero.")

        if warmup_duration is None:
            effective_warmup = (
                metrics_collector.get_latency_collection_start()
                if metrics_collector is not None
                else 0.0
            )
        else:
            effective_warmup = float(warmup_duration)
            if (
                not math.isfinite(effective_warmup)
                or effective_warmup < 0
                or effective_warmup > sim_duration
            ):
                raise ValueError("warmup_duration must lie within the simulation.")
            if (
                metrics_collector is not None
                and metrics_collector.get_latency_collection_start() != effective_warmup
            ):
                raise ValueError(
                    "The collector and simulation warm-up durations must match."
                )

        if seed is None:
            seed = random.getrandbits(63)
        if isinstance(seed, bool) or not isinstance(seed, int) or seed < 0:
            raise ValueError("seed must be a non-negative integer.")

        native_result = get_native_backend().simulate(
            self,
            seed=seed,
            horizon=sim_duration,
            warmup_duration=effective_warmup,
            capture_state=metrics_collector is not None,
        )
        if metrics_collector is not None:
            metrics_collector.load_native_state(
                native_result,
                self.get_system_replicas(),
            )

        native_metrics = native_result["metrics"]
        return Metrics(
            native_metrics["mean_utilizations"],
            native_metrics["mean_latencies"],
            analytical_metrics.get_stability(),
            native_metrics["p95_latencies"],
            native_metrics["p99_latencies"],
            native_metrics["request_class_p95_latencies"],
        )

    def run_warmup_ensemble(self, *, seeds, horizon, bucket_width, workers=1):
        """Run native empty-start replications and aggregate buckets online."""
        if not self.solve().get_stability():
            raise ValueError("Warm-up pilots require a stable replica configuration.")
        return get_native_backend().warmup_ensemble(
            self,
            seeds=seeds,
            horizon=horizon,
            bucket_width=bucket_width,
            workers=workers,
        )

    def simulate_measurement_replications(
        self,
        *,
        warmup_duration,
        measurement_duration,
        seeds,
        workers=1,
        maximum_drain_time=10_000.0,
    ):
        """Run independent ``[W, W + M)`` cohorts and drain tagged requests.

        Background traffic continues after the tagging window until the final
        tagged request exits. Each returned record contains one request-class
        p95 per replication, suitable for across-replication inference.
        """
        if not self.solve().get_stability():
            raise ValueError("Measurement replications require a stable configuration.")
        return get_native_backend().measurement_replications(
            self,
            seeds=seeds,
            warmup_duration=warmup_duration,
            measurement_duration=measurement_duration,
            workers=workers,
            maximum_drain_time=maximum_drain_time,
        )

    def get_request_classes(self):
        return self.__request_classes

    def get_type(self):
        return self.__type

    def get_external_traffic_services(self):
        return self.__external_traffic_services

    def get_name(self):
        return self.__name

    def get_traffic_model_name(self):
        """Return the structured workload model name, if applicable."""
        return self.__traffic_model_name

    def get_workload_base_path(self):
        return self.__workload_base_path

    def get_analytical_latency_model(self):
        models = self.get_analytical_queueing_models()
        unique = tuple(dict.fromkeys(model.value for model in models.values()))
        return unique[0] if len(unique) == 1 else "mixed per-service queueing models"

    def native_simulation_supports_distributions(self):
        return True

    def get_analytical_queueing_models(self):
        """Return the automatically selected queueing model per service."""
        models = self.__current_queueing_models()
        return {
            service.get_name(): models[service.get_id()]
            for service in self.get_services().values()
        }

    def get_analytical_model_details(self):
        """Return model assumptions and variability inputs for diagnostics."""
        arrival_scvs = self.get_effective_arrival_scvs()
        models = self.__current_queueing_models()
        return {
            service.get_name(): {
                "model": models[service.get_id()].value,
                "exact": models[service.get_id()].exact,
                "arrival_scv": arrival_scvs[service.get_id()],
                "service_scv": self.__service_time_scvs[service.get_name()],
            }
            for service in self.get_services().values()
        }

    def get_workload_spec(self):
        """Return an isolated copy suitable for constructing another System."""
        return copy.deepcopy(self.__workload_spec)

    def get_request_class_concurrency_weights(self):
        """Return normalized external-traffic shares by request class."""
        return dict(self.__request_class_concurrency_weights)

    def get_request_class_mean_latency_limits(self):
        """Return analytical end-to-end mean-latency limits in seconds."""
        kind = self.get_request_class_latency_slo_kind()
        if kind == "percentile":
            raise ValueError(
                "The workload selects percentile latency SLOs, which require "
                "simulation rather than strict analytical optimization."
            )
        return {
            request_class: slo.mean_seconds
            for request_class, slo in self.__request_class_latency_slos.items()
            if slo.kind == "mean"
        }

    def get_request_class_latency_slo_kind(self):
        """Return ``mean``, ``percentile``, or ``None`` for this workload."""
        kinds = {slo.kind for slo in self.__request_class_latency_slos.values()}
        if not kinds:
            return None
        if len(kinds) != 1:
            raise ValueError("The workload mixes incompatible latency SLO kinds.")
        return next(iter(kinds))

    def get_request_class_latency_slos(self):
        """Return isolated declarations for request classes that define an SLO."""
        return {
            request_class: slo.to_dict()
            for request_class, slo in self.__request_class_latency_slos.items()
        }

    def get_request_class_percentile_latency_slos(self):
        """Return declared simulation percentile SLOs by request class."""
        kind = self.get_request_class_latency_slo_kind()
        if kind == "mean":
            raise ValueError(
                "The workload selects mean latency SLOs for analytical "
                "optimization, not percentile SLOs."
            )
        return {
            request_class: {
                "quantile": slo.percentile_quantile,
                "maximum_seconds": slo.percentile_maximum_seconds,
            }
            for request_class, slo in self.__request_class_latency_slos.items()
            if slo.kind == "percentile"
        }

    def get_request_class_percentile_latency_limits(self, *, quantile=0.95):
        """Return limits for the simulation quantile currently being evaluated."""
        if (
            isinstance(quantile, bool)
            or not isinstance(quantile, (int, float))
            or not math.isfinite(float(quantile))
            or not 0.0 < float(quantile) < 1.0
        ):
            raise ValueError("quantile must be a finite number between 0 and 1.")
        requested_quantile = float(quantile)
        slos = self.get_request_class_percentile_latency_slos()
        mismatched = {
            request_class: slo["quantile"]
            for request_class, slo in slos.items()
            if not math.isclose(
                slo["quantile"],
                requested_quantile,
                rel_tol=0.0,
                abs_tol=1e-12,
            )
        }
        if mismatched:
            raise ValueError(
                f"The workload does not define p{requested_quantile * 100:g} "
                f"limits for request classes: {sorted(mismatched)}."
            )
        return {
            request_class: slo["maximum_seconds"]
            for request_class, slo in slos.items()
        }

    def get_configured_external_arrival_rates(self):
        """Return pre-weight gamma values by class and service-ID order."""
        services = tuple(
            sorted(self.get_services().values(), key=lambda item: item.get_id())
        )
        return {
            request_class: tuple(
                self.__configured_external_arrival_rates[request_class].get(
                    service.get_name(),
                    0.0,
                )
                for service in services
            )
            for request_class in self.get_request_classes()
        }

    def get_service(self, name):
        return self.__services.get(name, None)

    def get_service_by_id(self, service_id):
        """Return the service assigned to ``service_id``, or ``None`` if absent."""
        for service in self.__services.values():
            if service.get_id() == service_id:
                return service
        return None

    def get_services(self):
        return self.__services

    def get_system_replicas(self):
        replicas = [0 for _ in range(len(self.get_services()))]
        for service in self.get_services().values():
            service_id = service.get_id()
            replicas[service_id] = service.get_replicas()
        return replicas

    def get_effective_arrival_rates(self):
        """Return aggregate analytical arrival rates in service-ID order."""
        request_class_rates = self.__calculate_request_class_arrival_rates()
        return self.__calculate_aggregate_arrival_rates(request_class_rates).tolist()

    def get_effective_arrival_scvs(self):
        """Return routed arrival-process SCV approximations in service-ID order."""
        request_class_rates = self.__calculate_request_class_arrival_rates()
        lambdas = self.__calculate_aggregate_arrival_rates(request_class_rates)
        system_input = self.__initialize_system_input(self.__request_classes[0])
        mus = np.array(system_input["mus"], dtype=float)
        replicas = np.array(system_input["replicas"], dtype=int)
        utilizations = lambdas / (replicas * mus)
        return self.__calculate_effective_arrival_scvs(
            request_class_rates,
            utilizations,
            replicas,
        )

    def get_service_time_scvs(self):
        values = [0.0] * len(self.get_services())
        for service in self.get_services().values():
            values[service.get_id()] = self.__service_time_scvs[
                service.get_name()
            ]
        return values

    def get_service_time_distribution_specs(self):
        """Return immutable validated service-time specifications by service."""
        return dict(self.__service_time_specs)

    def get_external_arrival_distribution_specs(self):
        """Return validated external interarrival specifications by class/entry."""
        return {
            request_class: dict(streams)
            for request_class, streams in self.__entry_arrival_specs.items()
        }

    def get_minimum_stable_replicas(self):
        """Return the minimum per-service replicas for strict stability.

        Replica counts are returned in service-ID order. For each service the
        smallest positive integer ``c`` satisfying ``arrival_rate < c * mu``
        is ``floor(arrival_rate / mu) + 1``.
        """
        arrival_rates = self.get_effective_arrival_rates()
        minimum_replicas = [0 for _ in range(len(self.get_services()))]

        for service in self.get_services().values():
            service_rate = service.get_mu()
            if service_rate <= 0:
                raise ValueError("Service rates must be greater than zero.")

            service_id = service.get_id()
            minimum_replicas[service_id] = (
                math.floor(arrival_rates[service_id] / service_rate) + 1
            )

        return minimum_replicas

    def get_request_class_effective_arrival_rates(self):
        """Return analytical arrival rates by class and service-ID order."""
        return {
            request_class: arrival_rates.tolist()
            for request_class, arrival_rates in self.__calculate_request_class_arrival_rates().items()
        }

    def get_request_class_mean_latencies(self):
        """Estimate end-to-end mean latency from visit ratios and queue means.

        These values are analytical means. They are not p95 estimates and do
        not carry simulation confidence intervals.
        """
        service_latencies = self.solve().get_mean_latencies()
        class_rates = self.__calculate_request_class_arrival_rates()
        results = {}
        for request_class, effective_rates in class_rates.items():
            external_rate = sum(
                service.get_gamma(request_class)
                for service in self.get_services().values()
            )
            if external_rate <= 0.0:
                raise ValueError(
                    f"Request class {request_class!r} has no external traffic."
                )
            results[request_class] = sum(
                float(rate) * float(latency)
                for rate, latency in zip(effective_rates, service_latencies)
            ) / external_rate
        return results

    def configure_system_replicas(self, replicas_config):
        for service in self.get_services().values():
            service_id = service.get_id()
            service.set_replicas(replicas_config[service_id])

    def add_service(self, name, component):
        self.__services[name] = component

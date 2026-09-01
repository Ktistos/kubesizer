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
        parameters["block_duration"] = float(parameters["block_duration"]) * time_scale
    samples = specification.samples
    sample_clusters = specification.sample_clusters
    if specification.distribution in {
        "empirical_interarrival",
        "arrival_trace",
    }:
        samples = tuple(value * time_scale for value in samples)
    if specification.distribution == "empirical_interarrival" and sample_clusters:
        sample_clusters = tuple(
            tuple(value * time_scale for value in cluster)
            for cluster in sample_clusters
        )
    return DistributionSpec(
        distribution=specification.distribution,
        mean=target_mean,
        scv=specification.scv,
        parameters=tuple(sorted(parameters.items())),
        samples=samples,
        sample_clusters=sample_clusters,
    )


def _reachable_call_services(call_graph, entry_services):
    reachable = set(entry_services)
    pending = list(reachable)
    while pending:
        source = pending.pop()
        for target, mean_calls in call_graph.get(source, {}).items():
            if mean_calls > 0.0 and target not in reachable:
                reachable.add(target)
                pending.append(target)
    return reachable


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
        self.__request_class_failure_rate_slos = {}
        self.__request_class_visit_ratios = {}
        self.__request_class_workflows = {}
        self.__request_class_call_graphs = {}
        self.__request_class_call_execution_modes = {}
        self.__request_class_fixed_latency_overheads = {}
        self.__request_class_latency_overhead_specs = {}
        self.__request_class_latency_boundaries = {}
        self.__request_class_latency_component_specs = {}
        self.__request_class_end_to_end_timeouts = {}
        self.__service_processing_slots_per_replica = {}
        self.__service_concurrency_limits_per_replica = {}
        self.__service_execution_models = {}
        self.__service_replica_queue_models = {}
        self.__service_blocking_time_specs = {}
        self.__request_class_service_blocking_time_specs = {}
        self.__class_specific_service_times = False
        self.__joint_input_uncertainty = None
        self.__communication_latency_spec = None
        self.__simulation_placement_model = None
        if structured:
            parsed = parse_workload_spec(
                workload,
                base_path=workload_base_path,
            )
            self.__traffic_distribution_spec = parsed.traffic_model
            self.__request_class_latency_slos = dict(parsed.request_class_latency_slos)
            self.__request_class_failure_rate_slos = dict(
                parsed.request_class_failure_rate_slos
            )
            self.__request_class_visit_ratios = {
                request_class: dict(visits)
                for request_class, visits in (parsed.request_class_visit_ratios.items())
            }
            self.__request_class_workflows = {
                request_class: tuple(workflow)
                for request_class, workflow in parsed.request_class_workflows.items()
            }
            self.__request_class_call_graphs = {
                request_class: {
                    source: dict(targets) for source, targets in call_graph.items()
                }
                for request_class, call_graph in (
                    parsed.request_class_call_graphs.items()
                )
            }
            self.__request_class_call_execution_modes = dict(
                parsed.request_class_call_execution_modes
            )
            self.__request_class_fixed_latency_overheads = dict(
                parsed.request_class_fixed_latency_overheads
            )
            self.__request_class_latency_overhead_specs = dict(
                parsed.request_class_latency_overhead_specs
            )
            self.__request_class_latency_boundaries = dict(
                parsed.request_class_latency_boundaries
            )
            self.__request_class_latency_component_specs = {
                request_class: dict(components)
                for request_class, components in (
                    parsed.request_class_latency_component_specs.items()
                )
            }
            self.__request_class_end_to_end_timeouts = dict(
                parsed.request_class_end_to_end_timeouts
            )
            self.__service_processing_slots_per_replica = dict(
                parsed.traffic_model.processing_slots_per_replica
            )
            self.__service_concurrency_limits_per_replica = dict(
                parsed.traffic_model.concurrency_limits_per_replica
            )
            self.__service_execution_models = dict(
                parsed.traffic_model.service_execution_models
            )
            self.__service_replica_queue_models = dict(
                parsed.traffic_model.service_replica_queue_models
            )
            self.__service_blocking_time_specs = dict(
                parsed.traffic_model.service_blocking_times
            )
            self.__request_class_service_blocking_time_specs = {
                request_class: dict(specifications)
                for request_class, specifications in (
                    parsed.traffic_model.service_blocking_times_by_request_class.items()
                )
            }
            self.__joint_input_uncertainty = parsed.traffic_model.input_uncertainty
            self.__communication_latency_spec = (
                parsed.traffic_model.communication_latency
            )
            self.__class_specific_service_times = any(
                len(
                    {
                        specifications[service_name]
                        for specifications in (
                            parsed.traffic_model.service_times_by_request_class.values()
                        )
                        if service_name in specifications
                    }
                )
                > 1
                for service_name in parsed.service_names
            )
        workload = normalize_workload_spec(
            workload,
            base_path=workload_base_path,
        )
        # type refers to either open or closed system
        self.__type = type
        self.__services: dict[str, Service] = {}
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
                elif (
                    service.get_mu() != service_mu
                    and not self.__class_specific_service_times
                ):
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
        if not self.__service_processing_slots_per_replica:
            self.__service_processing_slots_per_replica = {
                service.get_name(): 1 for service in self.get_services().values()
            }
        if not self.__service_concurrency_limits_per_replica:
            self.__service_concurrency_limits_per_replica = {
                service.get_name(): (
                    None
                    if self.__service_execution_models.get(service.get_name())
                    == "processor_sharing"
                    else self.__service_processing_slots_per_replica[
                        service.get_name()
                    ]
                )
                for service in self.get_services().values()
            }
        if not self.__service_execution_models:
            self.__service_execution_models = {
                service.get_name(): "fixed_slots"
                for service in self.get_services().values()
            }
        if not self.__service_replica_queue_models:
            self.__service_replica_queue_models = {
                service.get_name(): "pooled"
                for service in self.get_services().values()
            }
        self.__poisson_arrival_flags = self.__calculate_poisson_arrival_flags()

    def __initialize_distribution_metadata(self):
        if self.__traffic_distribution_spec is None:
            self.__service_time_specs = {
                service.get_name(): DistributionSpec(
                    distribution="exponential",
                    mean=(
                        1.0 / service.get_mu() if service.get_mu() > 0.0 else math.inf
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
            self.__request_class_service_time_specs = {
                request_class: dict(self.__service_time_specs)
                for request_class in self.get_request_classes()
            }
            self.__service_blocking_time_specs = {}
            self.__request_class_service_blocking_time_specs = {
                request_class: {} for request_class in self.get_request_classes()
            }
        else:
            traffic_model = self.__traffic_distribution_spec
            self.__service_time_specs = dict(traffic_model.service_times)
            self.__request_class_service_time_specs = {
                request_class: dict(specifications)
                for request_class, specifications in (
                    traffic_model.service_times_by_request_class.items()
                )
            }
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
            service_name: (
                "class_mixture"
                if len(
                    {
                        specifications[service_name]
                        for specifications in self.__request_class_service_time_specs.values()
                        if service_name in specifications
                    }
                )
                > 1
                else process.distribution
            )
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
            call_graph = self.__request_class_call_graphs.get(request_class)
            if call_graph is not None:
                reachable = {
                    self.get_service(service_name).get_id()
                    for service_name in _reachable_call_services(
                        call_graph,
                        (
                            service.get_name()
                            for service in self.get_services().values()
                            if service.get_gamma(request_class) > 0.0
                        ),
                    )
                }
                reachable_by_class[request_class] = reachable
                for service_id in reachable:
                    flags[service_id] = False
                continue
            explicit_visits = self.__request_class_visit_ratios.get(request_class)
            if explicit_visits is not None:
                reachable = {
                    self.get_service(service_name).get_id()
                    for service_name in explicit_visits
                }
                reachable_by_class[request_class] = reachable
                # Visit ratios specify mean flow only. They do not specify the
                # timing or dependence of fan-out/repeated calls, so an exact
                # Poisson arrival claim would be unjustified.
                for service_id in reachable:
                    flags[service_id] = False
                continue
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
                replicas=(
                    service.get_replicas()
                    * self.__service_processing_slots_per_replica[service.get_name()]
                ),
            )
        return tuple(models)

    def __calculate_effective_arrival_scvs(
        self,
        request_class_rates,
        utilizations,
        replicas,
        service_scvs=None,
    ):
        service_count = len(self.get_services())
        if service_scvs is None:
            service_scvs = self.get_service_time_scvs()

        external_components = [[] for _ in range(service_count)]
        routed_components = []
        for request_class, class_rates in request_class_rates.items():
            explicit_visits = self.__request_class_visit_ratios.get(request_class)
            if explicit_visits is not None:
                external_streams = []
                for service in self.get_services().values():
                    external_rate = service.get_gamma(request_class)
                    if external_rate > 0.0:
                        external_streams.append(
                            (
                                external_rate,
                                self.__entry_arrival_scvs[request_class].get(
                                    service.get_name(), 1.0
                                ),
                            )
                        )
                class_arrival_scv = combine_independent_stream_scvs(external_streams)
                for service_name in explicit_visits:
                    service_id = self.get_service(service_name).get_id()
                    flow_rate = float(class_rates[service_id])
                    if flow_rate > 0.0:
                        external_components[service_id].append(
                            (flow_rate, class_arrival_scv)
                        )
                continue
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
                        thinned_stream_scv(departure_scvs[source_id], probability),
                    )
                )
            raw = [
                (
                    1.0
                    if self.__poisson_arrival_flags[service_id]
                    else combine_independent_stream_scvs(components[service_id])
                )
                for service_id in range(service_count)
            ]
            updated = [
                0.5 * previous + 0.5 * candidate
                for previous, candidate in zip(arrival_scvs, raw)
            ]
            if (
                max(
                    abs(candidate - previous)
                    for candidate, previous in zip(updated, arrival_scvs)
                )
                <= 1e-10
            ):
                return updated
            arrival_scvs = updated
        raise ValueError("Analytical arrival-variability propagation did not converge.")

    def __initialize_system_input(self, request_class_name):
        num_of_services = len(self.get_services())
        gammas = [0.0 for _ in range(num_of_services)]
        mus = [0.0 for _ in range(num_of_services)]
        replicas = [0 for _ in range(num_of_services)]
        servers = [0 for _ in range(num_of_services)]

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
            servers[order_index] = (
                service_replicas
                * self.__service_processing_slots_per_replica[service_name]
            )

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
            "servers": servers,
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
        rates = {}
        for request_class in self.get_request_classes():
            call_graph = self.__request_class_call_graphs.get(request_class)
            if call_graph is not None:
                system_input = self.__initialize_system_input(request_class)
                service_count = len(self.get_services())
                calls = np.zeros((service_count, service_count), dtype=float)
                for source_name, targets in call_graph.items():
                    source_id = self.get_service(source_name).get_id()
                    for target_name, mean_calls in targets.items():
                        calls[source_id, self.get_service(target_name).get_id()] = (
                            mean_calls
                        )
                spectral_radius = max(abs(value) for value in np.linalg.eigvals(calls))
                if spectral_radius >= 1.0 - 1e-12:
                    raise ValueError(
                        f"Request class {request_class!r} synchronous call graph "
                        "is not open; its mean offspring spectral radius must be "
                        "less than one."
                    )
                rates[request_class] = np.linalg.solve(
                    np.eye(service_count) - calls.T,
                    np.array(system_input["gammas"], dtype=float),
                )
                continue
            explicit_visits = self.__request_class_visit_ratios.get(request_class)
            if explicit_visits is None:
                rates[request_class] = self.__calculate_effective_arrival_rates(
                    self.__initialize_system_input(request_class)
                )
                continue
            external_rate = sum(
                service.get_gamma(request_class)
                for service in self.get_services().values()
            )
            rates[request_class] = np.array(
                [
                    external_rate * explicit_visits.get(service.get_name(), 0.0)
                    for service in sorted(
                        self.get_services().values(),
                        key=lambda item: item.get_id(),
                    )
                ],
                dtype=float,
            )
        return rates

    def __calculate_aggregate_arrival_rates(self, request_class_rates):
        aggregate = np.zeros(len(self.get_services()), dtype=float)
        for arrival_rates in request_class_rates.values():
            aggregate += arrival_rates
        return aggregate

    def __calculate_service_time_moments(self, request_class_rates):
        """Return arrival-weighted service means and SCVs at every service.

        When operations have distinct demands at one service, FCFS sees their
        arrival-weighted mixture. The second moment includes both the declared
        within-class variability and between-class mean differences.
        """
        means = [0.0] * len(self.get_services())
        scvs = [0.0] * len(self.get_services())
        for service in self.get_services().values():
            service_id = service.get_id()
            observations = []
            for request_class, arrival_rates in request_class_rates.items():
                rate = float(arrival_rates[service_id])
                specification = self.__request_class_service_time_specs.get(
                    request_class, {}
                ).get(service.get_name())
                if rate > 0.0:
                    if specification is None:
                        raise ValueError(
                            f"No service-time distribution exists for request "
                            f"class {request_class!r} at {service.get_name()!r}."
                        )
                    observations.append((rate, specification))
            total_rate = sum(rate for rate, _ in observations)
            if total_rate <= 0.0:
                specification = self.__service_time_specs[service.get_name()]
                means[service_id] = specification.mean
                scvs[service_id] = specification.scv
                continue
            mean = (
                sum(rate * specification.mean for rate, specification in observations)
                / total_rate
            )
            second_moment = (
                sum(
                    rate
                    * (1.0 + specification.scv)
                    * specification.mean
                    * specification.mean
                    for rate, specification in observations
                )
                / total_rate
            )
            means[service_id] = mean
            scvs[service_id] = max(0.0, second_moment / (mean * mean) - 1.0)
        return means, scvs

    def solve(self):
        """Solve load and automatically selected per-service queueing models."""
        request_class_rates = self.__calculate_request_class_arrival_rates()
        lambdas = self.__calculate_aggregate_arrival_rates(request_class_rates)

        system_input = self.__initialize_system_input(self.__request_classes[0])
        service_means, service_scvs = self.__calculate_service_time_moments(
            request_class_rates
        )
        mus = 1.0 / np.array(service_means, dtype=float)
        servers = np.array(system_input["servers"], dtype=int)

        capacities = servers * mus
        utilizations = lambdas / capacities
        arrival_scvs = self.__calculate_effective_arrival_scvs(
            request_class_rates,
            utilizations,
            servers,
            service_scvs=service_scvs,
        )

        lambdas = lambdas.tolist()
        mus = mus.tolist()
        servers = servers.tolist()
        utilizations = utilizations.tolist()
        concurrency_utilizations = self.__concurrency_utilization_lower_bounds(
            request_class_rates
        )
        queueing_models = self.__current_queueing_models()
        latencies = []
        for service_id, model in enumerate(queueing_models):
            if model is AnalyticalQueueingModel.MMC:
                latency = mmc_mean_response_time(
                    lambdas[service_id],
                    mus[service_id],
                    servers[service_id],
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
                    servers[service_id],
                    arrival_scv=arrival_scvs[service_id],
                    service_scv=service_scvs[service_id],
                )
            latencies.append(latency)

        return Metrics(
            utilizations,
            latencies,
            (
                self.__is_stable(utilizations)
                and self.__is_stable(concurrency_utilizations)
            ),
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
        self.require_simulation_compatible_workload()
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
        self.require_simulation_compatible_workload()
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
        self.require_simulation_compatible_workload()
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
        service_scvs = self.get_service_time_scvs()
        models = self.__current_queueing_models()
        concurrency_utilizations = self.get_concurrency_utilization_lower_bounds()
        return {
            service.get_name(): {
                "model": models[service.get_id()].value,
                "exact": models[service.get_id()].exact,
                "arrival_scv": arrival_scvs[service.get_id()],
                "service_scv": service_scvs[service.get_id()],
                "class_specific_service_time_mixture": (
                    self.__class_specific_service_times
                ),
                "concurrency_limit_per_replica": (
                    self.__service_concurrency_limits_per_replica[service.get_name()]
                ),
                "processing_slots_per_replica": (
                    self.__service_processing_slots_per_replica[service.get_name()]
                ),
                "execution_model": self.__service_execution_models[service.get_name()],
                "replica_queue_model": self.__service_replica_queue_models[
                    service.get_name()
                ],
                "non_cpu_blocking_time_by_request_class": {
                    request_class: specification.mean
                    for request_class in self.get_request_classes()
                    if (
                        specification := self.__request_class_service_blocking_time_specs.get(
                            request_class,
                            {},
                        ).get(service.get_name())
                    )
                    is not None
                },
                "effective_processing_slots": (
                    service.get_replicas()
                    * self.__service_processing_slots_per_replica[service.get_name()]
                ),
                "concurrency_utilization_lower_bound": (
                    concurrency_utilizations[service.get_id()]
                ),
            }
            for service in self.get_services().values()
        }

    def get_workload_spec(self):
        """Return an isolated copy suitable for constructing another System."""
        return copy.deepcopy(self.__workload_spec)

    def get_request_class_concurrency_weights(self):
        """Return normalized external-traffic shares by request class."""
        return dict(self.__request_class_concurrency_weights)

    def get_request_class_visit_ratios(self):
        """Return explicitly supplied analytical visits per external request."""
        return {
            request_class: dict(visits)
            for request_class, visits in self.__request_class_visit_ratios.items()
        }

    def get_request_class_workflows(self):
        """Return executable ordered visit specifications by request class."""
        return {
            request_class: tuple(workflow)
            for request_class, workflow in self.__request_class_workflows.items()
        }

    def get_request_class_call_graphs(self):
        """Return expected synchronous child calls per source service call."""
        return {
            request_class: {
                source: dict(targets) for source, targets in call_graph.items()
            }
            for request_class, call_graph in self.__request_class_call_graphs.items()
        }

    def get_request_class_call_execution_modes(self):
        """Return parallel/sequential sibling-call semantics by class."""
        return dict(self.__request_class_call_execution_modes)

    def get_service_blocking_time_distribution_specs(self):
        """Return shared non-CPU blocking-time distributions by service.

        A blocking interval releases the service's CPU processing capacity but
        keeps one admitted-concurrency slot occupied. It therefore represents
        measured I/O, lock, or external wait rather than CPU execution work.
        """
        return dict(self.__service_blocking_time_specs)

    def get_request_class_service_blocking_time_distribution_specs(self):
        """Return class-specific non-CPU blocking distributions by service."""
        return {
            request_class: dict(specifications)
            for request_class, specifications in (
                self.__request_class_service_blocking_time_specs.items()
            )
        }

    def get_strict_analytical_incompatibilities(self):
        """Return declared semantics not certified by the analytical model."""
        incompatibilities = []
        if any(self.__request_class_service_blocking_time_specs.values()):
            incompatibilities.append(
                "per-visit non-CPU blocking time occupies finite service "
                "concurrency after CPU work completes"
            )
        held_concurrency_sources = sorted(
            {
                source
                for call_graph in self.__request_class_call_graphs.values()
                for source, targets in call_graph.items()
                if targets
                and self.__service_concurrency_limits_per_replica[source] is not None
            }
        )
        if held_concurrency_sources:
            incompatibilities.append(
                "synchronous downstream calls hold finite parent concurrency "
                "at services " + ", ".join(held_concurrency_sources)
            )
        return tuple(incompatibilities)

    def get_request_class_fixed_latency_overheads(self):
        """Return the mean non-queueing latency for each request class."""
        return dict(self.__request_class_fixed_latency_overheads)

    def get_request_class_latency_overhead_distribution_specs(self):
        """Return validated per-request non-queueing duration distributions."""
        return dict(self.__request_class_latency_overhead_specs)

    def get_request_class_latency_boundaries(self):
        """Return the observation boundary represented by each class latency."""
        return dict(self.__request_class_latency_boundaries)

    def get_request_class_latency_component_distribution_specs(self):
        """Return named, independently measured non-service latency components."""
        return {
            request_class: dict(components)
            for request_class, components in (
                self.__request_class_latency_component_specs.items()
            )
        }

    def get_communication_latency_distribution_spec(self):
        """Return same/cross-node one-way evidence used before placement."""
        return self.__communication_latency_spec

    def get_request_class_end_to_end_timeouts(self):
        """Return client-visible terminal deadlines; zero disables a deadline."""
        return dict(self.__request_class_end_to_end_timeouts)

    def require_simulation_compatible_workload(self):
        """Reject mean-flow-only visits that do not define executable paths."""
        visit_classes = tuple(
            sorted(
                set(self.__request_class_visit_ratios)
                - set(self.__request_class_workflows)
            )
        )
        if visit_classes:
            raise ValueError(
                "Simulation requires sequential routing probabilities, but request "
                f"classes {list(visit_classes)} define analytical visit ratios. "
                "Use analytical replica evaluation or replace visits with an "
                "executable workflow."
            )

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

    def get_request_class_failure_rate_slos(self):
        """Return terminal-failure-rate limits declared for simulation."""
        return dict(self.__request_class_failure_rate_slos)

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
            request_class: slo["maximum_seconds"] for request_class, slo in slos.items()
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

    def get_service_concurrency_limits_per_replica(self):
        """Return maximum in-flight calls admitted by one replica."""
        return dict(self.__service_concurrency_limits_per_replica)

    def get_service_processing_slots_per_replica(self):
        """Return effective processing servers contributed by one replica."""
        return dict(self.__service_processing_slots_per_replica)

    def get_service_execution_models(self):
        """Return the native execution semantics selected for each service."""
        return dict(self.__service_execution_models)

    def get_service_replica_queue_models(self):
        """Return how requests are dispatched to queues across replicas."""
        return dict(self.__service_replica_queue_models)

    def get_joint_input_uncertainty(self):
        """Return aligned input-window uncertainty sampled once per replication."""
        return self.__joint_input_uncertainty

    def configure_simulation_placement(
        self,
        placement,
        communication_latency_seconds,
        execution_time_multipliers=None,
    ):
        """Attach a fixed placement to subsequent native simulations.

        Communication latency is directional and interpreted as one-way delay.
        Synchronous calls pay both the request and response directions.  Node
        execution multipliers are optional measured relative slowdowns; a
        missing multiplier means one, not an inferred contention penalty.
        """
        if not isinstance(placement, dict) or not placement:
            raise ValueError("placement must be a nonempty node mapping.")
        node_names = tuple(placement)
        service_names = tuple(self.__services)
        if any(not isinstance(name, str) or not name for name in node_names):
            raise ValueError("placement node names must be nonempty strings.")
        normalized_placement = {}
        totals = {name: 0 for name in service_names}
        for node_name in node_names:
            raw_counts = placement[node_name]
            if not isinstance(raw_counts, dict):
                raise ValueError("each placement node must map services to counts.")
            unknown = set(raw_counts) - set(service_names)
            if unknown:
                raise ValueError(f"placement contains unknown services: {unknown}.")
            counts = {}
            for service_name in service_names:
                value = raw_counts.get(service_name, 0)
                if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                    raise ValueError("placement replica counts must be non-negative integers.")
                counts[service_name] = value
                totals[service_name] += value
            normalized_placement[node_name] = counts
        expected = {
            name: service.get_replicas()
            for name, service in self.__services.items()
        }
        if totals != expected:
            raise ValueError(
                "placement replica totals do not match the configured system replicas."
            )
        if not isinstance(communication_latency_seconds, dict):
            raise ValueError("communication_latency_seconds must be a node mapping.")
        latency_matrix = {}
        for source in node_names:
            raw_row = communication_latency_seconds.get(source)
            if not isinstance(raw_row, dict):
                raise ValueError(f"missing communication latency row for {source!r}.")
            row = {}
            for destination in node_names:
                raw_value = raw_row.get(destination)
                if raw_value is None and source == destination:
                    raw_value = 0.0
                if raw_value is None:
                    raise ValueError(
                        f"missing communication latency {source!r}->{destination!r}."
                    )
                value = float(raw_value)
                if not math.isfinite(value) or value < 0.0:
                    raise ValueError("communication latencies must be finite and non-negative.")
                row[destination] = value
            latency_matrix[source] = row
        raw_multipliers = execution_time_multipliers or {}
        unknown_multiplier_nodes = set(raw_multipliers) - set(node_names)
        if unknown_multiplier_nodes:
            raise ValueError(
                "execution multipliers contain nodes outside the placement."
            )
        multipliers = {}
        for node_name in node_names:
            value = float(raw_multipliers.get(node_name, 1.0))
            if not math.isfinite(value) or value <= 0.0:
                raise ValueError("node execution multipliers must be finite and positive.")
            multipliers[node_name] = value
        self.__simulation_placement_model = {
            "node_names": node_names,
            "placement": normalized_placement,
            "communication_latency_seconds": latency_matrix,
            "execution_time_multipliers": multipliers,
        }

    def clear_simulation_placement(self):
        self.__simulation_placement_model = None

    def get_simulation_placement_model(self):
        if self.__simulation_placement_model is None:
            return None
        model = self.__simulation_placement_model
        return {
            "node_names": tuple(model["node_names"]),
            "placement": {
                node: dict(counts)
                for node, counts in model["placement"].items()
            },
            "communication_latency_seconds": {
                node: dict(row)
                for node, row in model["communication_latency_seconds"].items()
            },
            "execution_time_multipliers": dict(
                model["execution_time_multipliers"]
            ),
        }

    def get_effective_service_capacities(self):
        """Return total processing slots in service-ID order."""
        capacities = [0 for _ in range(len(self.get_services()))]
        for service in self.get_services().values():
            capacities[service.get_id()] = (
                service.get_replicas()
                * self.__service_processing_slots_per_replica[service.get_name()]
            )
        return capacities

    def get_effective_arrival_rates(self):
        """Return aggregate analytical arrival rates in service-ID order."""
        request_class_rates = self.__calculate_request_class_arrival_rates()
        return self.__calculate_aggregate_arrival_rates(request_class_rates).tolist()

    def get_effective_arrival_scvs(self):
        """Return routed arrival-process SCV approximations in service-ID order."""
        request_class_rates = self.__calculate_request_class_arrival_rates()
        lambdas = self.__calculate_aggregate_arrival_rates(request_class_rates)
        system_input = self.__initialize_system_input(self.__request_classes[0])
        service_means, service_scvs = self.__calculate_service_time_moments(
            request_class_rates
        )
        mus = 1.0 / np.array(service_means, dtype=float)
        servers = np.array(system_input["servers"], dtype=int)
        utilizations = lambdas / (servers * mus)
        return self.__calculate_effective_arrival_scvs(
            request_class_rates,
            utilizations,
            servers,
            service_scvs=service_scvs,
        )

    def get_service_time_scvs(self):
        request_class_rates = self.__calculate_request_class_arrival_rates()
        _, values = self.__calculate_service_time_moments(request_class_rates)
        return values

    def get_service_time_distribution_specs(self):
        """Return canonical specs; simulation rejects heterogeneous class specs."""
        return dict(self.__service_time_specs)

    def get_request_class_service_time_distribution_specs(self):
        """Return validated service-time distributions by request class/service."""
        return {
            request_class: dict(specifications)
            for request_class, specifications in (
                self.__request_class_service_time_specs.items()
            )
        }

    def get_external_arrival_distribution_specs(self):
        """Return validated external interarrival specifications by class/entry."""
        return {
            request_class: dict(streams)
            for request_class, streams in self.__entry_arrival_specs.items()
        }

    def get_minimum_stable_replicas(self):
        """Return the minimum per-service replicas for strict stability.

        Replica counts are returned in service-ID order. If one replica exposes
        ``p`` effective processing slots, the strict stability condition is
        ``offered_load < replicas * p``.
        """
        request_class_rates = self.__calculate_request_class_arrival_rates()
        concurrency_offered_loads = self.__concurrency_offered_loads(
            request_class_rates
        )
        minimum_replicas = [0 for _ in range(len(self.get_services()))]

        for service in self.get_services().values():
            service_id = service.get_id()
            offered_load = sum(
                float(arrival_rates[service_id])
                * self.__request_class_service_time_specs[request_class][
                    service.get_name()
                ].mean
                for request_class, arrival_rates in request_class_rates.items()
                if float(arrival_rates[service_id]) > 0.0
            )
            processing_slots = self.__service_processing_slots_per_replica[
                service.get_name()
            ]
            processing_minimum = math.floor(offered_load / processing_slots) + 1
            concurrency_limit = self.__service_concurrency_limits_per_replica[
                service.get_name()
            ]
            concurrency_minimum = (
                1
                if concurrency_limit is None
                else math.floor(
                    concurrency_offered_loads[service_id] / concurrency_limit
                )
                + 1
            )
            minimum_replicas[service_id] = max(
                processing_minimum,
                concurrency_minimum,
            )

        return minimum_replicas

    def __concurrency_offered_loads(self, request_class_rates):
        """Return a necessary lower bound on admitted-concurrency occupancy.

        A visit occupies concurrency for at least its own processing and
        non-CPU blocking phases. Synchronous downstream waiting can extend the
        holding time further, so this deliberately remains a lower bound.
        """
        offered = [0.0 for _ in range(len(self.get_services()))]
        for request_class, arrival_rates in request_class_rates.items():
            blocking_specs = self.__request_class_service_blocking_time_specs.get(
                request_class,
                {},
            )
            service_specs = self.__request_class_service_time_specs[request_class]
            for service in self.get_services().values():
                rate = float(arrival_rates[service.get_id()])
                if rate <= 0.0:
                    continue
                blocking = blocking_specs.get(service.get_name())
                holding_time = service_specs[service.get_name()].mean + (
                    blocking.mean if blocking is not None else 0.0
                )
                offered[service.get_id()] += rate * holding_time
        return offered

    def __concurrency_utilization_lower_bounds(self, request_class_rates):
        offered = self.__concurrency_offered_loads(request_class_rates)
        utilizations = [0.0 for _ in range(len(self.get_services()))]
        for service in self.get_services().values():
            service_id = service.get_id()
            limit = self.__service_concurrency_limits_per_replica[
                service.get_name()
            ]
            if limit is None:
                continue
            capacity = service.get_replicas() * limit
            utilizations[service_id] = offered[service_id] / capacity
        return utilizations

    def get_concurrency_utilization_lower_bounds(self):
        """Return necessary per-service finite-concurrency utilizations.

        Values include processing and declared non-CPU blocking, but exclude
        queueing and synchronous child-call waiting. Therefore a value at or
        above one proves instability; a smaller value does not prove it.
        """
        return self.__concurrency_utilization_lower_bounds(
            self.__calculate_request_class_arrival_rates()
        )

    def get_request_class_effective_arrival_rates(self):
        """Return analytical arrival rates by class and service-ID order."""
        return {
            request_class: arrival_rates.tolist()
            for request_class, arrival_rates in self.__calculate_request_class_arrival_rates().items()
        }

    def __mean_one_way_latency(self, source_name, target_name):
        placement = self.__simulation_placement_model
        if placement is not None:
            source_counts = {
                node: placement["placement"][node][source_name]
                for node in placement["node_names"]
            }
            target_counts = {
                node: placement["placement"][node][target_name]
                for node in placement["node_names"]
            }
            source_total = sum(source_counts.values())
            target_total = sum(target_counts.values())
            if source_total <= 0 or target_total <= 0:
                raise ValueError("Placed services must have at least one replica.")
            expected = 0.0
            for source_node, source_count in source_counts.items():
                for target_node, target_count in target_counts.items():
                    probability = (
                        source_count
                        * target_count
                        / (source_total * target_total)
                    )
                    expected += probability * placement[
                        "communication_latency_seconds"
                    ][source_node][target_node]
            return expected
        specification = self.__communication_latency_spec
        if specification is None:
            return 0.0
        probability = specification.unplaced_cross_node_probability
        return (
            (1.0 - probability) * specification.same_node_one_way.mean
            + probability * specification.cross_node_one_way.mean
        )

    def __mean_call_round_trip_latency(self, source_name, target_name):
        return self.__mean_one_way_latency(
            source_name,
            target_name,
        ) + self.__mean_one_way_latency(target_name, source_name)

    def __mean_request_class_network_latency(
        self,
        request_class,
        effective_rates,
        external_rate,
    ):
        call_graph = self.__request_class_call_graphs.get(request_class)
        if call_graph is not None:
            result = 0.0
            for source_name, targets in call_graph.items():
                source = self.get_service(source_name)
                source_visits = (
                    float(effective_rates[source.get_id()]) / external_rate
                )
                for target_name, mean_calls in targets.items():
                    result += (
                        source_visits
                        * mean_calls
                        * self.__mean_call_round_trip_latency(
                            source_name,
                            target_name,
                        )
                    )
            return result
        workflow = self.__request_class_workflows.get(request_class)
        if workflow is not None:
            # A workflow has ordered blocks. Repeated visits within a block use
            # the same service-to-service latency semantics as every other
            # visit transition. Fractional visit counts are expectations.
            result = 0.0
            for visit in workflow:
                result += max(0.0, visit.mean_visits - 1.0) * (
                    self.__mean_one_way_latency(
                        visit.service,
                        visit.service,
                    )
                )
            for source, target in zip(workflow, workflow[1:]):
                result += min(1.0, target.mean_visits) * (
                    self.__mean_one_way_latency(
                        source.service,
                        target.service,
                    )
                )
            return result
        result = 0.0
        for source in self.get_services().values():
            source_visits = float(effective_rates[source.get_id()]) / external_rate
            if source_visits <= 0.0:
                continue
            for edge in source.get_adjacency_list(request_class):
                target = edge.get_target()
                probability = edge.get_routing_prob()
                if target is None or probability <= 0.0:
                    continue
                result += source_visits * probability * (
                    self.__mean_one_way_latency(
                        source.get_name(),
                        target.get_name(),
                    )
                )
        return result

    def get_request_class_mean_latencies(self):
        """Estimate end-to-end mean latency from visit ratios and queue means.

        These values are analytical means. They are not p95 estimates and do
        not carry simulation confidence intervals.
        """
        service_latencies = self.solve().get_mean_latencies()
        class_rates = self.__calculate_request_class_arrival_rates()
        service_means, _ = self.__calculate_service_time_moments(class_rates)
        queue_waits = [
            max(0.0, float(latency) - service_means[service_id])
            for service_id, latency in enumerate(service_latencies)
        ]
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
            queueing_latency = 0.0
            for service in self.get_services().values():
                service_id = service.get_id()
                rate = float(effective_rates[service_id])
                if rate <= 0.0:
                    continue
                specification = self.__request_class_service_time_specs[request_class][
                    service.get_name()
                ]
                visits = rate / external_rate
                queueing_latency += visits * (
                    specification.mean + queue_waits[service_id]
                )
                blocking = self.__request_class_service_blocking_time_specs.get(
                    request_class,
                    {},
                ).get(service.get_name())
                if blocking is not None:
                    queueing_latency += visits * blocking.mean
            results[request_class] = (
                queueing_latency
                + self.__mean_request_class_network_latency(
                    request_class,
                    effective_rates,
                    external_rate,
                )
                + self.__request_class_fixed_latency_overheads.get(
                    request_class, 0.0
                )
            )
        return results

    def configure_system_replicas(self, replicas_config):
        for service in self.get_services().values():
            service_id = service.get_id()
            service.set_replicas(replicas_config[service_id])

    def add_service(self, name, component):
        self.__services[name] = component

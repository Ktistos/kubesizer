import math
from collections.abc import Mapping
from dataclasses import dataclass
from itertools import product

from .base import OptimizationProblem


DEFAULT_COST_WEIGHTS = {
    "network": 0.7,
    "replica_spreading": 0.3,
}


@dataclass(frozen=True)
class NodePlacementEvaluation:
    """Constraint and objective values for one fixed-replica placement."""

    configuration: tuple[tuple[str, tuple[tuple[str, int], ...]], ...]
    valid: bool
    total_cost: float
    network_cost_raw: float
    network_cost_normalized: float
    average_network_delay_seconds: float
    replica_spreading_penalty: float
    constraint_violations: tuple[str, ...]
    resource_feasibility_checked: bool

    @property
    def feasible(self):
        """Compatibility alias used by optimization problem callers."""
        return self.valid

    @property
    def cost(self):
        """Return the weighted placement objective."""
        return self.total_cost

    def to_dict(self):
        """Return the result structure used by API and reporting layers."""
        return {
            "valid": self.valid,
            "total_cost": self.total_cost,
            "network_cost_raw": self.network_cost_raw,
            "network_cost_normalized": self.network_cost_normalized,
            "average_network_delay_seconds": (
                self.average_network_delay_seconds
            ),
            "replica_spreading_penalty": self.replica_spreading_penalty,
            "constraint_violations": list(self.constraint_violations),
            "resource_feasibility_checked": (
                self.resource_feasibility_checked
            ),
        }


class NodePlacementProblem(OptimizationProblem):
    """Evaluate fixed-replica service placements on Kubernetes nodes.

    A candidate uses the node-oriented representation::

        {
            "node-a": {"api": 2, "worker": 1},
            "node-b": {"api": 1},
        }

    Replica totals are immutable constraints. The objective combines
    normalized latency-weighted service communication with a normalized
    same-service concentration penalty. CPU and memory constraints are
    enabled only when requests are supplied for every service.
    """

    def __init__(
        self,
        cluster,
        cost_weights=None,
        service_resource_requests=None,
    ):
        self.__cluster = cluster
        self.__system = cluster.get_system()
        self.__services = dict(self.__system.get_services())
        self.__nodes = dict(cluster.get_nodes())

        if not self.__services:
            raise ValueError("Node placement requires at least one service.")
        if not self.__nodes:
            raise ValueError("Node placement requires at least one node.")

        self.__service_names = tuple(self.__services)
        self.__node_names = tuple(self.__nodes)
        self.__fixed_replicas = self.__read_fixed_replica_counts()
        self.__cost_weights = self.__normalize_cost_weights(cost_weights)
        self.__resource_requests = self.__normalize_resource_requests(
            service_resource_requests
        )
        (
            self.__node_latencies,
            self.__maximum_inter_node_latency,
        ) = self.__read_node_latencies()
        (
            self.__service_arrival_rates,
            self.__edge_traffic_rates,
            self.__total_external_arrival_rate,
        ) = self.__derive_traffic_rates()
        self.__total_edge_traffic_rate = sum(
            self.__edge_traffic_rates.values()
        )

    def evaluate(self, configuration):
        """Validate and calculate both placement objective components."""
        placement, violations = self.__validate_configuration(configuration)

        if self.__resource_requests is not None:
            violations.extend(self.__validate_resource_capacities(placement))

        canonical_configuration = self.__canonical_configuration(placement)
        resource_checked = self.__resource_requests is not None

        if violations:
            return NodePlacementEvaluation(
                configuration=canonical_configuration,
                valid=False,
                total_cost=math.inf,
                network_cost_raw=math.inf,
                network_cost_normalized=math.inf,
                average_network_delay_seconds=math.inf,
                replica_spreading_penalty=math.inf,
                constraint_violations=tuple(violations),
                resource_feasibility_checked=resource_checked,
            )

        network_cost_raw = self.__calculate_network_cost(placement)
        network_denominator = (
            self.__maximum_inter_node_latency
            * self.__total_edge_traffic_rate
        )
        network_cost_normalized = (
            network_cost_raw / network_denominator
            if network_denominator > 0
            else 0.0
        )
        average_network_delay = (
            network_cost_raw / self.__total_external_arrival_rate
            if self.__total_external_arrival_rate > 0
            else 0.0
        )
        spreading_penalty = self.__calculate_spreading_penalty(placement)
        total_cost = (
            self.__cost_weights["network"] * network_cost_normalized
            + self.__cost_weights["replica_spreading"]
            * spreading_penalty
        )

        return NodePlacementEvaluation(
            configuration=canonical_configuration,
            valid=True,
            total_cost=total_cost,
            network_cost_raw=network_cost_raw,
            network_cost_normalized=network_cost_normalized,
            average_network_delay_seconds=average_network_delay,
            replica_spreading_penalty=spreading_penalty,
            constraint_violations=(),
            resource_feasibility_checked=resource_checked,
        )

    def is_feasible(self, configuration):
        """Return whether the candidate satisfies every active constraint."""
        return self.evaluate(configuration).valid

    def get_fixed_replica_counts(self):
        return dict(self.__fixed_replicas)

    def get_service_arrival_rates(self):
        return dict(self.__service_arrival_rates)

    def get_edge_traffic_rates(self):
        return dict(self.__edge_traffic_rates)

    def get_cost_weights(self):
        return dict(self.__cost_weights)

    def resource_feasibility_is_checked(self):
        return self.__resource_requests is not None

    def configuration_count(self):
        """Return the exact size of the fixed-replica placement space."""
        node_count = len(self.__node_names)
        return math.prod(
            math.comb(replica_count + node_count - 1, node_count - 1)
            for replica_count in self.__fixed_replicas.values()
        )

    def iter_configurations(self):
        """Yield every placement that preserves all fixed replica totals."""
        node_count = len(self.__node_names)
        distribution_options = tuple(
            tuple(
                self.__replica_distributions(replica_count, node_count)
            )
            for replica_count in self.__fixed_replicas.values()
        )

        for service_distributions in product(*distribution_options):
            placement = {
                node_name: {} for node_name in self.__node_names
            }

            for service_name, distribution in zip(
                self.__service_names,
                service_distributions,
            ):
                for node_name, replica_count in zip(
                    self.__node_names,
                    distribution,
                ):
                    if replica_count > 0:
                        placement[node_name][service_name] = replica_count

            yield {
                node_name: service_counts
                for node_name, service_counts in placement.items()
                if service_counts
            }

    @staticmethod
    def __replica_distributions(replica_count, node_count):
        if node_count == 1:
            yield (replica_count,)
            return

        for first_node_count in range(replica_count + 1):
            for remainder in NodePlacementProblem.__replica_distributions(
                replica_count - first_node_count,
                node_count - 1,
            ):
                yield (first_node_count,) + remainder

    def __read_fixed_replica_counts(self):
        fixed_replicas = {}
        for service_name, service in self.__services.items():
            replicas = service.get_replicas()
            if (
                isinstance(replicas, bool)
                or not isinstance(replicas, int)
                or replicas < 1
            ):
                raise ValueError(
                    f"Service '{service_name}' must have a positive integer "
                    "replica count."
                )
            fixed_replicas[service_name] = replicas
        return fixed_replicas

    def __normalize_cost_weights(self, cost_weights):
        normalized = dict(DEFAULT_COST_WEIGHTS)
        if cost_weights is None:
            return normalized
        if not isinstance(cost_weights, Mapping):
            raise TypeError("cost_weights must be a mapping.")

        unknown_weights = set(cost_weights) - set(DEFAULT_COST_WEIGHTS)
        if unknown_weights:
            names = ", ".join(sorted(unknown_weights))
            raise ValueError(f"Unknown placement cost weights: {names}.")

        normalized.update(cost_weights)
        for name, value in normalized.items():
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value < 0
            ):
                raise ValueError(
                    f"Cost weight '{name}' must be a finite nonnegative number."
                )
        return normalized

    def __normalize_resource_requests(self, resource_requests):
        if resource_requests is None:
            return None
        if not isinstance(resource_requests, Mapping):
            raise TypeError("service_resource_requests must be a mapping.")

        supplied_services = set(resource_requests)
        required_services = set(self.__service_names)
        missing_services = required_services - supplied_services
        unknown_services = supplied_services - required_services
        if missing_services or unknown_services:
            details = []
            if missing_services:
                details.append(
                    "missing: " + ", ".join(sorted(missing_services))
                )
            if unknown_services:
                details.append(
                    "unknown: " + ", ".join(sorted(unknown_services))
                )
            raise ValueError(
                "Resource requests must be supplied for every service ("
                + "; ".join(details)
                + ")."
            )

        normalized = {}
        for service_name in self.__service_names:
            requests = resource_requests[service_name]
            if not isinstance(requests, Mapping):
                raise TypeError(
                    f"Resource requests for '{service_name}' must be a mapping."
                )

            normalized[service_name] = {
                "cpu_request_millicores": self.__validate_resource_value(
                    requests.get("cpu_request_millicores"),
                    service_name,
                    "cpu_request_millicores",
                ),
                "memory_request_bytes": self.__validate_resource_value(
                    requests.get("memory_request_bytes"),
                    service_name,
                    "memory_request_bytes",
                ),
            }
        return normalized

    @staticmethod
    def __validate_resource_value(value, service_name, field_name):
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or value < 0
        ):
            raise ValueError(
                f"{field_name} for service '{service_name}' must be a "
                "nonnegative integer."
            )
        return value

    def __read_node_latencies(self):
        latencies = {}
        maximum_inter_node_latency = 0.0

        for source_name, source_node in self.__nodes.items():
            raw_latencies = source_node.get_node_communication_latencies()
            if not isinstance(raw_latencies, Mapping):
                raise TypeError(
                    f"Communication latencies for node '{source_name}' "
                    "must be a mapping."
                )

            for destination_name in self.__node_names:
                if destination_name == source_name:
                    latency = raw_latencies.get(destination_name, 0.0)
                elif destination_name not in raw_latencies:
                    raise ValueError(
                        f"Node '{source_name}' has no communication latency "
                        f"to node '{destination_name}'."
                    )
                else:
                    latency = raw_latencies[destination_name]

                if (
                    isinstance(latency, bool)
                    or not isinstance(latency, (int, float))
                    or not math.isfinite(latency)
                    or latency < 0
                ):
                    raise ValueError(
                        f"Latency from '{source_name}' to "
                        f"'{destination_name}' must be a finite "
                        "nonnegative number."
                    )

                latency = float(latency)
                latencies[(source_name, destination_name)] = latency
                if destination_name != source_name:
                    maximum_inter_node_latency = max(
                        maximum_inter_node_latency,
                        latency,
                    )

        return latencies, maximum_inter_node_latency

    def __derive_traffic_rates(self):
        system_type = self.__system.get_type()
        system_type = getattr(system_type, "value", system_type)
        if system_type != "open":
            raise ValueError(
                "Fixed-replica node placement currently requires an open "
                "queueing network."
            )

        services_by_id = {}
        for service_name, service in self.__services.items():
            service_id = service.get_id()
            if (
                isinstance(service_id, bool)
                or not isinstance(service_id, int)
                or service_id < 0
                or service_id >= len(self.__services)
                or service_id in services_by_id
            ):
                raise ValueError(
                    "Services must have unique contiguous IDs starting at zero."
                )
            services_by_id[service_id] = service_name

        if len(services_by_id) != len(self.__services):
            raise ValueError(
                "Services must have unique contiguous IDs starting at zero."
            )

        try:
            arrival_values = tuple(
                self.__system.get_effective_arrival_rates()
            )
        except Exception as exc:
            raise ValueError(
                "The open-network traffic equations could not be solved."
            ) from exc

        if len(arrival_values) != len(self.__services):
            raise ValueError(
                "The traffic solution must contain one rate per service."
            )

        arrival_rates = {}
        for service_id, service_name in services_by_id.items():
            arrival_rate = arrival_values[service_id]
            if (
                isinstance(arrival_rate, bool)
                or not isinstance(arrival_rate, (int, float))
                or not math.isfinite(arrival_rate)
                or arrival_rate < -1e-12
            ):
                raise ValueError(
                    "The open-network traffic equations must have a finite "
                    "nonnegative solution."
                )
            arrival_rates[service_name] = max(0.0, float(arrival_rate))

        external_arrivals = {}
        total_external_arrival_rate = 0.0
        for service_name, service in self.__services.items():
            gamma = service.get_gamma()
            if (
                isinstance(gamma, bool)
                or not isinstance(gamma, (int, float))
                or not math.isfinite(gamma)
                or gamma < 0
            ):
                raise ValueError(
                    f"External arrival rate for '{service_name}' must be a "
                    "finite nonnegative number."
                )
            external_arrivals[service_name] = float(gamma)
            total_external_arrival_rate += float(gamma)

        edge_traffic_rates = {}
        incoming_traffic = {
            service_name: 0.0 for service_name in self.__service_names
        }

        for source_name, source_service in self.__services.items():
            for edge in source_service.get_adjacency_list():
                target_service = edge.get_target()
                if target_service is None:
                    continue

                target_name = target_service.get_name()
                if target_name not in self.__services:
                    raise ValueError(
                        f"Route from '{source_name}' targets unknown service "
                        f"'{target_name}'."
                    )

                route_value = edge.get_routing_prob()
                if (
                    isinstance(route_value, bool)
                    or not isinstance(route_value, (int, float))
                    or not math.isfinite(route_value)
                    or route_value < 0
                ):
                    raise ValueError(
                        f"Route value from '{source_name}' to "
                        f"'{target_name}' must be a finite nonnegative number."
                    )

                traffic_rate = (
                    arrival_rates[source_name] * float(route_value)
                )
                edge_key = (source_name, target_name)
                edge_traffic_rates[edge_key] = (
                    edge_traffic_rates.get(edge_key, 0.0) + traffic_rate
                )
                incoming_traffic[target_name] += traffic_rate

        for service_name in self.__service_names:
            expected_arrival_rate = (
                external_arrivals[service_name]
                + incoming_traffic[service_name]
            )
            if not math.isclose(
                arrival_rates[service_name],
                expected_arrival_rate,
                rel_tol=1e-9,
                abs_tol=1e-9,
            ):
                raise ValueError(
                    "The open-network traffic solution does not satisfy "
                    f"the traffic equation for service '{service_name}'."
                )

        return (
            arrival_rates,
            edge_traffic_rates,
            total_external_arrival_rate,
        )

    def __validate_configuration(self, configuration):
        placement = {
            node_name: {
                service_name: 0
                for service_name in self.__service_names
            }
            for node_name in self.__node_names
        }
        violations = []

        if not isinstance(configuration, Mapping):
            return placement, [
                "Placement configuration must be a node-to-service mapping."
            ]

        for node_name, service_counts in configuration.items():
            if node_name not in self.__nodes:
                violations.append(f"Unknown node '{node_name}'.")
                continue
            if not isinstance(service_counts, Mapping):
                violations.append(
                    f"Placement for node '{node_name}' must be a "
                    "service-to-count mapping."
                )
                continue

            for service_name, count in service_counts.items():
                if service_name not in self.__services:
                    violations.append(
                        f"Unknown service '{service_name}' on node "
                        f"'{node_name}'."
                    )
                    continue
                if (
                    isinstance(count, bool)
                    or not isinstance(count, int)
                    or count < 0
                ):
                    violations.append(
                        f"Replica count for service '{service_name}' on "
                        f"node '{node_name}' must be a nonnegative integer."
                    )
                    continue
                placement[node_name][service_name] = count

        for service_name, fixed_count in self.__fixed_replicas.items():
            placed_count = sum(
                placement[node_name][service_name]
                for node_name in self.__node_names
            )
            if placed_count != fixed_count:
                violations.append(
                    f"Service '{service_name}' must have exactly "
                    f"{fixed_count} replicas; placement has {placed_count}."
                )

        return placement, violations

    def __validate_resource_capacities(self, placement):
        violations = []

        for node_name, node in self.__nodes.items():
            cpu_used = sum(
                placement[node_name][service_name]
                * self.__resource_requests[service_name][
                    "cpu_request_millicores"
                ]
                for service_name in self.__service_names
            )
            memory_used = sum(
                placement[node_name][service_name]
                * self.__resource_requests[service_name][
                    "memory_request_bytes"
                ]
                for service_name in self.__service_names
            )

            allocatable_cpu = node.get_allocatable_cpu()
            allocatable_memory = node.get_allocatable_memory()
            self.__validate_node_capacity(
                node_name,
                "allocatable CPU",
                allocatable_cpu,
            )
            self.__validate_node_capacity(
                node_name,
                "allocatable memory",
                allocatable_memory,
            )

            if cpu_used > allocatable_cpu:
                violations.append(
                    f"Node '{node_name}' CPU request {cpu_used}m exceeds "
                    f"allocatable CPU {allocatable_cpu}m."
                )
            if memory_used > allocatable_memory:
                violations.append(
                    f"Node '{node_name}' memory request {memory_used} bytes "
                    f"exceeds allocatable memory {allocatable_memory} bytes."
                )

        return violations

    @staticmethod
    def __validate_node_capacity(node_name, resource_name, value):
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value < 0
        ):
            raise ValueError(
                f"Node '{node_name}' {resource_name} must be a finite "
                "nonnegative number."
            )

    def __calculate_network_cost(self, placement):
        network_cost = 0.0

        for (
            source_service,
            destination_service,
        ), traffic_rate in self.__edge_traffic_rates.items():
            source_replicas = self.__fixed_replicas[source_service]
            destination_replicas = self.__fixed_replicas[
                destination_service
            ]

            for source_node in self.__node_names:
                source_fraction = (
                    placement[source_node][source_service]
                    / source_replicas
                )
                if source_fraction == 0:
                    continue

                for destination_node in self.__node_names:
                    destination_fraction = (
                        placement[destination_node][destination_service]
                        / destination_replicas
                    )
                    if destination_fraction == 0:
                        continue

                    network_cost += (
                        traffic_rate
                        * source_fraction
                        * destination_fraction
                        * self.__node_latencies[
                            (source_node, destination_node)
                        ]
                    )

        return network_cost

    def __calculate_spreading_penalty(self, placement):
        service_penalties = []
        node_count = len(self.__node_names)

        for service_name, replica_count in self.__fixed_replicas.items():
            if replica_count == 1:
                continue

            concentration = sum(
                (
                    placement[node_name][service_name]
                    / replica_count
                )
                ** 2
                for node_name in self.__node_names
            )
            quotient, remainder = divmod(replica_count, node_count)
            minimum_concentration = (
                remainder * (quotient + 1) ** 2
                + (node_count - remainder) * quotient**2
            ) / replica_count**2
            normalization = 1.0 - minimum_concentration

            if normalization <= 1e-15:
                service_penalty = 0.0
            else:
                service_penalty = (
                    concentration - minimum_concentration
                ) / normalization
                service_penalty = min(1.0, max(0.0, service_penalty))

            service_penalties.append(service_penalty)

        if not service_penalties:
            return 0.0
        return sum(service_penalties) / len(service_penalties)

    def __canonical_configuration(self, placement):
        return tuple(
            (
                node_name,
                tuple(
                    (service_name, placement[node_name][service_name])
                    for service_name in self.__service_names
                    if placement[node_name][service_name] > 0
                ),
            )
            for node_name in self.__node_names
        )

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
            "average_network_delay_seconds": (self.average_network_delay_seconds),
            "replica_spreading_penalty": self.replica_spreading_penalty,
            "constraint_violations": list(self.constraint_violations),
            "resource_feasibility_checked": (self.resource_feasibility_checked),
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
        hard_constraint=None,
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
        if hard_constraint is not None and not callable(hard_constraint):
            raise TypeError("hard_constraint must be callable.")
        self.__hard_constraint = hard_constraint
        (
            self.__node_latencies,
            self.__maximum_inter_node_latency,
        ) = self.__read_node_latencies()
        (
            self.__service_arrival_rates,
            self.__edge_traffic_rates,
            self.__total_external_arrival_rate,
        ) = self.__derive_traffic_rates()
        self.__total_edge_traffic_rate = sum(self.__edge_traffic_rates.values())

    def evaluate(self, configuration):
        """Validate and calculate both placement objective components."""
        placement, violations = self.__validate_configuration(configuration)

        if self.__resource_requests is not None:
            violations.extend(self.__validate_resource_capacities(placement))
        complete_distributions = {
            service_name: tuple(
                placement[node_name][service_name] for node_name in self.__node_names
            )
            for service_name in self.__service_names
        }
        if not self.__hard_constraint_allows(complete_distributions, complete=True):
            violations.append("Placement violates the configured hard constraint.")

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
            self.__maximum_inter_node_latency * self.__total_edge_traffic_rate
        )
        network_cost_normalized = (
            network_cost_raw / network_denominator if network_denominator > 0 else 0.0
        )
        average_network_delay = (
            network_cost_raw / self.__total_external_arrival_rate
            if self.__total_external_arrival_rate > 0
            else 0.0
        )
        spreading_penalty = self.__calculate_spreading_penalty(placement)
        total_cost = (
            self.__cost_weights["network"] * network_cost_normalized
            + self.__cost_weights["replica_spreading"] * spreading_penalty
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

    def get_service_names(self):
        """Return the stable service order used by placement distributions."""
        return self.__service_names

    def get_node_names(self):
        """Return the stable node order used by placement distributions."""
        return self.__node_names

    def get_service_arrival_rates(self):
        return dict(self.__service_arrival_rates)

    def get_edge_traffic_rates(self):
        return dict(self.__edge_traffic_rates)

    def get_cost_weights(self):
        return dict(self.__cost_weights)

    def resource_feasibility_is_checked(self):
        return self.__resource_requests is not None

    def get_replica_items(self):
        """Return stable identities for identical replicas used by heuristics."""
        return tuple(
            (service_name, ordinal)
            for service_name in self.__service_names
            for ordinal in range(self.__fixed_replicas[service_name])
        )

    def get_replica_difficulty_scores(self):
        """Return deterministic construction scores; larger means harder."""
        incident = {name: 0.0 for name in self.__service_names}
        for (source, destination), rate in self.__edge_traffic_rates.items():
            incident[source] += rate
            if destination != source:
                incident[destination] += rate
        maximum_incident = max(incident.values(), default=0.0)
        scores = {}
        for service_name in self.__service_names:
            fit_count = len(self.__node_names)
            resource_pressure = 0.0
            if self.__resource_requests is not None:
                request = self.__resource_requests[service_name]
                fit_count = 0
                pressures = []
                for node_name, node in self.__nodes.items():
                    cpu = node.get_allocatable_cpu()
                    memory = node.get_allocatable_memory()
                    fits = (
                        request["cpu_request_millicores"] <= cpu
                        and request["memory_request_bytes"] <= memory
                    )
                    fit_count += int(fits)
                    if fits:
                        pressures.append(
                            max(
                                (
                                    request["cpu_request_millicores"] / cpu
                                    if cpu > 0
                                    else 0.0
                                ),
                                (
                                    request["memory_request_bytes"] / memory
                                    if memory > 0
                                    else 0.0
                                ),
                            )
                        )
                resource_pressure = max(pressures, default=math.inf)
            communication = (
                incident[service_name] / maximum_incident
                if maximum_incident > 0
                else 0.0
            )
            scarcity = len(self.__node_names) / fit_count if fit_count > 0 else math.inf
            spreading = float(self.__fixed_replicas[service_name] > 1)
            scores[service_name] = (
                resource_pressure + communication + scarcity + spreading
            )
        return scores

    def normalize_partial_replica_distributions(self, service_distributions):
        """Validate distributions that may contain fewer than all replicas."""
        if not isinstance(service_distributions, Mapping):
            raise TypeError("Service distributions must be a mapping.")
        unknown = set(service_distributions) - set(self.__service_names)
        if unknown:
            names = ", ".join(sorted(unknown))
            raise ValueError(f"Unknown services in partial placement: {names}.")
        normalized = {}
        node_count = len(self.__node_names)
        for service_name in self.__service_names:
            distribution = tuple(
                service_distributions.get(service_name, (0,) * node_count)
            )
            if (
                len(distribution) != node_count
                or any(
                    isinstance(value, bool) or not isinstance(value, int) or value < 0
                    for value in distribution
                )
                or sum(distribution) > self.__fixed_replicas[service_name]
            ):
                raise ValueError(
                    f"Partial distribution for '{service_name}' must contain "
                    f"{node_count} nonnegative integers with total at most "
                    f"{self.__fixed_replicas[service_name]}."
                )
            normalized[service_name] = distribution
        return normalized

    def partial_replica_assignment_is_feasible(self, service_distributions):
        distributions = self.normalize_partial_replica_distributions(
            service_distributions
        )
        if self.__resource_requests is not None:
            for node_index, (node_name, node) in enumerate(self.__nodes.items()):
                cpu = sum(
                    distribution[node_index]
                    * self.__resource_requests[service_name]["cpu_request_millicores"]
                    for service_name, distribution in distributions.items()
                )
                memory = sum(
                    distribution[node_index]
                    * self.__resource_requests[service_name]["memory_request_bytes"]
                    for service_name, distribution in distributions.items()
                )
                if (
                    cpu > node.get_allocatable_cpu()
                    or memory > node.get_allocatable_memory()
                ):
                    return False
        complete = all(
            sum(distributions[name]) == self.__fixed_replicas[name]
            for name in self.__service_names
        )
        return self.__hard_constraint_allows(distributions, complete=complete)

    def partial_replica_cost_estimate(self, service_distributions):
        """Score a partial replica placement without claiming an exact bound."""
        distributions = self.normalize_partial_replica_distributions(
            service_distributions
        )
        raw_network = sum(
            rate
            * self.__distribution_pair_latency(
                source,
                distributions[source],
                destination,
                distributions[destination],
            )
            for (source, destination), rate in self.__edge_traffic_rates.items()
        )
        denominator = self.__maximum_inter_node_latency * self.__total_edge_traffic_rate
        network = raw_network / denominator if denominator > 0 else 0.0
        spreading_terms = []
        for service_name, distribution in distributions.items():
            replica_count = self.__fixed_replicas[service_name]
            if replica_count <= 1:
                continue
            spreading_terms.append(
                sum((count / replica_count) ** 2 for count in distribution)
            )
        spreading = (
            sum(spreading_terms) / len(spreading_terms) if spreading_terms else 0.0
        )
        return (
            self.__cost_weights["network"] * network
            + self.__cost_weights["replica_spreading"] * spreading
        )

    def partial_resource_signature(self, service_distributions):
        """Return a coarse per-node load signature for beam diversity."""
        distributions = self.normalize_partial_replica_distributions(
            service_distributions
        )
        if self.__resource_requests is None:
            return tuple(
                sum(distribution[index] for distribution in distributions.values())
                for index in range(len(self.__node_names))
            )
        signature = []
        for index, (node_name, node) in enumerate(self.__nodes.items()):
            cpu = sum(
                distribution[index]
                * self.__resource_requests[service_name]["cpu_request_millicores"]
                for service_name, distribution in distributions.items()
            )
            memory = sum(
                distribution[index]
                * self.__resource_requests[service_name]["memory_request_bytes"]
                for service_name, distribution in distributions.items()
            )
            cpu_capacity = node.get_allocatable_cpu()
            memory_capacity = node.get_allocatable_memory()
            signature.append(
                (
                    round(cpu / cpu_capacity, 2) if cpu_capacity > 0 else 0.0,
                    round(memory / memory_capacity, 2) if memory_capacity > 0 else 0.0,
                )
            )
        return tuple(signature)

    def configuration_to_distributions(self, configuration):
        placement, violations = self.__validate_configuration(configuration)
        if violations:
            raise ValueError("; ".join(violations))
        return {
            service_name: tuple(
                placement[node_name][service_name] for node_name in self.__node_names
            )
            for service_name in self.__service_names
        }

    def evaluate_distributions(self, service_distributions):
        return self.evaluate(self.build_configuration(service_distributions))

    def placement_lower_bound(self):
        """Return the exact solver's valid empty-assignment relaxation."""
        return self.partial_cost_lower_bound({})

    def placement_change_delta(self, service_distributions, replacements):
        """Return exact objective delta for changed service distributions."""
        current = self.__validate_service_distributions(
            service_distributions,
            require_complete=True,
        )
        candidate = dict(current)
        candidate.update(replacements)
        candidate = self.__validate_service_distributions(
            candidate,
            require_complete=True,
        )
        if not self.partial_replica_assignment_is_feasible(candidate):
            return math.inf, candidate

        changed = set(replacements)
        raw_network_delta = 0.0
        for (source, destination), rate in self.__edge_traffic_rates.items():
            if source not in changed and destination not in changed:
                continue
            before = self.__distribution_pair_latency(
                source,
                current[source],
                destination,
                current[destination],
            )
            after = self.__distribution_pair_latency(
                source,
                candidate[source],
                destination,
                candidate[destination],
            )
            raw_network_delta += rate * (after - before)
        denominator = self.__maximum_inter_node_latency * self.__total_edge_traffic_rate
        network_delta = raw_network_delta / denominator if denominator > 0 else 0.0

        spreading_service_count = sum(
            value > 1 for value in self.__fixed_replicas.values()
        )
        spreading_delta = 0.0
        if spreading_service_count:
            spreading_delta = (
                sum(
                    self.__distribution_spreading_penalty(
                        service_name, candidate[service_name]
                    )
                    - self.__distribution_spreading_penalty(
                        service_name, current[service_name]
                    )
                    for service_name in changed
                    if self.__fixed_replicas[service_name] > 1
                )
                / spreading_service_count
            )
        delta = (
            self.__cost_weights["network"] * network_delta
            + self.__cost_weights["replica_spreading"] * spreading_delta
        )
        return delta, candidate

    def relocation_delta(
        self, service_distributions, service_name, source_node, destination_node
    ):
        node_index = {name: index for index, name in enumerate(self.__node_names)}
        source = node_index[source_node]
        destination = node_index[destination_node]
        distribution = list(service_distributions[service_name])
        if source == destination or distribution[source] <= 0:
            return math.inf, service_distributions
        distribution[source] -= 1
        distribution[destination] += 1
        return self.placement_change_delta(
            service_distributions,
            {service_name: tuple(distribution)},
        )

    def swap_delta(
        self,
        service_distributions,
        left_service,
        left_node,
        right_service,
        right_node,
    ):
        if left_node == right_node or left_service == right_service:
            return math.inf, service_distributions
        node_index = {name: index for index, name in enumerate(self.__node_names)}
        left_index = node_index[left_node]
        right_index = node_index[right_node]
        left = list(service_distributions[left_service])
        right = list(service_distributions[right_service])
        if left[left_index] <= 0 or right[right_index] <= 0:
            return math.inf, service_distributions
        left[left_index] -= 1
        left[right_index] += 1
        right[right_index] -= 1
        right[left_index] += 1
        return self.placement_change_delta(
            service_distributions,
            {
                left_service: tuple(left),
                right_service: tuple(right),
            },
        )

    def iter_replica_distribution_options(self, service_name):
        """Yield node distributions without materializing a large search space."""
        if service_name not in self.__fixed_replicas:
            raise KeyError(f"Unknown service '{service_name}'.")
        yield from self.__replica_distributions(
            self.__fixed_replicas[service_name],
            len(self.__node_names),
        )

    def get_replica_distribution_options(self, service_name):
        """Return every node distribution for exact placement search."""
        return tuple(self.iter_replica_distribution_options(service_name))

    def build_configuration(self, service_distributions):
        """Build a node-oriented configuration from all service distributions."""
        distributions = self.__validate_service_distributions(
            service_distributions,
            require_complete=True,
        )
        placement = {node_name: {} for node_name in self.__node_names}
        for service_name in self.__service_names:
            for node_name, replica_count in zip(
                self.__node_names,
                distributions[service_name],
            ):
                if replica_count:
                    placement[node_name][service_name] = replica_count
        return {
            node_name: service_counts
            for node_name, service_counts in placement.items()
            if service_counts
        }

    def partial_assignment_is_resource_feasible(self, service_distributions):
        """Return whether assigned services fit current node capacities.

        Unassigned services are deliberately ignored. Therefore ``False``
        proves that no completion of the partial assignment can be feasible.
        """
        distributions = self.__validate_service_distributions(
            service_distributions,
            require_complete=False,
        )
        if self.__resource_requests is not None:
            for node_index, (node_name, node) in enumerate(self.__nodes.items()):
                cpu_used = sum(
                    distribution[node_index]
                    * self.__resource_requests[service_name]["cpu_request_millicores"]
                    for service_name, distribution in distributions.items()
                )
                memory_used = sum(
                    distribution[node_index]
                    * self.__resource_requests[service_name]["memory_request_bytes"]
                    for service_name, distribution in distributions.items()
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
                if cpu_used > allocatable_cpu or memory_used > allocatable_memory:
                    return False
        complete = len(distributions) == len(self.__service_names)
        return self.__hard_constraint_allows(distributions, complete=complete)

    def partial_cost_lower_bound(self, service_distributions):
        """Return an admissible objective bound for a partial placement.

        Exact terms are used whenever their services are assigned. For a
        communication edge with one unassigned endpoint, that endpoint is
        allowed to choose its best node without capacity or spreading costs.
        Relaxing those constraints can only reduce the objective, so the
        result is safe for exact branch-and-bound pruning.
        """
        distributions = self.__validate_service_distributions(
            service_distributions,
            require_complete=False,
        )
        network_cost_lower_bound = 0.0
        global_minimum_latency = min(self.__node_latencies.values())

        for (
            source_service,
            destination_service,
        ), traffic_rate in self.__edge_traffic_rates.items():
            source_distribution = distributions.get(source_service)
            destination_distribution = distributions.get(destination_service)

            if source_distribution is not None and destination_distribution is not None:
                edge_latency = self.__distribution_pair_latency(
                    source_service,
                    source_distribution,
                    destination_service,
                    destination_distribution,
                )
            elif source_distribution is not None:
                source_replicas = self.__fixed_replicas[source_service]
                edge_latency = min(
                    sum(
                        source_distribution[source_index]
                        / source_replicas
                        * self.__node_latencies[(source_node, destination_node)]
                        for source_index, source_node in enumerate(self.__node_names)
                    )
                    for destination_node in self.__node_names
                )
            elif destination_distribution is not None:
                destination_replicas = self.__fixed_replicas[destination_service]
                edge_latency = min(
                    sum(
                        destination_distribution[destination_index]
                        / destination_replicas
                        * self.__node_latencies[(source_node, destination_node)]
                        for destination_index, destination_node in enumerate(
                            self.__node_names
                        )
                    )
                    for source_node in self.__node_names
                )
            else:
                edge_latency = global_minimum_latency

            network_cost_lower_bound += traffic_rate * edge_latency

        network_denominator = (
            self.__maximum_inter_node_latency * self.__total_edge_traffic_rate
        )
        normalized_network_lower_bound = (
            network_cost_lower_bound / network_denominator
            if network_denominator > 0
            else 0.0
        )

        spreading_service_count = sum(
            replica_count > 1 for replica_count in self.__fixed_replicas.values()
        )
        spreading_lower_bound = 0.0
        if spreading_service_count:
            spreading_lower_bound = (
                sum(
                    self.__distribution_spreading_penalty(
                        service_name,
                        distribution,
                    )
                    for service_name, distribution in distributions.items()
                    if self.__fixed_replicas[service_name] > 1
                )
                / spreading_service_count
            )

        return (
            self.__cost_weights["network"] * normalized_network_lower_bound
            + self.__cost_weights["replica_spreading"] * spreading_lower_bound
        )

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
            tuple(self.__replica_distributions(replica_count, node_count))
            for replica_count in self.__fixed_replicas.values()
        )

        for service_distributions in product(*distribution_options):
            placement = {node_name: {} for node_name in self.__node_names}

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
                details.append("missing: " + ", ".join(sorted(missing_services)))
            if unknown_services:
                details.append("unknown: " + ", ".join(sorted(unknown_services)))
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
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
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
            arrival_values = tuple(self.__system.get_effective_arrival_rates())
        except Exception as exc:
            raise ValueError(
                "The open-network traffic equations could not be solved."
            ) from exc

        if len(arrival_values) != len(self.__services):
            raise ValueError("The traffic solution must contain one rate per service.")

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

        request_classes = tuple(self.__system.get_request_classes())
        request_class_arrival_rates = (
            self.__system.get_request_class_effective_arrival_rates()
        )
        if set(request_class_arrival_rates) != set(request_classes):
            raise ValueError(
                "The class traffic solution must define every request class."
            )
        for request_class in request_classes:
            class_rates = tuple(request_class_arrival_rates[request_class])
            if len(class_rates) != len(self.__services) or any(
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value < -1e-12
                for value in class_rates
            ):
                raise ValueError(
                    "Each request-class traffic solution must contain one "
                    "finite nonnegative rate per service."
                )

        external_arrivals = {}
        total_external_arrival_rate = 0.0
        for service_name, service in self.__services.items():
            gamma = sum(
                service.get_gamma(request_class) for request_class in request_classes
            )
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
        incoming_traffic = {service_name: 0.0 for service_name in self.__service_names}
        call_graphs = (
            self.__system.get_request_class_call_graphs()
            if hasattr(self.__system, "get_request_class_call_graphs")
            else {}
        )
        visit_ratios = (
            self.__system.get_request_class_visit_ratios()
            if hasattr(self.__system, "get_request_class_visit_ratios")
            else {}
        )
        all_flows_decomposable = True

        for request_class in request_classes:
            class_rates = request_class_arrival_rates[request_class]
            call_graph = call_graphs.get(request_class)
            if call_graph is not None:
                for source_name, targets in call_graph.items():
                    source_rate = float(
                        class_rates[self.__services[source_name].get_id()]
                    )
                    for target_name, mean_calls in targets.items():
                        traffic_rate = source_rate * float(mean_calls)
                        edge_key = (source_name, target_name)
                        edge_traffic_rates[edge_key] = (
                            edge_traffic_rates.get(edge_key, 0.0) + traffic_rate
                        )
                        incoming_traffic[target_name] += traffic_rate
                continue

            if request_class in visit_ratios:
                # Mean visits identify per-service load but do not identify
                # which source produced each visit. Keep placement feasible and
                # omit an unsupported communication cost instead of inventing
                # service edges.
                all_flows_decomposable = False
                continue

            for source_name, source_service in self.__services.items():
                source_rate = float(class_rates[source_service.get_id()])
                for edge in source_service.get_adjacency_list(request_class):
                    target_service = edge.get_target()
                    if target_service is None:
                        continue

                    target_name = target_service.get_name()
                    if target_name not in self.__services:
                        raise ValueError(
                            f"Route from '{source_name}' targets unknown "
                            f"service '{target_name}'."
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
                            f"'{target_name}' must be a finite nonnegative "
                            "number."
                        )

                    traffic_rate = source_rate * float(route_value)
                    edge_key = (source_name, target_name)
                    edge_traffic_rates[edge_key] = (
                        edge_traffic_rates.get(edge_key, 0.0) + traffic_rate
                    )
                    incoming_traffic[target_name] += traffic_rate

        if all_flows_decomposable:
            for service_name in self.__service_names:
                expected_arrival_rate = (
                    external_arrivals[service_name] + incoming_traffic[service_name]
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

    def __hard_constraint_allows(self, distributions, *, complete):
        if self.__hard_constraint is None:
            return True
        node_count = len(self.__node_names)
        distributions = {
            service_name: tuple(distributions.get(service_name, (0,) * node_count))
            for service_name in self.__service_names
        }
        placement = {
            node_name: {
                service_name: distributions[service_name][node_index]
                for service_name in self.__service_names
                if distributions[service_name][node_index] > 0
            }
            for node_index, node_name in enumerate(self.__node_names)
        }
        try:
            return bool(self.__hard_constraint(placement, complete=complete))
        except TypeError:
            return bool(self.__hard_constraint(placement))

    def __validate_configuration(self, configuration):
        placement = {
            node_name: {service_name: 0 for service_name in self.__service_names}
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
                        f"Unknown service '{service_name}' on node " f"'{node_name}'."
                    )
                    continue
                if isinstance(count, bool) or not isinstance(count, int) or count < 0:
                    violations.append(
                        f"Replica count for service '{service_name}' on "
                        f"node '{node_name}' must be a nonnegative integer."
                    )
                    continue
                placement[node_name][service_name] = count

        for service_name, fixed_count in self.__fixed_replicas.items():
            placed_count = sum(
                placement[node_name][service_name] for node_name in self.__node_names
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
                * self.__resource_requests[service_name]["cpu_request_millicores"]
                for service_name in self.__service_names
            )
            memory_used = sum(
                placement[node_name][service_name]
                * self.__resource_requests[service_name]["memory_request_bytes"]
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
            destination_replicas = self.__fixed_replicas[destination_service]

            for source_node in self.__node_names:
                source_fraction = (
                    placement[source_node][source_service] / source_replicas
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
                        * self.__node_latencies[(source_node, destination_node)]
                    )

        return network_cost

    def __distribution_pair_latency(
        self,
        source_service,
        source_distribution,
        destination_service,
        destination_distribution,
    ):
        source_replicas = self.__fixed_replicas[source_service]
        destination_replicas = self.__fixed_replicas[destination_service]
        return sum(
            source_distribution[source_index]
            / source_replicas
            * destination_distribution[destination_index]
            / destination_replicas
            * self.__node_latencies[(source_node, destination_node)]
            for source_index, source_node in enumerate(self.__node_names)
            for destination_index, destination_node in enumerate(self.__node_names)
        )

    def __calculate_spreading_penalty(self, placement):
        service_penalties = [
            self.__distribution_spreading_penalty(
                service_name,
                tuple(
                    placement[node_name][service_name]
                    for node_name in self.__node_names
                ),
            )
            for service_name, replica_count in self.__fixed_replicas.items()
            if replica_count > 1
        ]
        if not service_penalties:
            return 0.0
        return sum(service_penalties) / len(service_penalties)

    def __distribution_spreading_penalty(
        self,
        service_name,
        distribution,
    ):
        replica_count = self.__fixed_replicas[service_name]
        if replica_count == 1:
            return 0.0

        node_count = len(self.__node_names)
        concentration = sum(
            (replicas_on_node / replica_count) ** 2 for replicas_on_node in distribution
        )
        quotient, remainder = divmod(replica_count, node_count)
        minimum_concentration = (
            remainder * (quotient + 1) ** 2 + (node_count - remainder) * quotient**2
        ) / replica_count**2
        normalization = 1.0 - minimum_concentration
        if normalization <= 1e-15:
            return 0.0
        penalty = (concentration - minimum_concentration) / normalization
        return min(1.0, max(0.0, penalty))

    def __validate_service_distributions(
        self,
        service_distributions,
        *,
        require_complete,
    ):
        if not isinstance(service_distributions, Mapping):
            raise TypeError("Service distributions must be a mapping.")

        unknown_services = set(service_distributions) - set(self.__service_names)
        if unknown_services:
            names = ", ".join(sorted(unknown_services))
            raise ValueError(f"Unknown services in placement search: {names}.")
        if require_complete:
            missing_services = set(self.__service_names) - set(service_distributions)
            if missing_services:
                names = ", ".join(sorted(missing_services))
                raise ValueError(f"Missing services in complete placement: {names}.")

        normalized = {}
        node_count = len(self.__node_names)
        for service_name, raw_distribution in service_distributions.items():
            distribution = tuple(raw_distribution)
            if (
                len(distribution) != node_count
                or any(
                    isinstance(value, bool) or not isinstance(value, int) or value < 0
                    for value in distribution
                )
                or sum(distribution) != self.__fixed_replicas[service_name]
            ):
                raise ValueError(
                    f"Distribution for service '{service_name}' must contain "
                    f"{node_count} nonnegative integers summing to "
                    f"{self.__fixed_replicas[service_name]}."
                )
            normalized[service_name] = distribution
        return normalized

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

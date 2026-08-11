import copy
import math
from enum import Enum
from .service import Service
from metrics import Metrics
from native_backend import get_native_backend

import random

import numpy as np


class QueueingNetworkType(str, Enum):
    OPEN = "open"
    CLOSED = "closed"
    HYBRID = "hybrid"


class System:

    def __init__(self, name, workload, type: QueueingNetworkType):
        self.__name = name
        self.__workload_spec = copy.deepcopy(workload)
        # type refers to either open or closed system
        self.__type = type
        self.__services = {}
        self.__external_traffic_services = []

        if not workload:
            raise ValueError("At least one request class is required.")
        self.__request_classes = tuple(workload.keys())

        # Build the union of services before registering routes, so targets can
        # be resolved regardless of declaration order or request class.
        for request_class in workload.values():
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

        for request_class_name, request_class in workload.items():
            for service_name, service_data in request_class.items():
                service = self.get_service(service_name)
                service.add_gamma(
                    request_class_name,
                    service_data.get("gamma", 0),
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

    def __mmc_latency(self, service_lambda, per_replica_mu, replicas):
        if per_replica_mu <= 0 or replicas <= 0:
            return float("inf")

        if service_lambda == 0:
            return 1 / per_replica_mu

        rho = service_lambda / (replicas * per_replica_mu)

        if rho >= 1:
            return float("inf")

        a = service_lambda / per_replica_mu

        sum_terms = 0.0
        for n in range(replicas):
            sum_terms += (a**n) / math.factorial(n)

        last_term = ((a**replicas) / math.factorial(replicas)) * (1 / (1 - rho))

        p0 = 1 / (sum_terms + last_term)

        p_wait = last_term * p0

        wq = p_wait / (replicas * per_replica_mu - service_lambda)

        response_time = (1 / per_replica_mu) + wq

        return response_time

    def solve(self):
        request_class_rates = self.__calculate_request_class_arrival_rates()
        lambdas = self.__calculate_aggregate_arrival_rates(request_class_rates)

        num_of_services = len(self.get_services())
        system_input = self.__initialize_system_input(self.__request_classes[0])
        mus = np.array(system_input["mus"], dtype=float)
        replicas = np.array(system_input["replicas"], dtype=int)

        capacities = replicas * mus
        utilizations = lambdas / capacities

        lambdas = lambdas.tolist()
        mus = mus.tolist()
        replicas = replicas.tolist()
        utilizations = utilizations.tolist()

        latencies = [
            self.__mmc_latency(lambdas[i], mus[i], replicas[i])
            for i in range(num_of_services)
        ]

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
                and metrics_collector.get_latency_collection_start()
                != effective_warmup
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

    def get_workload_spec(self):
        """Return an isolated copy suitable for constructing another System."""
        return copy.deepcopy(self.__workload_spec)

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

    def configure_system_replicas(self, replicas_config):
        for service in self.get_services().values():
            service_id = service.get_id()
            service.set_replicas(replicas_config[service_id])

    def add_service(self, name, component):
        self.__services[name] = component

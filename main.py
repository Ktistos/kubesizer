import random

import yaml

from optimization import AStarOptimizer, ReplicaSizingProblem
from performance_models import QueueingNetworkType, System


SIMULATION_DURATION = 300
REQUEST_CLASS_SLOS = {"api": 1.0}  # End-to-end p95 limits, in seconds.
MAX_REPLICAS_PER_SERVICE = 10


def main():
    random.seed(42)

    with open("workload.yaml", "r", encoding="utf-8") as file:
        workload = yaml.safe_load(file)

    system = System("STELAR", workload, QueueingNetworkType.OPEN)
    problem = ReplicaSizingProblem(
        system=system,
        simulation_duration=SIMULATION_DURATION,
        request_class_slos=REQUEST_CLASS_SLOS,
        max_replicas=MAX_REPLICAS_PER_SERVICE,
    )
    result = AStarOptimizer().optimize(problem)

    # The search evaluates many configurations, so explicitly apply its result.
    system.configure_system_replicas(result.configuration)

    evaluation = result.metrics
    service_names = tuple(system.get_services())
    replicas = dict(zip(service_names, result.configuration))
    utilizations = dict(
        zip(
            service_names,
            evaluation.analytical_metrics.get_mean_utilizations(),
        )
    )

    print("Replica configuration:", replicas)
    print("Total replicas:", evaluation.cost)
    print("Replicas added from the initial state:", result.cost)
    print(
        "Request-class p95 latencies:",
        evaluation.simulation_metrics.get_request_class_p95_latencies(),
    )
    print("Worst normalized p95 ratio:", evaluation.tie_breaker)
    print("Analytical utilizations:", utilizations)
    print("Explored states:", result.explored_states)
    print("Search path:", result.path)

if __name__ == "__main__":
    main()

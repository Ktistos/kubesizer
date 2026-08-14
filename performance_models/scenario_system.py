import copy
from dataclasses import dataclass
from types import MappingProxyType

from metrics import Metrics

from .system import System
from .workload import parse_workload_spec


@dataclass(frozen=True)
class ScenarioAnalyticalResult:
    """Per-scenario evidence plus the conservative all-scenario summary."""

    scenario_metrics: MappingProxyType
    robust_metrics: Metrics
    latency_models: MappingProxyType
    queueing_models: MappingProxyType


class ScenarioSystem:
    """Analytical view of all required workload scenarios.

    A replica configuration is analytically stable only when it is stable in
    every declared scenario. Scenario results remain individually available;
    ``solve`` returns componentwise worst utilization and mean-latency values
    solely for compatibility with analytical optimizer ranking.

    This class intentionally has no single-scenario simulation method. The
    statistical workflow explicitly runs every contained ``System`` and
    requires all scenario/class confidence bounds to pass.
    """

    def __init__(
        self,
        name,
        workload,
        type,
        *,
        workload_base_path=None,
    ):
        parsed = parse_workload_spec(workload, base_path=workload_base_path)
        self.__name = name
        self.__type = type
        self.__workload_spec = copy.deepcopy(workload)
        self.__workload_base_path = workload_base_path
        self.__systems = {
            scenario_name: System(
                f"{name}:{scenario_name}",
                workload,
                type,
                scenario_name=scenario_name,
                workload_base_path=workload_base_path,
            )
            for scenario_name in parsed.scenarios
        }
        self.__scenario_names = tuple(self.__systems)
        self.__representative = self.__systems[self.__scenario_names[0]]
        expected_services = tuple(self.__representative.get_services())
        expected_classes = self.__representative.get_request_classes()
        for scenario_name, system in self.__systems.items():
            if tuple(system.get_services()) != expected_services:
                raise ValueError(
                    f"Scenario {scenario_name!r} changes service identities or order."
                )
            if system.get_request_classes() != expected_classes:
                raise ValueError(
                    f"Scenario {scenario_name!r} changes request-class identities."
                )

    def get_name(self):
        return self.__name

    def get_type(self):
        return self.__type

    def get_workload_spec(self):
        return copy.deepcopy(self.__workload_spec)

    def get_workload_base_path(self):
        return self.__workload_base_path

    def get_scenario_names(self):
        return self.__scenario_names

    def get_scenario_system(self, scenario_name):
        try:
            return self.__systems[scenario_name]
        except KeyError as error:
            choices = ", ".join(self.__scenario_names)
            raise ValueError(
                f"Unknown scenario {scenario_name!r}; expected one of: {choices}."
            ) from error

    def get_scenario_systems(self):
        return dict(self.__systems)

    def get_services(self):
        """Return representative services in the shared stable ID order."""
        return self.__representative.get_services()

    def get_request_classes(self):
        return self.__representative.get_request_classes()

    def get_system_replicas(self):
        return self.__representative.get_system_replicas()

    def configure_system_replicas(self, replicas_config):
        replicas = tuple(replicas_config)
        for system in self.__systems.values():
            system.configure_system_replicas(replicas)

    def get_effective_arrival_rates_by_scenario(self):
        return {
            name: tuple(system.get_effective_arrival_rates())
            for name, system in self.__systems.items()
        }

    def get_request_class_effective_arrival_rates_by_scenario(self):
        return {
            name: system.get_request_class_effective_arrival_rates()
            for name, system in self.__systems.items()
        }

    def get_service_rates_by_scenario(self):
        return {
            name: tuple(
                service.get_mu() for service in system.get_services().values()
            )
            for name, system in self.__systems.items()
        }

    def get_effective_arrival_scvs_by_scenario(self):
        return {
            name: tuple(system.get_effective_arrival_scvs())
            for name, system in self.__systems.items()
        }

    def get_service_time_scvs_by_scenario(self):
        return {
            name: tuple(system.get_service_time_scvs())
            for name, system in self.__systems.items()
        }

    def get_analytical_queueing_models_by_scenario(self):
        return {
            name: system.get_analytical_queueing_models()
            for name, system in self.__systems.items()
        }

    def get_analytical_latency_models(self):
        """Describe whether each scenario's M/M/c latency is exact or a proxy."""
        return {
            name: system.get_analytical_latency_model()
            for name, system in self.__systems.items()
        }

    def get_request_class_mean_latencies_by_scenario(self):
        """Return analytical end-to-end mean latency for every scenario/class."""
        return {
            name: system.get_request_class_mean_latencies()
            for name, system in self.__systems.items()
        }

    def get_request_class_mean_latencies(self):
        """Return the conservative maximum analytical mean across scenarios."""
        values = self.get_request_class_mean_latencies_by_scenario()
        return {
            request_class: max(
                scenario_values[request_class]
                for scenario_values in values.values()
            )
            for request_class in self.get_request_classes()
        }

    def get_minimum_stable_replicas_by_scenario(self):
        return {
            name: tuple(system.get_minimum_stable_replicas())
            for name, system in self.__systems.items()
        }

    def get_minimum_stable_replicas(self):
        """Return the componentwise minimum satisfying every scenario."""
        scenario_minima = tuple(
            self.get_minimum_stable_replicas_by_scenario().values()
        )
        return [max(values) for values in zip(*scenario_minima)]

    def solve_scenarios(self):
        return {
            name: system.solve()
            for name, system in self.__systems.items()
        }

    def analyze(self):
        scenario_metrics = self.solve_scenarios()
        service_count = len(self.get_services())
        robust_utilizations = [
            max(
                metrics.get_mean_utilizations()[service_id]
                for metrics in scenario_metrics.values()
            )
            for service_id in range(service_count)
        ]
        robust_latencies = [
            max(
                metrics.get_mean_latencies()[service_id]
                for metrics in scenario_metrics.values()
            )
            for service_id in range(service_count)
        ]
        robust = Metrics(
            robust_utilizations,
            robust_latencies,
            all(metrics.get_stability() for metrics in scenario_metrics.values()),
        )
        return ScenarioAnalyticalResult(
            scenario_metrics=MappingProxyType(scenario_metrics),
            robust_metrics=robust,
            latency_models=MappingProxyType(self.get_analytical_latency_models()),
            queueing_models=MappingProxyType(
                self.get_analytical_queueing_models_by_scenario()
            ),
        )

    def solve(self):
        return self.analyze().robust_metrics

    def get_effective_arrival_rates(self):
        raise ValueError(
            "A multi-scenario workload has no single effective-arrival vector; "
            "use get_effective_arrival_rates_by_scenario()."
        )

    def get_request_class_effective_arrival_rates(self):
        raise ValueError(
            "A multi-scenario workload has no single request-class traffic "
            "solution; use get_request_class_effective_arrival_rates_by_scenario()."
        )

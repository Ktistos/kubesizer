import math


class Metrics:
    def __init__(
        self,
        mean_utilizations,
        mean_latencies,
        stability,
        p95_latencies=None,
        p99_latencies=None,
    ):
        self.__mean_utilizations = list(mean_utilizations)
        self.__mean_latencies = list(mean_latencies)
        self.__stability = stability
        self.__p95_latencies = list(p95_latencies or [])
        self.__p99_latencies = list(p99_latencies or [])

        service_count = len(self.__mean_utilizations)
        self.__response_times = [[] for _ in range(service_count)]
        self.__busy_counts = [0 for _ in range(service_count)]
        self.__busy_areas = [0.0 for _ in range(service_count)]
        self.__last_busy_updates = [0.0 for _ in range(service_count)]

    @classmethod
    def for_simulation(cls, service_count):
        empty_metrics = [0.0 for _ in range(service_count)]
        return cls(
            empty_metrics,
            empty_metrics,
            stability=False,
            p95_latencies=empty_metrics,
            p99_latencies=empty_metrics,
        )

    @staticmethod
    def __percentile(values, percentile):
        if not values:
            return 0.0

        sorted_values = sorted(values)
        position = (len(sorted_values) - 1) * percentile / 100
        lower = int(position)
        upper = min(lower + 1, len(sorted_values) - 1)
        weight = position - lower
        return sorted_values[lower] * (1 - weight) + sorted_values[upper] * weight

    def __update_busy_area(self, service_id, timestamp):
        elapsed = timestamp - self.__last_busy_updates[service_id]
        self.__busy_areas[service_id] += self.__busy_counts[service_id] * elapsed
        self.__last_busy_updates[service_id] = timestamp

    def record_service_start(self, service_id, timestamp):
        self.__update_busy_area(service_id, timestamp)
        self.__busy_counts[service_id] += 1

    def record_request_completion(self, service_id, response_time, timestamp):
        self.__update_busy_area(service_id, timestamp)
        self.__busy_counts[service_id] -= 1
        self.__response_times[service_id].append(response_time)

    def finalize_simulation(self, simulation_duration, service_capacities, stability):
        for service_id, capacity in enumerate(service_capacities):
            self.__update_busy_area(service_id, simulation_duration)

            response_times = self.__response_times[service_id]
            self.__mean_utilizations[service_id] = (
                self.__busy_areas[service_id] / (capacity * simulation_duration)
                if capacity > 0 and simulation_duration > 0
                else 0.0
            )
            self.__mean_latencies[service_id] = (
                sum(response_times) / len(response_times) if response_times else 0.0
            )
            self.__p95_latencies[service_id] = self.__percentile(response_times, 95)
            self.__p99_latencies[service_id] = self.__percentile(response_times, 99)

        self.__stability = stability

    def is_nearly_equal(
        self,
        other,
        relative_tolerance=0.05,
        absolute_tolerance=1e-9,
    ):
        """Compare mean utilizations and latencies within the given tolerances."""
        if not isinstance(other, Metrics):
            return False
        if relative_tolerance < 0 or absolute_tolerance < 0:
            raise ValueError("Tolerances must be non-negative.")

        metric_pairs = (
            (self.__mean_utilizations, other.__mean_utilizations),
            (self.__mean_latencies, other.__mean_latencies),
        )

        for own_values, other_values in metric_pairs:
            if len(own_values) != len(other_values):
                return False
            if not all(
                math.isclose(
                    own_value,
                    other_value,
                    rel_tol=relative_tolerance,
                    abs_tol=absolute_tolerance,
                )
                for own_value, other_value in zip(own_values, other_values)
            ):
                return False

        return True

    def get_mean_utilizations(self):
        return self.__mean_utilizations
    
    def get_mean_latencies(self):
        return self.__mean_latencies
    
    def get_stability(self):
        return self.__stability
        
    def get_p95_latencies(self):
        return self.__p95_latencies
    
    def get_p99_latencies(self):
        return self.__p99_latencies

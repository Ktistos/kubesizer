from dataclasses import dataclass

from .metrics import Metrics


@dataclass(frozen=True)
class ExternalRequestContext:
    request_class: str
    entry_time: float


class SimulationMetricsCollector:
    def __init__(self, service_count):
        if service_count < 0:
            raise ValueError("service_count must be non-negative.")

        self.__service_count = service_count
        self.__response_times = [[] for _ in range(service_count)]
        self.__busy_counts = [0 for _ in range(service_count)]
        self.__busy_areas = [0.0 for _ in range(service_count)]
        self.__last_busy_updates = [0.0 for _ in range(service_count)]
        self.__request_class_response_times = {}

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

    def start_external_request(self, request_class, entry_time):
        return ExternalRequestContext(request_class, entry_time)

    def record_external_request_completion(self, request_context, completion_time):
        response_time = completion_time - request_context.entry_time
        self.__request_class_response_times.setdefault(
            request_context.request_class,
            [],
        ).append(response_time)

    def finalize(self, simulation_duration, service_capacities, stability):
        if len(service_capacities) != self.__service_count:
            raise ValueError("A capacity is required for every service.")

        mean_utilizations = [0.0 for _ in range(self.__service_count)]
        mean_latencies = [0.0 for _ in range(self.__service_count)]
        p95_latencies = [0.0 for _ in range(self.__service_count)]
        p99_latencies = [0.0 for _ in range(self.__service_count)]
        request_class_p95_latencies = {
            request_class: self.__percentile(response_times, 95)
            for request_class, response_times in self.__request_class_response_times.items()
        }

        for service_id, capacity in enumerate(service_capacities):
            self.__update_busy_area(service_id, simulation_duration)

            response_times = self.__response_times[service_id]
            if capacity > 0 and simulation_duration > 0:
                mean_utilizations[service_id] = self.__busy_areas[service_id] / (
                    capacity * simulation_duration
                )
            if response_times:
                mean_latencies[service_id] = sum(response_times) / len(response_times)

            p95_latencies[service_id] = self.__percentile(response_times, 95)
            p99_latencies[service_id] = self.__percentile(response_times, 99)

        return Metrics(
            mean_utilizations,
            mean_latencies,
            stability,
            p95_latencies,
            p99_latencies,
            request_class_p95_latencies,
        )

import math
from dataclasses import dataclass

from .metrics import Metrics


@dataclass(frozen=True)
class ExternalRequestContext:
    request_class: str
    entry_time: float
    eligible_for_latency_collection: bool


class SimulationMetricsCollector:
    def __init__(self, service_count, latency_collection_start=0.0):
        if service_count < 0:
            raise ValueError("service_count must be non-negative.")
        latency_collection_start = float(latency_collection_start)
        if not math.isfinite(latency_collection_start) or latency_collection_start < 0:
            raise ValueError(
                "latency_collection_start must be finite and non-negative."
            )

        self.__service_count = service_count
        self.__latency_collection_start = latency_collection_start
        self.__response_times = [[] for _ in range(service_count)]
        self.__busy_areas = [0.0 for _ in range(service_count)]
        self.__last_busy_updates = [0.0 for _ in range(service_count)]
        self.__request_class_response_times = {}
        self.__active_requests = 0
        self.__active_history = [(0.0, 0)]
        self.__queue_history = {
            service_id: [(0.0, 0)] for service_id in range(service_count)
        }
        self.__busy_history = {
            service_id: [(0.0, 0)] for service_id in range(service_count)
        }
        self.__service_resources = {}
        self.__native_final_queue_lengths = None
        self.__native_final_busy_replicas = None
        self.__native_capacities = None

    @staticmethod
    def __record_change(history, timestamp, value):
        timestamp = float(timestamp)
        value = int(value)
        previous_time, previous_value = history[-1]

        if timestamp < previous_time:
            raise ValueError("State history timestamps must not move backwards.")
        if timestamp == previous_time:
            if value == previous_value:
                return
            history[-1] = (timestamp, value)
            if len(history) > 1 and history[-2][1] == value:
                history.pop()
            return
        if value != previous_value:
            history.append((timestamp, value))

    def __validate_service_id(self, service_id):
        if not isinstance(service_id, int) or not 0 <= service_id < self.__service_count:
            raise ValueError("service_id is outside the collector's service range.")

    def __get_service_resource(self, service_id):
        self.__validate_service_id(service_id)
        resource = self.__service_resources.get(service_id)
        if resource is None:
            raise ValueError(f"No simulation resource is registered for service {service_id}.")
        return resource

    def register_service_resource(self, service_id, resource):
        """Register the authoritative SimPy resource for one service."""
        self.__validate_service_id(service_id)
        if resource.capacity <= 0:
            raise ValueError("Service resource capacity must be greater than zero.")
        if resource.count != 0 or len(resource.queue) != 0:
            raise ValueError("Service resources must be registered before receiving requests.")

        existing_resource = self.__service_resources.get(service_id)
        if existing_resource is not None and existing_resource is not resource:
            raise ValueError(f"A resource is already registered for service {service_id}.")
        self.__service_resources[service_id] = resource

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
        if elapsed < 0:
            raise ValueError("Busy-state timestamps must not move backwards.")
        current_busy = self.__busy_history[service_id][-1][1]
        self.__busy_areas[service_id] += current_busy * elapsed
        self.__last_busy_updates[service_id] = timestamp

    def record_service_state(self, service_id, timestamp):
        """Capture queue and busy state after a SimPy resource transition."""
        resource = self.__get_service_resource(service_id)
        self.__update_busy_area(service_id, timestamp)
        self.__record_change(
            self.__queue_history[service_id],
            timestamp,
            len(resource.queue),
        )
        self.__record_change(
            self.__busy_history[service_id],
            timestamp,
            resource.count,
        )

    def record_service_start(self, service_id, timestamp):
        self.__update_busy_area(service_id, timestamp)

    def record_request_completion(self, service_id, response_time, timestamp):
        self.__update_busy_area(service_id, timestamp)
        self.__response_times[service_id].append(response_time)

    def start_external_request(self, request_class, entry_time):
        self.__active_requests += 1
        self.__record_change(
            self.__active_history,
            entry_time,
            self.__active_requests,
        )
        return ExternalRequestContext(
            request_class,
            entry_time,
            entry_time >= self.__latency_collection_start,
        )

    def record_external_request_completion(self, request_context, completion_time):
        assert self.__active_requests > 0, "active_requests cannot become negative."
        response_time = completion_time - request_context.entry_time
        if request_context.eligible_for_latency_collection:
            self.__request_class_response_times.setdefault(
                request_context.request_class,
                [],
            ).append(response_time)
        self.__active_requests -= 1
        assert self.__active_requests >= 0, "active_requests cannot become negative."
        self.__record_change(
            self.__active_history,
            completion_time,
            self.__active_requests,
        )

    def load_native_state(self, result, service_capacities):
        """Load state captured by the mandatory C++ execution backend."""
        if self.__service_resources:
            raise ValueError("Cannot load native state after registering SimPy resources.")
        active_history = result.get("active_history")
        queue_history = result.get("queue_history")
        busy_history = result.get("busy_history")
        if active_history is None or queue_history is None or busy_history is None:
            raise ValueError("The native result does not contain state histories.")
        if len(queue_history) != self.__service_count or len(busy_history) != self.__service_count:
            raise ValueError("Native state history service count does not match collector.")
        capacities = tuple(int(value) for value in service_capacities)
        if len(capacities) != self.__service_count or any(value <= 0 for value in capacities):
            raise ValueError("A positive native capacity is required for every service.")

        self.__active_history = [
            (float(timestamp), int(value)) for timestamp, value in active_history
        ]
        self.__queue_history = {
            service_id: [
                (float(timestamp), int(value))
                for timestamp, value in queue_history[service_id]
            ]
            for service_id in range(self.__service_count)
        }
        self.__busy_history = {
            service_id: [
                (float(timestamp), int(value))
                for timestamp, value in busy_history[service_id]
            ]
            for service_id in range(self.__service_count)
        }
        self.__active_requests = int(result["active_requests_at_end"])
        self.__native_final_queue_lengths = tuple(
            int(value) for value in result["queue_lengths_at_end"]
        )
        self.__native_final_busy_replicas = tuple(
            int(value) for value in result["busy_replicas_at_end"]
        )
        self.__native_capacities = capacities

        if self.__active_requests < 0:
            raise ValueError("Native active_requests cannot be negative.")
        if self.__active_history[-1][1] != self.__active_requests:
            raise ValueError("Native active history does not match final state.")
        for service_id in range(self.__service_count):
            queue = self.__native_final_queue_lengths[service_id]
            busy = self.__native_final_busy_replicas[service_id]
            if queue < 0 or not 0 <= busy <= capacities[service_id]:
                raise ValueError("Native service state violates capacity invariants.")
            if self.__queue_history[service_id][-1][1] != queue:
                raise ValueError("Native queue history does not match final state.")
            if self.__busy_history[service_id][-1][1] != busy:
                raise ValueError("Native busy history does not match final state.")

    def get_active_requests(self):
        return self.__active_requests

    def get_latency_collection_start(self):
        return self.__latency_collection_start

    def get_active_history(self):
        return tuple(self.__active_history)

    def get_queue_history(self):
        return {
            service_id: tuple(history)
            for service_id, history in self.__queue_history.items()
        }

    def get_busy_history(self):
        return {
            service_id: tuple(history)
            for service_id, history in self.__busy_history.items()
        }

    def get_queue_length(self, service_id):
        if self.__native_final_queue_lengths is not None:
            self.__validate_service_id(service_id)
            return self.__native_final_queue_lengths[service_id]
        resource = self.__get_service_resource(service_id)
        return len(resource.queue)

    def get_busy_replicas(self, service_id):
        if self.__native_final_busy_replicas is not None:
            self.__validate_service_id(service_id)
            return self.__native_final_busy_replicas[service_id]
        resource = self.__get_service_resource(service_id)
        return resource.count

    def get_number_in_system(self, service_id):
        return self.get_queue_length(service_id) + self.get_busy_replicas(service_id)

    def get_utilization(self, service_id):
        if self.__native_capacities is not None:
            self.__validate_service_id(service_id)
            return (
                self.__native_final_busy_replicas[service_id]
                / self.__native_capacities[service_id]
            )
        resource = self.__get_service_resource(service_id)
        return resource.count / resource.capacity

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

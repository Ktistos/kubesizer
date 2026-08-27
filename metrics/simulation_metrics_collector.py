import math


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
        self.__active_requests = 0
        self.__active_history = [(0.0, 0)]
        self.__queue_history = {
            service_id: [(0.0, 0)] for service_id in range(service_count)
        }
        self.__busy_history = {
            service_id: [(0.0, 0)] for service_id in range(service_count)
        }
        self.__native_final_queue_lengths = None
        self.__native_final_busy_replicas = None
        self.__native_capacities = None

    def __validate_service_id(self, service_id):
        if not isinstance(service_id, int) or not 0 <= service_id < self.__service_count:
            raise ValueError("service_id is outside the collector's service range.")

    @staticmethod
    def __validated_history(raw_history, label):
        history = tuple((float(time), int(value)) for time, value in raw_history)
        if not history or history[0] != (0.0, 0):
            raise ValueError(f"{label} must begin at (0.0, 0).")
        if any(
            not math.isfinite(timestamp) or timestamp < 0.0 or value < 0
            for timestamp, value in history
        ):
            raise ValueError(
                f"{label} must contain finite, non-negative state points."
            )
        if any(
            right[0] <= left[0]
            or right[1] == left[1]
            for left, right in zip(history, history[1:])
        ):
            raise ValueError(
                f"{label} must have increasing timestamps and changed, "
                "non-negative values."
            )
        return history

    def load_native_state(self, result, service_capacities):
        """Load state captured by the mandatory C++ execution backend."""
        active_history = result.get("active_history")
        queue_history = result.get("queue_history")
        busy_history = result.get("busy_history")
        if active_history is None or queue_history is None or busy_history is None:
            raise ValueError("The native result does not contain state histories.")
        if (
            len(queue_history) != self.__service_count
            or len(busy_history) != self.__service_count
        ):
            raise ValueError("Native state history service count does not match collector.")
        capacities = tuple(int(value) for value in service_capacities)
        if len(capacities) != self.__service_count or any(
            value <= 0 for value in capacities
        ):
            raise ValueError("A positive native capacity is required for every service.")

        self.__active_history = list(
            self.__validated_history(active_history, "active_history")
        )
        self.__queue_history = {
            service_id: list(
                self.__validated_history(
                    queue_history[service_id],
                    f"queue_history[{service_id}]",
                )
            )
            for service_id in range(self.__service_count)
        }
        self.__busy_history = {
            service_id: list(
                self.__validated_history(
                    busy_history[service_id],
                    f"busy_history[{service_id}]",
                )
            )
            for service_id in range(self.__service_count)
        }
        self.__active_requests = int(result["active_requests_at_end"])
        self.__native_final_queue_lengths = tuple(
            int(value) for value in result["queue_lengths_at_end"]
        )
        self.__native_final_busy_replicas = tuple(
            int(value) for value in result["busy_replicas_at_end"]
        )
        if (
            len(self.__native_final_queue_lengths) != self.__service_count
            or len(self.__native_final_busy_replicas) != self.__service_count
        ):
            raise ValueError("Native final state has the wrong service count.")
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
            if any(
                value > capacities[service_id]
                for _, value in self.__busy_history[service_id]
            ):
                raise ValueError("Native busy history exceeds service capacity.")

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
        self.__validate_service_id(service_id)
        if self.__native_final_queue_lengths is None:
            raise ValueError("No native simulation state has been loaded.")
        return self.__native_final_queue_lengths[service_id]

    def get_busy_replicas(self, service_id):
        self.__validate_service_id(service_id)
        if self.__native_final_busy_replicas is None:
            raise ValueError("No native simulation state has been loaded.")
        return self.__native_final_busy_replicas[service_id]

    def get_number_in_system(self, service_id):
        return self.get_queue_length(service_id) + self.get_busy_replicas(service_id)

    def get_utilization(self, service_id):
        self.__validate_service_id(service_id)
        if self.__native_capacities is None:
            raise ValueError("No native simulation state has been loaded.")
        return (
            self.__native_final_busy_replicas[service_id]
            / self.__native_capacities[service_id]
        )

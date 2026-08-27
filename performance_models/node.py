import math


class Node:

    def __init__(
        self,
        name,
        allocatable_cpu,
        allocatable_memory,
        node_communication_latencies,
        execution_time_multiplier=1.0,
    ):
        execution_time_multiplier = float(execution_time_multiplier)
        if (
            not math.isfinite(execution_time_multiplier)
            or execution_time_multiplier <= 0.0
        ):
            raise ValueError("execution_time_multiplier must be finite and positive.")
        self.__name = name
        self.__allocatable_cpu = allocatable_cpu
        self.__allocatable_memory = allocatable_memory
        self.__node_communication_latencies = node_communication_latencies
        self.__execution_time_multiplier = execution_time_multiplier
        self.__service_placements = {}

    def set_name(self, name):
        self.__name = name

    def set_allocatable_cpu(self, allocatable_cpu):
        self.__allocatable_cpu = allocatable_cpu

    def set_allocatable_memory(self, allocatable_memory):
        self.__allocatable_memory = allocatable_memory

    def set_node_communication_latencies(self, node_communication_latencies):
        self.__node_communication_latencies = node_communication_latencies

    def set_service_placements(self, service_placements):
        self.__service_placements = service_placements

    def add_service_replica(self, service):
        service_name = service.get_name()
        service_exists = self.__service_placements.get(service_name, None)

        if service_exists:
            self.__service_placements[service_name] += 1
        else:
            self.__service_placements[service_name] = 1
    def get_service_placements(self):
        return self.__service_placements

    def get_node_communication_latencies(self):
        return self.__node_communication_latencies

    def get_allocatable_memory(self):
        return self.__allocatable_memory

    def get_allocatable_cpu(self):
        return self.__allocatable_cpu

    def get_name(self):
        return self.__name

    def get_execution_time_multiplier(self):
        """Return a measured relative service-time factor for this node."""
        return self.__execution_time_multiplier

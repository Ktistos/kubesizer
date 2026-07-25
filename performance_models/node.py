class Node():

    def __init__(self, name, allocatable_cpu, allocatable_memory,node_communication_latencies):
        self.__name = name
        self.__allocatable_cpu = allocatable_cpu
        self.__allocatable_memory = allocatable_memory
        self.__node_communication_latencies = node_communication_latencies
        self.__service_placements = {}

    def set_name(self, name):
        self.__name = name

    def set_allocatable_cpu(self, allocatable_cpu):
        self.__allocatable_cpu = allocatable_cpu

    def set_allocatable_memory(self,allocatable_memory):
        self.__allocatable_memory = allocatable_memory

    def set_node_communication_latencies(self, node_communication_latencies):
        self.__node_communication_latencies = node_communication_latencies

    def set_service_placements(self,service_placements):
        self.__service_placements = service_placements

    def add_service_replica(self, service):
        service_name = service.get_name()
        service_exists = self.__service_placements.get(service_name, None)

        if service_exists:
            self.__service_placements[service_name] = self.__service_placements[service_name] + 1
        else :
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

        
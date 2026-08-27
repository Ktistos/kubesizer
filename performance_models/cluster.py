from itertools import cycle

from .node import Node


class Cluster:

    def __init__(self, name, resources, system):
        self.__name = name
        self.__system = system

        self.__nodes = {}

        for node in resources["nodes"]:
            self.__nodes[node["name"]] = Node(
                node["name"],
                node["allocatable_cpu_millicores"],
                node["allocatable_memory_bytes"],
                node["communication_latency_seconds"],
                node.get("execution_time_multiplier", 1.0),
            )

        self.__init_node_placement(system)

    def __init_node_placement(self, system):
        for service in system.get_services().values():
            node_cycle = cycle(self.__nodes.items())

            for _ in range(service.get_replicas()):
                current_node = next(node_cycle)[1]
                current_node.add_service_replica(service)

    def get_node_config(self):

        node_config = {}

        for node_name, node in self.get_nodes().items():
            node_config[node_name] = node.get_service_placements()

        return node_config

    def set_name(self, name):
        self.__name = name

    def get_name(self):
        return self.__name

    def get_system(self):
        return self.__system

    def get_node_by_name(self, node_name):
        return self.__nodes.get(node_name)

    def get_nodes(self):
        return self.__nodes

    def simulation_placement_model(self, placement):
        """Build the node-delay model consumed by the native simulator."""
        complete_placement = {
            name: dict(placement.get(name, {})) for name in self.__nodes
        }
        return {
            "placement": complete_placement,
            "communication_latency_seconds": {
                name: dict(node.get_node_communication_latencies())
                for name, node in self.__nodes.items()
            },
            "execution_time_multipliers": {
                name: node.get_execution_time_multiplier()
                for name, node in self.__nodes.items()
            },
        }

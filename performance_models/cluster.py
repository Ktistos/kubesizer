from .node import Node


class Cluster():

    def __init__(self, name, resources, system):
        self.__name = name

        self.__nodes = {}

        for node in resources["nodes"]:
            self.__nodes[node["name"]] = Node(node["name"], node["allocatable_cpu_millicores"], node["allocatable_memory_bytes"], node["communication_latency_seconds"])

        



    def set_name(self,name):
        self.__name = name

    def get_name(self):
        return self.__name

    def get_node_by_name(self, node_name):
        return self.__nodes.get(node_name)

    def get_nodes(self):
        return self.__nodes

    
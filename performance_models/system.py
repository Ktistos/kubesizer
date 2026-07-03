from enum import Enum

from performance_models.service import Service


class QueueingNetworkType(str, Enum):
    OPEN = "open"
    CLOSED = "closed"
    HYBRID = "hybrid"


class System():

    def __init__(self, name,service_spec, type : QueueingNetworkType):
        self.__name = name
        #type refers to either open or closed system
        self.__type = type
        self.__services = {}

        for service in service_spec:

            service_component = Service(self, service, service_spec[service]['gamma'], service_spec[service]['mu'], service_spec[service]['replicas'])

            self.add_service(service,service_component)

        for _, service_component in self.get_services().items():
            service_component.register_edges(service_spec[service]['routes'])




    def get_name(self):
        return self.__name

    def get_type(self):
        return self.__type

    def add_service(self, name, component):
        self.__services[name] = component

    def get_service(self, name):
        return self.__services.get(name, None)
    
    def get_services(self):
        return self.__services

    def solve(self):
        # Placeholder for solving the system
        pass
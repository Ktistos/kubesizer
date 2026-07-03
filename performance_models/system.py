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
        self.__services = []

        for service in service_spec:

            service_component = Service(service,service_spec[service]['routes'], service_spec[service]['gamma'], service_spec[service]['mu'], service_spec[service]['replicas'])

            self.add_service(service_component)


    def get_name(self):
        return self.__name

    def get_type(self):
        return self.__type

    def add_service(self, component):
        self.__services.append(component)
    
    def solve(self):
        # Placeholder for solving the system
        pass
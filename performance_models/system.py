from enum import Enum
import math
import simpy
from performance_models.service import Service

import random

import numpy as np

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
        self.__r_min=0
        self.__is_stable = False
        self.__external_traffic_services = []


        for service in service_spec:

            service_component = Service(self, service, service_spec[service]['gamma'], service_spec[service]['mu'], service_spec[service]['replicas'])

            self.add_service(service,service_component)

        for service, service_component in self.get_services().items():
            service_component.register_edges(service_spec[service].get('routes', {}))

        self.__utilizations = [ 0 for _ in range(len(self.get_services())) ]
        self.__latencies = [0 for _ in range(len(self.get_services()))]

    def __initialize_system_input(self):
        num_of_services = len(self.get_services())
        gammas = []
        mus = []
        replicas = []
        
        routing_probability_matrix = [[0 for _ in range(num_of_services)] for _ in range(num_of_services)]

        for service_name in self.get_services():
            service = self.get_service(service_name)
            order_index = service.get_id()
            service_replicas = service.get_replicas()
            service_gamma = service.get_gamma()

            if service_gamma > 0:
                self.get_external_traffic_services().append(service)

            service_mu = service.get_mu()
            gammas.append(service_gamma)
            mus.append(service_mu)
            replicas.append(service_replicas)

            adjacency_list = service.get_adjacency_list()

            for edge in adjacency_list:
                target_service = edge.get_target()
                routing_probability = edge.get_routing_prob()

                if target_service is not None:
                    outgoing_service_index = target_service.get_id()

                    routing_probability_matrix[order_index][outgoing_service_index] = routing_probability
                
        
        return {
            "gammas": gammas,
            "mus": mus,
            "replicas": replicas,
            "routing_probability_matrix": routing_probability_matrix
        }

    def get_external_traffic_services(self):
        return self.__external_traffic_services



    def is_stable(self, utilizations):
        for utilization in utilizations:
            if utilization >= 1.0:
                return False
        return True
    
    def get_bottleneck_service(self, utilizations):
        max_index = np.argmax(utilizations)
        return list(self.get_services().values())[max_index].get_name()
    
    def __mmc_latency(self,service_lambda, per_replica_mu, replicas):
        if per_replica_mu <= 0 or replicas <= 0:
            return float("inf")

        if service_lambda == 0:
            return 1 / per_replica_mu

        rho = service_lambda / (replicas * per_replica_mu)

        if rho >= 1:
            return float("inf")

        a = service_lambda / per_replica_mu

        sum_terms = 0.0
        for n in range(replicas):
            sum_terms += (a ** n) / math.factorial(n)

        last_term = ((a ** replicas) / math.factorial(replicas)) * (1 / (1 - rho))

        p0 = 1 / (sum_terms + last_term)

        p_wait = last_term * p0

        wq = p_wait / (replicas * per_replica_mu - service_lambda)

        response_time = (1 / per_replica_mu) + wq

        return response_time

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
    
    def get_r_min(self):
        return self.__r_min

    def get_system_replicas(self):
        replicas = [ 0 for _ in range(len(self.get_services())) ]
        for service in self.get_services().values():
            service_id = service.get_id()
            replicas[service_id] = service.get_replicas()
        return replicas
    
    def configure_system_replicas(self, replicas_config):
        for service in self.get_services().values():
            service_id = service.get_id()
            service.set_replicas(replicas_config[service_id])
    
    def get_utilizations(self):
        return self.__utilizations

    def get_latencies(self):
        return self.__latencies
    
    def get_stability(self):
        return self.__is_stable
    
    def set_stability(self, stability):
        self.__is_stable = stability

    def setUtilizations(self, utilizations):
        self.__utilizations = utilizations

    def setLatencies(self, latencies):
        self.__latencies = latencies

    def configure_system_replicas(self, replicas_config):
        for service in self.get_services().values():
            service_id = service.get_id()
            service.set_replicas(replicas_config[service_id])

 
    def solve(self):

        system_input = self.__initialize_system_input()

        #gathering the system input data for the solver
        
        num_of_services = len(self.get_services())

        gammas = np.array(system_input["gammas"], dtype=float)
        mus = np.array(system_input["mus"], dtype=float)
        replicas = np.array(system_input["replicas"], dtype=int)
        routing_probability_matrix = np.array(system_input["routing_probability_matrix"], dtype=float)
        
        identity_matrix = np.eye(num_of_services)


        #getting the lambdas for each service by solving the linear system of equations
        lambdas = np.linalg.solve(identity_matrix - routing_probability_matrix.T, gammas)

        #calculating the capacities and utilizations for each service
        capacities = replicas * mus
        utilizations = lambdas / capacities

        self.__r_min = np.sum((lambdas / gammas.sum()) * (1 / mus))

        system_metrics = {}

        self.set_stability(self.is_stable(utilizations))
        system_metrics["bottleneck_service"] = self.get_bottleneck_service(utilizations)
        system_metrics["services"] = [0 for _ in range(num_of_services)]

        lambdas = lambdas.tolist()
        mus = mus.tolist()
        gammas = gammas.tolist()
        replicas = replicas.tolist()
        capacities = capacities.tolist()
        utilizations = utilizations.tolist()

        self.setUtilizations(utilizations)
        self.setLatencies([self.__mmc_latency(lambdas[i], mus[i], replicas[i]) for i in range(num_of_services)])


    def simulate(self, sim_duration ):
        env = simpy.Environment()


        for service in self.get_services().values():
            service.set_active_simulation_resource(simpy.Resource(env,capacity = service.get_replicas()))
            env.process(service.generate_external_traffic(env))

        env.run(until = sim_duration)



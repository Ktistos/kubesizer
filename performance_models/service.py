from performance_models.edge import Edge
import random

class Service():
    __num_of_services = 0


    def __init__(self, parent_system, name,gamma = 0, mu=0, replicas=1):
        self.__id = Service.__num_of_services
        self.__parent_system = parent_system
        self.__name = name
        self.__adjacency_list = []
        self.__gamma = gamma
        self.__mu = mu   
        self.__replicas = replicas
        self.__active_simulation_resource = None

        Service.__num_of_services += 1

    def set_active_simulation_resource(self,resource):
        self.__active_simulation_resource = resource

    def get_active_simulation_resource(self):
        return self.__active_simulation_resource


    def set_replicas(self, replicas):
        self.__replicas = replicas

    def get_id(self):
        return self.__id

    def register_edges(self, edges_spec):
        total_probabiliy = 0

        for edge in edges_spec:
            probability = edges_spec[edge]
            total_probabiliy += probability
            edge_component = Edge(self,self.get_parent_system().get_service(edge) ,probability)
            self.add_edge(edge_component)
        
        #this assumes that the remaining routing probability points to the exit of the system
        remaining_probability = 1 - total_probabiliy
        if remaining_probability > 0:
            edge_component = Edge(self, None, remaining_probability)
            self.add_edge(edge_component)
        else:
            raise ValueError(f"Total probability of edges for service {self.get_name()} exceeds 1.")


    def get_parent_system(self):
        return self.__parent_system

    def get_name(self):
        return self.__name

    def get_adjacency_list(self):
        return self.__adjacency_list

    def get_gamma(self):
        return self.__gamma

    def get_mu(self):
        return self.__mu

    def get_replicas(self):
        return self.__replicas
    
    def add_edge(self, edge : Edge):
        self.get_adjacency_list().append(edge)


    def generate_external_traffic(self,env):
        
        sim_resource = self.get_active_simulation_resource()

        gamma = self.get_gamma()

        while True and ( sim_resource is not None) and (gamma > 0):
            successive_arrival_interval = random.expovariate(gamma)
            yield env.timeout(successive_arrival_interval)
            
            env.process(self.serve_request(env))

    def serve_request(self,env):
        sim_resource = self.get_active_simulation_resource()
        with sim_resource.request() as req:
            #request enters the queue
            yield req

            #request is served
            job_service_time = random.expovariate(self.get_mu())
            
            yield env.timeout(job_service_time)

            possible_routes=[]
            route_probabilities = []

            #request exits the service
            for edge in self.get_adjacency_list():
                possible_routes.append(edge.get_target())
                route_probabilities.append(edge.get_routing_prob())

            #request gets rerouted to another service, if None it leaves the system
            route_service = random.choices(possible_routes, weights=route_probabilities, k=1)[0]

            if route_service is not None:
                env.process(route_service.serve_request(env))


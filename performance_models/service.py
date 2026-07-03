from performance_models.edge import Edge


class Service():
    def __init__(self, parent_system, name,gamma = 0, mu=0, replicas=1):
        
        self.__parent_system = parent_system
        self.__name = name
        self.__adjacency_list = []
        self.__gamma = gamma
        self.__mu = mu   
        self.__replicas = replicas

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
        elif total_probabiliy > 1:
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

class ServiceGraph():
    def __init__(self, services):
        self.__services = services

    def get_services(self):
        return self.__services
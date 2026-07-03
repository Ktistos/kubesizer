from performance_models.edge import Edge


class Service():
    def __init__(self, name, adjacency_list={},gamma = 0, mu=0, replicas=1):
        self.__name = name
        self.__adjacency_list = adjacency_list
        self.__gamma = gamma
        self.__mu = mu   
        self.__replicas = replicas


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
        self.__adjacency_list.append(edge)

class ServiceGraph():
    def __init__(self, services):
        self.__services = services

    def get_services(self):
        return self.__services
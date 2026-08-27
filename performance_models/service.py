from .edge import Edge


class Service:
    __num_of_services = 0

    def __init__(
        self,
        parent_system,
        name,
        gamma=None,
        mu=0,
        replicas=1,
        service_id=None,
    ):
        if service_id is None:
            service_id = Service.__num_of_services
            Service.__num_of_services += 1

        self.__id = service_id
        self.__parent_system = parent_system
        self.__name = name
        self.__adjacency_list = {}
        self.__gamma = dict(gamma or {})
        self.__mu = mu
        self.__replicas = replicas

    def set_replicas(self, replicas):
        self.__replicas = replicas

    def get_id(self):
        return self.__id

    def register_edges(self, request_class, edges_spec):
        self.__adjacency_list[request_class] = []
        total_probability = 0

        for target_name, probability in edges_spec.items():
            target = self.get_parent_system().get_service(target_name)
            if target is None:
                raise ValueError(
                    f"Route from '{self.get_name()}' targets unknown service "
                    f"'{target_name}'."
                )
            total_probability += probability
            self.add_edge(request_class, Edge(self, target, probability))

        # this assumes that the remaining routing probability points to the exit of the system
        remaining_probability = 1 - total_probability
        if remaining_probability > 0:
            self.add_edge(request_class, Edge(self, None, remaining_probability))
        elif remaining_probability < 0:
            raise ValueError(
                f"Total probability of edges for service {self.get_name()} exceeds 1."
            )

    def get_parent_system(self):
        return self.__parent_system

    def get_name(self):
        return self.__name

    def get_adjacency_list(self, request_class):
        return tuple(self.__adjacency_list.get(request_class, ()))

    def get_gamma(self, request_class):
        return self.__gamma.get(request_class, 0.0)

    def add_gamma(self, request_class, gamma):
        self.__gamma[request_class] = gamma

    def get_mu(self):
        return self.__mu

    def get_replicas(self):
        return self.__replicas

    def add_edge(self, request_class, edge: Edge):
        self.__adjacency_list.setdefault(request_class, []).append(edge)

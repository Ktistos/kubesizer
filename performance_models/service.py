from .edge import Edge
import random


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
        self.__active_simulation_resource = None

    def set_active_simulation_resource(self, resource):
        self.__active_simulation_resource = resource

    def get_active_simulation_resource(self):
        return self.__active_simulation_resource

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

    def generate_external_traffic(self, env, metrics_collector, request_class):

        sim_resource = self.get_active_simulation_resource()

        gamma = self.get_gamma(request_class)

        while True and (sim_resource is not None) and (gamma > 0):
            successive_arrival_interval = random.expovariate(gamma)
            yield env.timeout(successive_arrival_interval)

            request_context = metrics_collector.start_external_request(
                request_class,
                env.now,
            )
            env.process(
                self.serve_request(
                    env,
                    metrics_collector,
                    request_class,
                    request_context,
                )
            )

    def serve_request(
        self, env, metrics_collector, request_class, request_context=None
    ):
        arrival_time = env.now
        sim_resource = self.get_active_simulation_resource()
        with sim_resource.request() as req:
            # The request either acquires a replica immediately or joins the queue.
            metrics_collector.record_service_state(self.get_id(), env.now)
            yield req
            # A queued request may have acquired a replica after another release.
            metrics_collector.record_service_state(self.get_id(), env.now)

            # request is served
            job_service_time = random.expovariate(self.get_mu())
            metrics_collector.record_service_start(self.get_id(), env.now)

            yield env.timeout(job_service_time)

            metrics_collector.record_request_completion(
                self.get_id(), env.now - arrival_time, env.now
            )

            possible_routes = []
            route_probabilities = []

            # request exits the service
            for edge in self.get_adjacency_list(request_class):
                possible_routes.append(edge.get_target())
                route_probabilities.append(edge.get_routing_prob())

            # request gets rerouted to another service, if None it leaves the system
            route_service = random.choices(
                possible_routes, weights=route_probabilities, k=1
            )[0]

            if route_service is not None:
                env.process(
                    route_service.serve_request(
                        env,
                        metrics_collector,
                        request_class,
                        request_context,
                    )
                )
            elif request_context is not None:
                metrics_collector.record_external_request_completion(
                    request_context,
                    env.now,
                )

        # Exiting the context releases the replica. A queued request may acquire
        # it later at the same simulation timestamp.
        metrics_collector.record_service_state(self.get_id(), env.now)

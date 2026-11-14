


class Edge:
    def __init__(self, source , target , routing_probability=0):
        self.__source = source
        self.__target = target
        self.__routing_prob = routing_probability

    def get_source(self):
        return self.__source
    def get_target(self):
        return self.__target
    def get_routing_prob(self):
        return self.__routing_prob  
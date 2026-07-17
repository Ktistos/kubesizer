class Metrics():
    def __init__(self, mean_utilizations, mean_latencies, stability, p95_latencies =[], p99_latencies = []):
        self.__mean_utilizations = mean_utilizations
        self.__mean_latencies = mean_latencies
        self.__stability = stability
        self.__p95_latencies = p95_latencies
        self.__p99_latencies = p99_latencies

    def get_mean_utilizations(self):
        return self.__mean_utilizations
    
    def get_mean_latencies(self):
        return self.__mean_latencies
    
    def get_stability(self):
        return self.__stability
        
    def get_p95_latencies(self):
        return self.__p95_latencies
    
    def get_p99_latencies(self):
        return self.__p99_latencies

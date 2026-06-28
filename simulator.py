import simpy
import random
import statistics
import utils

class Simulator:
    def __init__(self, env=None, seed=42, mu_rate=0, lambda_rate=0, latency_slo=0):
        self.generated_requests = 0
        self.num_of_requests_served = 0

        self.service_times = []
        self.waiting_times = []
        self.response_times = []
        self.metrics = {}
        self.queued_requests = 0
        self.system_requests = 0
        self.in_service_requests = 0

        self.max_queue_length = 0
        self.max_system_length = 0

        self.area_queue = 0.0
        self.area_system = 0.0
        self.area_busy = 0.0
        self.last_metric_time = 0.0
        self.mu_rate = mu_rate
        self.lambda_rate = lambda_rate
        self.latency_slo = latency_slo

        self.env = env if env is not None else simpy.Environment()
        random.seed(seed)
        self.resource = simpy.Resource(self.env, capacity=1)




    def update_time_weighted_metrics(self):
        """
        Updates time-weighted queue length, system length, and busy time.

        This is needed because queue length is not a simple average of samples.
        We need to weight each observed value by how long it lasted.
        """
        dt = self.env.now - self.last_metric_time

        dt = self.env.now - self.last_metric_time

        if dt > 0:
            self.area_queue += self.queued_requests * dt
            self.area_system += self.system_requests * dt
            self.area_busy += self.in_service_requests * dt
            self.last_metric_time = self.env.now

    def serve_request(self):


        arrival_time = self.env.now

        self.update_time_weighted_metrics()
        self.system_requests += 1
        self.max_system_length = max(self.max_system_length, self.system_requests)

        will_queue = self.resource.count >= self.resource.capacity

        if will_queue:
            self.queued_requests += 1
            self.max_queue_length = max(self.max_queue_length, self.queued_requests)

        with self.resource.request() as req:
            yield req

            service_start_time = self.env.now

            if will_queue:
                self.update_time_weighted_metrics()
                self.queued_requests -= 1

            self.update_time_weighted_metrics()
            self.in_service_requests += 1

            waiting_time = service_start_time - arrival_time

            job_service_time = random.expovariate(self.mu_rate)

            yield self.env.timeout(job_service_time)

            finish_time = self.env.now
            response_time = finish_time - arrival_time

            self.update_time_weighted_metrics()
            self.in_service_requests -= 1
            self.system_requests -= 1

            self.service_times.append(job_service_time)
            self.waiting_times.append(waiting_time)
            self.response_times.append(response_time)

            self.num_of_requests_served += 1



    def run(self,simulation_seconds):

        self.env.process(self.generate_requests())
        # First request at time 0
        self.env.run(until=simulation_seconds)

        self.build_metrics()

    def generate_requests(self):
        global generated_requests

        # First request at time 0
        self.generated_requests += 1
        self.env.process(self.serve_request())

        i = 1

        while True:
            interarrival_time = random.expovariate(self.lambda_rate)
            yield self.env.timeout(interarrival_time)

            self.generated_requests += 1
            self.env.process(self.serve_request())

            i += 1

    def update_time_weighted_metrics(self):
        """
        Updates time-weighted queue length, system length, and busy time.

        This is needed because queue length is not a simple average of samples.
        We need to weight each observed value by how long it lasted.
        """
        dt = self.env.now - self.last_metric_time

        if dt > 0:
            self.area_queue += self.queued_requests * dt
            self.area_system += self.system_requests * dt
            self.area_busy += self.in_service_requests * dt
            self.last_metric_time = self.env.now

    def build_metrics(self):
        self.update_time_weighted_metrics()

        simulation_time = self.env.now

        completed_requests = self.num_of_requests_served
        backlog_at_end = self.generated_requests - completed_requests

        mean_service_time = statistics.mean(self.service_times) if self.service_times else 0
        mean_waiting_time = statistics.mean(self.waiting_times) if self.waiting_times else 0
        mean_response_time = statistics.mean(self.response_times) if self.response_times else 0

        p95_waiting_time = utils.percentile(self.waiting_times, 95)
        p99_waiting_time = utils.percentile(self.  waiting_times, 99)

        p95_response_time = utils.percentile(self.response_times, 95)
        p99_response_time = utils.percentile(self.response_times, 99)

        max_waiting_time = max(self.waiting_times) if self.waiting_times else 0
        max_response_time = max(self.response_times) if self.response_times else 0

        observed_arrival_rate = self.generated_requests / simulation_time if simulation_time > 0 else 0
        throughput = self.num_of_requests_served / simulation_time if simulation_time > 0 else 0

        utilization = self.area_busy / (self.resource.capacity * simulation_time) if simulation_time > 0 else 0

        mean_queue_length = self.area_queue / simulation_time if simulation_time > 0 else 0
        mean_system_length = self.area_system / simulation_time if simulation_time > 0 else 0

        slo_violations = sum(1 for rt in self.response_times if rt > self.latency_slo)
        slo_violation_rate = slo_violations / self.num_of_requests_served if self.num_of_requests_served > 0 else 0

        # For M/M/c, stability condition is λ < cμ
        stable_theoretically = self.lambda_rate < self.resource.capacity * self.mu_rate

        # Little's Law: L = λW
        little_law_estimated_L = observed_arrival_rate * mean_response_time

        self.metrics = {
            "lambda_rate": self.lambda_rate,
            "mu_rate": self.mu_rate,
            "capacity": self.resource.capacity,

            "simulation_time": simulation_time,

            "generated_requests": self.generated_requests,
            "completed_requests": self.num_of_requests_served,
            "backlog_at_end": backlog_at_end,

            "observed_arrival_rate": observed_arrival_rate,
            "throughput": throughput,

            "mean_service_time": mean_service_time,

            "mean_waiting_time": mean_waiting_time,
            "p95_waiting_time": p95_waiting_time,
            "p99_waiting_time": p99_waiting_time,
            "max_waiting_time": max_waiting_time,

            "mean_response_time": mean_response_time,
            "p95_response_time": p95_response_time,
            "p99_response_time": p99_response_time,
            "max_response_time": max_response_time,

            "queue_length_at_end": self.queued_requests,
            "system_length_at_end": self.system_requests,
            "max_queue_length": self.max_queue_length,
            "max_system_length": self.max_system_length,
            "mean_queue_length": mean_queue_length,
            "mean_system_length": mean_system_length,

            "utilization": utilization,

            "latency_slo": self.latency_slo,
            "slo_violations": slo_violations,
            "slo_violation_rate": slo_violation_rate,

            "little_law_estimated_L": little_law_estimated_L,

            "stable_theoretically": stable_theoretically
        }
        return self.metrics


    def print_metrics(self, metrics=None):
        if metrics is None:
            metrics = self.metrics

        print("\n--- Simulation Metrics ---")

        for key, value in metrics.items():
            if isinstance(value, float):
                print(f"{key}: {value:.4f}")
            else:
                print(f"{key}: {value}")

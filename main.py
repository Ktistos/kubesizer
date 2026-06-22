import simpy
import random
import statistics



service_time = 0
num_of_requests_served = 0
queued_requests = 0

def serve_request(env, name, resource, mu_rate):
    global service_time, num_of_requests_served, queued_requests
    with resource.request() as req:
        queued_requests += 1
        yield req
        queued_requests -= 1

        job_service_time = random.expovariate(mu_rate)
        service_time = service_time + job_service_time
        yield env.timeout(job_service_time)
        num_of_requests_served += 1
        print(f"{name} is done.")

def generate_requests(env, resource, lambda_rate, mu_rate):
    env.process(serve_request(env, "Request 0", resource, mu_rate))
    i=0
    while True:
        yield env.timeout(random.expovariate(lambda_rate))
        print(f"Request {i} is generated.")
        env.process(serve_request(env, f"Request {i}", resource, mu_rate))
        i += 1


def main():
    env = simpy.Environment()

    resource = simpy.Resource(env, capacity=1)
    lambda_rate = 0.5
    mu_rate = 0.4

    env.process(generate_requests(env, resource, lambda_rate, mu_rate))
    env.run(until=100000)
    mean_service_time = service_time / num_of_requests_served if num_of_requests_served > 0 else 0
    print(f"Mean service time: {mean_service_time}")
    print(f"Number of requests served: {num_of_requests_served}")
    print(f"Queued requests: {queued_requests}")
    print(f"Utilization: {service_time / env.now if env.now > 0 else 0}")
    print(f"Throughput: {num_of_requests_served / env.now if env.now > 0 else 0}")
    print(f"Little 's Law: {lambda_rate * mean_service_time}")







if __name__ == "__main__":
    main()


    # Your main code logic here
    print("This script is being run directly.")
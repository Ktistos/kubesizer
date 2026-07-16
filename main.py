
import yaml
from performance_models import Service, Edge, System, QueueingNetworkType, service
import math


def stop_optimization(damage=[]):
    
    for i in range(len(damage)):
        if damage[i] != math.inf:
            return False
    return True


def main():

    with open("workload.yaml", "r") as file:
        workload = yaml.safe_load(file)

    service_spec = workload['services']

    system = System('STELAR', service_spec, QueueingNetworkType.OPEN)

    services = system.get_services()

    system.simulate(100000)
    
    system.solve()

    damage = [0 for _ in range(len(services))]
    
    print(system.get_system_replicas())

    """
    while not stop_optimization(damage):

        system.solve()
        replicas = system.get_system_replicas()
        latencies = system.get_latencies()

        damage = [0 for _ in range(len(services))]

        for service in services.values():
            service_id = service.get_id()

            before_latency = latencies[service_id]
            replicas[service_id] = replicas[service_id] - 1

            if replicas[service_id]==0:
                replicas[service_id] = 1
                system.configure_system_replicas(replicas)
                damage[service_id] = math.inf
                continue

            system.configure_system_replicas(replicas)

            system.solve()

            stability = system.get_stability()

            if not stability:
                replicas[service_id] = replicas[service_id] + 1
                system.configure_system_replicas(replicas)
                damage[service_id]=math.inf
                continue

            after_latency = system.get_latencies()[service_id]
            
            replicas[service_id] = replicas[service_id] + 1
            
            system.configure_system_replicas(replicas)
            
            system.solve()

            damage[service_id]=(after_latency - before_latency)


        min_damage_index =damage.index(min(damage))

        replicas[min_damage_index] = (replicas[min_damage_index] - 1) if (replicas[min_damage_index] >1) else 1

        system.configure_system_replicas(replicas)
    """




if __name__ == "__main__":
    main()

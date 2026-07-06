
import yaml
from performance_models import Service, Edge, System, QueueingNetworkType

def main():

    with open("workload.yaml", "r") as file:
        workload = yaml.safe_load(file)

    service_spec = workload['services']

    system = System('STELAR', service_spec, QueueingNetworkType.OPEN)

    print(system.solve())


if __name__ == "__main__":
    main()

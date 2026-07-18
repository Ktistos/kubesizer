import math


class Metrics:
    def __init__(
        self,
        mean_utilizations,
        mean_latencies,
        stability,
        p95_latencies=None,
        p99_latencies=None,
    ):
        self.__mean_utilizations = list(mean_utilizations)
        self.__mean_latencies = list(mean_latencies)
        self.__stability = stability
        self.__p95_latencies = list(p95_latencies or [])
        self.__p99_latencies = list(p99_latencies or [])

    def is_nearly_equal(
        self,
        other,
        relative_tolerance=0.05,
        absolute_tolerance=1e-9,
    ):
        """Compare mean utilizations and latencies within the given tolerances."""
        if not isinstance(other, Metrics):
            return False
        if relative_tolerance < 0 or absolute_tolerance < 0:
            raise ValueError("Tolerances must be non-negative.")

        metric_pairs = (
            (self.__mean_utilizations, other.__mean_utilizations),
            (self.__mean_latencies, other.__mean_latencies),
        )

        for own_values, other_values in metric_pairs:
            if len(own_values) != len(other_values):
                return False
            if not all(
                math.isclose(
                    own_value,
                    other_value,
                    rel_tol=relative_tolerance,
                    abs_tol=absolute_tolerance,
                )
                for own_value, other_value in zip(own_values, other_values)
            ):
                return False

        return True

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

import math
import statistics
from dataclasses import dataclass
from enum import Enum


class ReliabilityStatus(str, Enum):
    RELIABLY_FEASIBLE = "reliably_feasible"
    RELIABLY_INFEASIBLE = "reliably_infeasible"
    UNCERTAIN = "uncertain"


@dataclass(frozen=True)
class RequestClassP95ConfidenceInterval:
    request_class: str
    slo: float
    replication_count: int
    mean_p95: float
    standard_deviation: float
    standard_error: float
    confidence_level: float
    lower_bound: float
    upper_bound: float
    half_width: float
    relative_half_width: float
    status: ReliabilityStatus
    replication_p95_values: tuple[float, ...]
    measured_request_counts: tuple[int, ...]


@dataclass(frozen=True)
class RequestClassP95Reliability:
    replica_configuration: tuple[int, ...]
    seeds: tuple[int, ...]
    warmup_duration: float
    measurement_duration: float
    family_confidence_level: float
    per_interval_confidence_level: float
    multiple_class_adjustment: str
    request_class_intervals: dict[str, RequestClassP95ConfidenceInterval]
    status: ReliabilityStatus

    @property
    def feasible(self):
        return self.status == ReliabilityStatus.RELIABLY_FEASIBLE

    @property
    def replication_count(self):
        return len(self.seeds)


def _beta_continued_fraction(a, b, x):
    maximum_iterations = 300
    epsilon = 3e-14
    minimum = 1e-300
    qab = a + b
    qap = a + 1.0
    qam = a - 1.0
    c = 1.0
    d = 1.0 - qab * x / qap
    if abs(d) < minimum:
        d = minimum
    d = 1.0 / d
    result = d

    for iteration in range(1, maximum_iterations + 1):
        twice = 2 * iteration
        coefficient = (
            iteration * (b - iteration) * x
            / ((qam + twice) * (a + twice))
        )
        d = 1.0 + coefficient * d
        if abs(d) < minimum:
            d = minimum
        c = 1.0 + coefficient / c
        if abs(c) < minimum:
            c = minimum
        d = 1.0 / d
        result *= d * c

        coefficient = -(
            (a + iteration)
            * (qab + iteration)
            * x
            / ((a + twice) * (qap + twice))
        )
        d = 1.0 + coefficient * d
        if abs(d) < minimum:
            d = minimum
        c = 1.0 + coefficient / c
        if abs(c) < minimum:
            c = minimum
        d = 1.0 / d
        change = d * c
        result *= change
        if abs(change - 1.0) <= epsilon:
            return result

    raise RuntimeError("Incomplete-beta continued fraction did not converge.")


def _regularized_incomplete_beta(a, b, x):
    if not 0.0 <= x <= 1.0:
        raise ValueError("x must lie in [0, 1].")
    if x == 0.0:
        return 0.0
    if x == 1.0:
        return 1.0
    leading = math.exp(
        math.lgamma(a + b)
        - math.lgamma(a)
        - math.lgamma(b)
        + a * math.log(x)
        + b * math.log1p(-x)
    )
    if x < (a + 1.0) / (a + b + 2.0):
        return leading * _beta_continued_fraction(a, b, x) / a
    return 1.0 - (
        leading * _beta_continued_fraction(b, a, 1.0 - x) / b
    )


def student_t_cdf(value, degrees_of_freedom):
    if isinstance(degrees_of_freedom, bool) or degrees_of_freedom <= 0:
        raise ValueError("degrees_of_freedom must be positive.")
    if not math.isfinite(value):
        return 0.0 if value < 0 else 1.0
    if value == 0.0:
        return 0.5
    transformed = degrees_of_freedom / (
        degrees_of_freedom + value * value
    )
    tail = 0.5 * _regularized_incomplete_beta(
        degrees_of_freedom / 2.0,
        0.5,
        transformed,
    )
    return 1.0 - tail if value > 0 else tail


def student_t_quantile(probability, degrees_of_freedom):
    """Return a Student-t quantile without requiring SciPy."""
    probability = float(probability)
    if not 0.0 < probability < 1.0:
        raise ValueError("probability must lie strictly between zero and one.")
    if probability == 0.5:
        return 0.0
    if probability < 0.5:
        return -student_t_quantile(1.0 - probability, degrees_of_freedom)

    lower = 0.0
    upper = 1.0
    while student_t_cdf(upper, degrees_of_freedom) < probability:
        upper *= 2.0
        if not math.isfinite(upper):
            raise RuntimeError("Could not bracket Student-t quantile.")
    for _ in range(100):
        middle = (lower + upper) / 2.0
        if student_t_cdf(middle, degrees_of_freedom) < probability:
            lower = middle
        else:
            upper = middle
    return (lower + upper) / 2.0


def evaluate_request_class_p95_reliability(
    replications,
    request_class_slos,
    *,
    confidence_level=0.95,
    adjust_for_multiple_classes=True,
):
    """Classify fixed independent replication-level p95 observations."""
    replications = tuple(replications)
    request_class_slos = dict(request_class_slos)
    if len(replications) < 2:
        raise ValueError("At least two independent replications are required.")
    if not request_class_slos:
        raise ValueError("At least one request-class SLO is required.")
    if any(
        not math.isfinite(float(slo)) or float(slo) <= 0
        for slo in request_class_slos.values()
    ):
        raise ValueError("Request-class SLOs must be finite and positive.")
    confidence_level = float(confidence_level)
    if not 0.0 < confidence_level < 1.0:
        raise ValueError("confidence_level must lie strictly between zero and one.")

    first = replications[0]
    configuration = tuple(first.replica_configuration)
    warmup_duration = float(first.warmup_duration)
    measurement_duration = float(first.measurement_duration)
    seeds = tuple(replication.seed for replication in replications)
    if len(set(seeds)) != len(seeds):
        raise ValueError("Replication seeds must be unique.")
    for replication in replications[1:]:
        if (
            tuple(replication.replica_configuration) != configuration
            or float(replication.warmup_duration) != warmup_duration
            or float(replication.measurement_duration) != measurement_duration
        ):
            raise ValueError(
                "Replications must share configuration, warm-up, and "
                "measurement duration."
            )

    class_count = len(request_class_slos)
    family_alpha = 1.0 - confidence_level
    per_interval_alpha = (
        family_alpha / class_count
        if adjust_for_multiple_classes
        else family_alpha
    )
    per_interval_confidence = 1.0 - per_interval_alpha
    critical_value = student_t_quantile(
        1.0 - per_interval_alpha / 2.0,
        len(replications) - 1,
    )

    intervals = {}
    for request_class, raw_slo in request_class_slos.items():
        p95_values = []
        request_counts = []
        for replication in replications:
            if request_class not in replication.request_class_p95_latencies:
                raise ValueError(
                    f"Replication {replication.seed} has no p95 for "
                    f"request class {request_class!r}."
                )
            count = int(replication.measured_request_counts.get(request_class, 0))
            if count <= 0:
                raise ValueError(
                    f"Replication {replication.seed} measured no requests for "
                    f"class {request_class!r}."
                )
            value = float(
                replication.request_class_p95_latencies[request_class]
            )
            if not math.isfinite(value) or value < 0:
                raise ValueError(
                    "Replication p95 values must be finite and non-negative."
                )
            p95_values.append(value)
            request_counts.append(count)

        mean_p95 = statistics.fmean(p95_values)
        standard_deviation = statistics.stdev(p95_values)
        standard_error = standard_deviation / math.sqrt(len(p95_values))
        half_width = critical_value * standard_error
        lower_bound = mean_p95 - half_width
        upper_bound = mean_p95 + half_width
        relative_half_width = (
            half_width / abs(mean_p95)
            if mean_p95 != 0
            else (0.0 if half_width == 0 else math.inf)
        )
        slo = float(raw_slo)
        if upper_bound <= slo:
            status = ReliabilityStatus.RELIABLY_FEASIBLE
        elif lower_bound > slo:
            status = ReliabilityStatus.RELIABLY_INFEASIBLE
        else:
            status = ReliabilityStatus.UNCERTAIN
        intervals[request_class] = RequestClassP95ConfidenceInterval(
            request_class=request_class,
            slo=slo,
            replication_count=len(p95_values),
            mean_p95=mean_p95,
            standard_deviation=standard_deviation,
            standard_error=standard_error,
            confidence_level=per_interval_confidence,
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            half_width=half_width,
            relative_half_width=relative_half_width,
            status=status,
            replication_p95_values=tuple(p95_values),
            measured_request_counts=tuple(request_counts),
        )

    statuses = {interval.status for interval in intervals.values()}
    if statuses == {ReliabilityStatus.RELIABLY_FEASIBLE}:
        overall_status = ReliabilityStatus.RELIABLY_FEASIBLE
    elif ReliabilityStatus.RELIABLY_INFEASIBLE in statuses:
        overall_status = ReliabilityStatus.RELIABLY_INFEASIBLE
    else:
        overall_status = ReliabilityStatus.UNCERTAIN
    return RequestClassP95Reliability(
        replica_configuration=configuration,
        seeds=seeds,
        warmup_duration=warmup_duration,
        measurement_duration=measurement_duration,
        family_confidence_level=confidence_level,
        per_interval_confidence_level=per_interval_confidence,
        multiple_class_adjustment=(
            "bonferroni" if adjust_for_multiple_classes else "none"
        ),
        request_class_intervals=intervals,
        status=overall_status,
    )

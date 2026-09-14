"""Small Student-t helpers used by portable confidence bounds."""

import math


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
        coefficient = iteration * (b - iteration) * x / ((qam + twice) * (a + twice))
        d = 1.0 + coefficient * d
        if abs(d) < minimum:
            d = minimum
        c = 1.0 + coefficient / c
        if abs(c) < minimum:
            c = minimum
        d = 1.0 / d
        result *= d * c

        coefficient = -(
            (a + iteration) * (qab + iteration) * x / ((a + twice) * (qap + twice))
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
    return 1.0 - leading * _beta_continued_fraction(b, a, 1.0 - x) / b


def student_t_cdf(value, degrees_of_freedom):
    if (
        isinstance(degrees_of_freedom, bool)
        or not isinstance(degrees_of_freedom, int)
        or degrees_of_freedom <= 0
    ):
        raise ValueError("degrees_of_freedom must be a positive integer.")
    value = float(value)
    if not math.isfinite(value):
        return 0.0 if value < 0 else 1.0
    if value == 0.0:
        return 0.5
    transformed = degrees_of_freedom / (degrees_of_freedom + value * value)
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

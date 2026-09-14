import math

import pytest

from metrics import student_t_cdf, student_t_quantile


def test_student_t_matches_known_critical_values():
    assert student_t_quantile(0.975, 29) == pytest.approx(
        2.045229642,
        rel=1e-9,
    )
    assert student_t_quantile(0.975, 1) == pytest.approx(
        12.706204736,
        rel=1e-9,
    )
    assert student_t_cdf(student_t_quantile(0.995, 29), 29) == pytest.approx(
        0.995,
        abs=1e-12,
    )


@pytest.mark.parametrize("degrees_of_freedom", (True, 0, -1, 1.5))
def test_student_t_rejects_invalid_degrees_of_freedom(degrees_of_freedom):
    with pytest.raises(ValueError, match="positive integer"):
        student_t_cdf(0.0, degrees_of_freedom)


def test_student_t_handles_symmetry_and_infinite_values():
    assert student_t_quantile(0.025, 29) == pytest.approx(
        -student_t_quantile(0.975, 29)
    )
    assert student_t_cdf(-math.inf, 4) == 0.0
    assert student_t_cdf(math.inf, 4) == 1.0

#include "kubesizer/custom_distribution.h"

#include <cmath>
#include <limits>

namespace {

double invalid_sample() {
    return std::numeric_limits<double>::quiet_NaN();
}

}  // namespace

extern "C" int kubesizer_custom_distribution_abi_version() {
    return KUBESIZER_CUSTOM_DISTRIBUTION_ABI_VERSION;
}

extern "C" double kubesizer_unit_multiplier(
    kubesizer_uniform_random_fn,
    void*,
    const double*,
    size_t parameter_count
) {
    return parameter_count == 0 ? 1.0 : invalid_sample();
}

extern "C" double kubesizer_two_point_multiplier(
    kubesizer_uniform_random_fn uniform_random,
    void* random_state,
    const double* parameters,
    size_t parameter_count
) {
    if (uniform_random == nullptr || parameters == nullptr || parameter_count != 3) {
        return invalid_sample();
    }
    const double low = parameters[0];
    const double high = parameters[1];
    const double low_probability = parameters[2];
    if (
        !std::isfinite(low)
        || !std::isfinite(high)
        || !std::isfinite(low_probability)
        || low <= 0.0
        || high <= 0.0
        || low_probability < 0.0
        || low_probability > 1.0
    ) {
        return invalid_sample();
    }
    return uniform_random(random_state) < low_probability ? low : high;
}

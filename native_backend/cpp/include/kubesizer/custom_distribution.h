#ifndef KUBESIZER_CUSTOM_DISTRIBUTION_H
#define KUBESIZER_CUSTOM_DISTRIBUTION_H

#include <stddef.h>

#define KUBESIZER_CUSTOM_DISTRIBUTION_ABI_VERSION 1

#ifdef __cplusplus
extern "C" {
#endif

typedef double (*kubesizer_uniform_random_fn)(void* random_state);

typedef int (*kubesizer_custom_abi_version_fn)(void);

/* Every plugin must export this exact symbol. */
int kubesizer_custom_distribution_abi_version(void);

/*
 * Return one positive, finite, dimensionless sample with expected mean 1.
 * The simulator multiplies the result by the workload's declared mean.
 * Implementations may call uniform_random repeatedly and must not retain
 * random_state or let exceptions cross this C ABI boundary.
 */
typedef double (*kubesizer_custom_sample_fn)(
    kubesizer_uniform_random_fn uniform_random,
    void* random_state,
    const double* parameters,
    size_t parameter_count
);

#ifdef __cplusplus
}
#endif

#endif

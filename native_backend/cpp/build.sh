#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -z "${KUBESIZER_NATIVE_PREFIX:-}" ]]; then
    if [[ -n "${CONDA_PREFIX:-}" && -x "${CONDA_PREFIX}/bin/cmake" ]]; then
        KUBESIZER_NATIVE_PREFIX="${CONDA_PREFIX}"
    else
        KUBESIZER_NATIVE_PREFIX="/home/ktistos/miniconda3/envs/simulation"
    fi
fi
export KUBESIZER_NATIVE_PREFIX
cmake_bin="${KUBESIZER_NATIVE_PREFIX}/bin/cmake"

if [[ ! -x "${cmake_bin}" ]]; then
    echo "CMake was not found at ${cmake_bin}. Set KUBESIZER_NATIVE_PREFIX." >&2
    exit 2
fi

"${cmake_bin}" -S "${script_dir}" -B "${script_dir}/build-release" -DCMAKE_BUILD_TYPE=Release
"${cmake_bin}" --build "${script_dir}/build-release" --parallel

if [[ "${1:-}" == "--sanitizers" ]]; then
    "${cmake_bin}" -S "${script_dir}" -B "${script_dir}/build-sanitize" -DCMAKE_BUILD_TYPE=Debug -DENABLE_SANITIZERS=ON
    "${cmake_bin}" --build "${script_dir}/build-sanitize" --parallel
fi

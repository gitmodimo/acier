#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

if [[ $# -ne 4 ]]; then
  echo "Usage: $0 ARROW_SOURCE BUILD_DIRECTORY INSTALL_PREFIX release|sanitizers" >&2
  exit 2
fi

ci_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source-path=SCRIPTDIR
# shellcheck source=arrow.env
source "$ci_dir/arrow.env"
arrow_source=$(realpath "$1")
build_directory=$(realpath -m "$2")
install_prefix=$(realpath -m "$3")
mode=$4

actual_revision=$(git -C "$arrow_source" rev-parse HEAD)
if [[ $actual_revision != "$ARROW_REVISION" ]]; then
  echo "Expected Arrow $ARROW_REVISION; found $actual_revision" >&2
  exit 1
fi
if ! git -C "$arrow_source" diff --quiet HEAD -- cpp .env; then
  echo "The pinned Arrow source contains modifications" >&2
  exit 1
fi

case "$mode" in
  release)
    build_type=Release
    sanitizers=OFF
    ;;
  sanitizers)
    build_type=Debug
    sanitizers=ON
    ;;
  *) echo "Unknown configuration: $mode" >&2; exit 2 ;;
esac

cmake -S "$arrow_source/cpp" -B "$build_directory" -G Ninja \
  -DCMAKE_BUILD_TYPE="$build_type" \
  -DCMAKE_CXX_FLAGS_DEBUG="-O1 -g1 -fno-omit-frame-pointer" \
  -DCMAKE_INSTALL_PREFIX="$install_prefix" \
  -DCMAKE_INSTALL_LIBDIR=lib \
  -DARROW_BUILD_SHARED=ON \
  -DARROW_BUILD_STATIC=ON \
  -DARROW_POSITION_INDEPENDENT_CODE=ON \
  -DARROW_ACERO=ON \
  -DARROW_COMPUTE=ON \
  -DARROW_DATASET=ON \
  -DARROW_FILESYSTEM=ON \
  -DARROW_IPC=ON \
  -DARROW_SUBSTRAIT=OFF \
  -DARROW_BUILD_TESTS=OFF \
  -DARROW_BUILD_BENCHMARKS=OFF \
  -DARROW_TESTING=OFF \
  -DARROW_DEPENDENCY_SOURCE=BUNDLED \
  -DARROW_JEMALLOC=OFF \
  -DARROW_MIMALLOC=OFF \
  -DARROW_WITH_RE2=OFF \
  -DARROW_WITH_UTF8PROC=OFF \
  -DARROW_WITH_BACKTRACE=OFF \
  -DARROW_SIMD_LEVEL=NONE \
  -DARROW_RUNTIME_SIMD_LEVEL=NONE \
  -DARROW_USE_CCACHE=OFF \
  -DARROW_USE_ASAN="$sanitizers" \
  -DARROW_USE_UBSAN="$sanitizers"
cmake --build "$build_directory" --parallel "${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
cmake --install "$build_directory"
printf '%s\n' "$ARROW_REVISION" > "$install_prefix/acier-arrow-revision"

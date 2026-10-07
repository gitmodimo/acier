#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "Usage: $0 ARROW_INSTALL_PREFIX BUILD_DIRECTORY release|sanitizers" >&2
  exit 2
fi

ci_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
source_directory=$(dirname "$ci_dir")
arrow_prefix=$(realpath "$1")
build_root=$(realpath -m "$2")
mode=$3
parallel=${CMAKE_BUILD_PARALLEL_LEVEL:-2}
extra_flags=()

case "$mode" in
  release)
    build_type=Release
    linkages=(SHARED STATIC)
    ;;
  sanitizers)
    build_type=Debug
    linkages=(SHARED)
    extra_flags+=(
      "-DCMAKE_CXX_FLAGS=${CXXFLAGS:-} -fsanitize=address,undefined -fno-sanitize-recover=all -fno-omit-frame-pointer"
      "-DCMAKE_CXX_FLAGS_DEBUG=-O1 -g1"
      "-DCMAKE_EXE_LINKER_FLAGS=-fsanitize=address,undefined"
      "-DCMAKE_SHARED_LINKER_FLAGS=-fsanitize=address,undefined"
    )
    export ASAN_OPTIONS=detect_leaks=1:abort_on_error=1
    export UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1
    ;;
  *) echo "Unknown configuration: $mode" >&2; exit 2 ;;
esac

arrow_library_path="$arrow_prefix/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
for linkage in "${linkages[@]}"; do
  build_directory="$build_root/${linkage,,}"
  install_prefix="$build_directory/install"
  shared=OFF
  if [[ $linkage == SHARED ]]; then shared=ON; fi
  export LD_LIBRARY_PATH="$arrow_library_path"
  cmake -S "$source_directory" -B "$build_directory" -G Ninja \
    -DCMAKE_BUILD_TYPE="$build_type" \
    -DCMAKE_PREFIX_PATH="$arrow_prefix" \
    -DCMAKE_INSTALL_PREFIX="$install_prefix" \
    -DCMAKE_INSTALL_LIBDIR=lib \
    -DCMAKE_FIND_USE_PACKAGE_REGISTRY=OFF \
    -DCMAKE_FIND_USE_SYSTEM_PACKAGE_REGISTRY=OFF \
    -DACIER_BUILD_TESTS=ON \
    -DACIER_ARROW_LINKAGE="$linkage" \
    -DBUILD_SHARED_LIBS="$shared" \
    "${extra_flags[@]}"
  cmake --build "$build_directory" --parallel "$parallel"
  ctest --test-dir "$build_directory" --output-on-failure --no-tests=error \
    --parallel "$parallel" --timeout 120
  cmake --install "$build_directory"

  # Consume the installed export in a separate CMake project, using only its
  # public target and installed Arrow package. This also checks static linkage.
  consumer_directory="$build_directory/consumer"
  export LD_LIBRARY_PATH="$install_prefix/lib:$arrow_library_path"
  cmake -S "$source_directory/tests/installed" -B "$consumer_directory" -G Ninja \
    -DCMAKE_BUILD_TYPE="$build_type" \
    -DCMAKE_PREFIX_PATH="$install_prefix;$arrow_prefix" \
    -DCMAKE_FIND_USE_PACKAGE_REGISTRY=OFF \
    -DCMAKE_FIND_USE_SYSTEM_PACKAGE_REGISTRY=OFF \
    "${extra_flags[@]}"
  cmake --build "$consumer_directory" --parallel "$parallel"
  ctest --test-dir "$consumer_directory" --output-on-failure --no-tests=error \
    --timeout 120
done

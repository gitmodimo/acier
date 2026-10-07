# Package and dependencies

Comparison baseline: Apache Arrow 26 RC, commit `446167169a0dc547e00e1bc29f417a40c0ca267a`.

## Relationship to upstream

- Acier is a separate compiled library, exported as `Acier::acier`, built against
  installed Arrow, Acero, Compute and Dataset packages. It needs no Arrow source
  checkout or Substrait package.
- Arrow execution-plan and ordinary data types retain their original identities.
  Acier owns its extension factories, options and implementations.
- `BUILD_SHARED_LIBS` selects Acier linkage independently of `ACIER_ARROW_LINKAGE`,
  whose values are `AUTO`, `SHARED` and `STATIC`. Installed exports retain the
  selected Arrow linkage and require the Arrow version used at compilation.
- Rebuild Acier when Arrow changes. Matching version strings alone do not prove
  binary compatibility between different builds.
- C++17 is the minimum; the build inherits a newer standard required by Arrow.
  Apache-2.0 licensing and Arrow attribution are retained.
- Header checks compile common public headers independently and together in both
  orders. Optional format wrappers are excluded from this common-header target.
  The installed consumer checks the exported target in a separate CMake project.

## Implementation and coverage

- Acier: [CMakeLists.txt](../../CMakeLists.txt), [cmake/AcierConfig.cmake.in](../../cmake/AcierConfig.cmake.in), [NOTICE.txt](../../NOTICE.txt).
- Regression coverage: [tests/headers.cmake](../../tests/headers.cmake), [tests/installed/CMakeLists.txt](../../tests/installed/CMakeLists.txt).

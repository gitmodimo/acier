# Continuous integration

Comparison baseline: Apache Arrow 26 RC, commit `446167169a0dc547e00e1bc29f417a40c0ca267a`.

## Relationship to upstream validation

- CI builds one immutable upstream Arrow revision, recorded in `ci/arrow.env`.
  Its Arrow 25.0.1 pin is separate from the Arrow 26 RC comparison baseline of
  these component pages; the pages do not imply a changed CI dependency.
- The release job builds shared and static configurations. The Clang Debug job
  instruments both Arrow and Acier with AddressSanitizer and UndefinedBehaviorSanitizer.
- Each configuration builds the common-header checks, runs all eight runtime
  suites, installs Acier and runs a separate installed-package consumer.
- The workflow has read-only permissions and no publishing step. Arrow install
  caches are keyed by revision, build scripts, mode and toolchain details.
- Local reproduction instructions are in the [developer guide](../README.md).
  CI coverage does not establish Windows, ARM64, no-thread Arrow builds or
  representative application performance.

## Implementation and coverage

- Acier: [.github/workflows/ci.yml](../../.github/workflows/ci.yml), [ci/arrow.env](../../ci/arrow.env), [ci/build-arrow.sh](../../ci/build-arrow.sh), [ci/test.sh](../../ci/test.sh).
- Regression coverage: [tests/installed/CMakeLists.txt](../../tests/installed/CMakeLists.txt).

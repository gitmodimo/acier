# Platform and tracing boundaries

Comparison baseline: Apache Arrow 26 RC, commit `446167169a0dc547e00e1bc29f417a40c0ca267a`.

## Relationship to upstream

- Acier consumes installed Arrow packages and does not patch Arrow's Windows
  compatibility headers or OpenTelemetry export annotations.
- Acier supplies its own shared-library visibility macro; Arrow's platform and
  tracing configuration remains a property of the selected Arrow build.
- CI covers Linux shared/static release builds and a shared sanitizer configuration.
  It does not establish Windows or OpenTelemetry-enabled build/runtime support.
- Internal tracing symbols are not an Acier public API, even when Arrow exports
  them. See [package](package.md) and [CI](ci.md) for build boundaries.

## Implementation and coverage

- Acier: [CMakeLists.txt](../../CMakeLists.txt), [include/acier/visibility.h](../../include/acier/visibility.h), [.github/workflows/ci.yml](../../.github/workflows/ci.yml).

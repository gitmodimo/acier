# Backpressure composition

Comparison baseline: Apache Arrow 26 RC, commit `446167169a0dc547e00e1bc29f417a40c0ca267a`.
Upstream reference: [acero/backpressure_handler.h](https://github.com/apache/arrow/blob/446167169a0dc547e00e1bc29f417a40c0ca267a/cpp/src/arrow/acero/backpressure_handler.h).

## Additions to upstream Acero

- Acier provides `BackpressureController`, `BackpressureControlWrapper`,
  `BackpressureCombiner` and its `Source` API. These composition helpers are not
  present in the upstream baseline.
- Arrow's `BackpressureControl` and `BackpressureHandler` remain shared types;
  Acier does not replace them.
- A combiner pauses when any source pauses by default, or can be configured to
  pause only when all sources pause. Stop releases pressure and ignores later pauses.
- Controlled callbacks run outside the combiner mutex. Delivery is serialized and
  state is reconciled after synchronous callback reentry.

## Boundaries and coverage

Connections are established before concurrent use. Controllers and source objects
must outlive their use by the combiner; there is no dynamic disconnection API.
Tests cover any/all combinations, repeated transitions, stop and callback reentry.

## Implementation and coverage

- Acier: [include/acier/backpressure.h](../../include/acier/backpressure.h), [src/pipeline/backpressure.cc](../../src/pipeline/backpressure.cc).
- Regression coverage: [tests/pipeline_test.cc](../../tests/pipeline_test.cc).

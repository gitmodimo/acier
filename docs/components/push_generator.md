# PushGenerator backpressure

Comparison baseline: Apache Arrow 26 RC, commit `446167169a0dc547e00e1bc29f417a40c0ca267a`.
Upstream reference: [util/async_generator.h](https://github.com/apache/arrow/blob/446167169a0dc547e00e1bc29f417a40c0ca267a/cpp/src/arrow/util/async_generator.h).

## Differences from upstream

- `acier::PushGenerator<T>` adds construction with an Acero `BackpressureHandler`;
  upstream's PushGenerator has only its default constructor.
- Buffered push/pop transitions notify the handler in mutation order. Direct
  delivery to a waiting consumer does not change queue size.
- Notifications and future completion run outside the queue mutex. Reentrant
  notifications are serialized; handlers must not throw.
- A second simultaneously waiting consumer receives an `Invalid` result.

## Shared behavior and boundaries

- The producer holds weak state. Generator destruction invalidates the producer;
  close rejects new pushes and allows queued values to drain.
- Arrow futures, errors and the existing BackpressureHandler type are reused.
- Tests cover watermark crossings, direct delivery, close/drain, expired producers,
  errors, duplicate waiters, reentry and concurrent ordered producer/consumer use.

## Implementation and coverage

- Acier: [include/acier/util/async_generator.h](../../include/acier/util/async_generator.h).
- Regression coverage: [tests/generator_test.cc](../../tests/generator_test.cc).

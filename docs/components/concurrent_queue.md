# Concurrent queues

Comparison baseline: Apache Arrow 26 RC, commit `446167169a0dc547e00e1bc29f417a40c0ca267a`.
Upstream reference: [acero/concurrent_queue_internal.h](https://github.com/apache/arrow/blob/446167169a0dc547e00e1bc29f417a40c0ca267a/cpp/src/arrow/acero/concurrent_queue_internal.h).

## Differences from upstream

- Acier exposes owned `ConcurrentQueue` and `BackpressureConcurrentQueue`
  templates in a public header; Arrow's corresponding implementation is internal.
- Backpressure notifications run outside the queue mutex, with serialized delivery
  and reconciliation after reentrant changes.
- Shutdown state and push are checked under the same lock, preventing a late push
  from repopulating a queue after shutdown clears it.

## Shared behavior and boundaries

- FIFO operations and high/low watermark semantics are retained.
- `ForceShutdown` clears queued values and rejects subsequent pushes. It does not
  wake a blocked `WaitAndPop` or create a completion sentinel; users must arrange
  completion of blocked consumers separately.
- `Front` returns a reference whose validity requires protection against removal.
- Tests cover FIFO order, watermark transitions, shutdown, late pushes and a pause
  callback that reenters the queue to clear it.

## Implementation and coverage

- Acier: [include/acier/concurrent_queue.h](../../include/acier/concurrent_queue.h).
- Regression coverage: [tests/pipeline_test.cc](../../tests/pipeline_test.cc).

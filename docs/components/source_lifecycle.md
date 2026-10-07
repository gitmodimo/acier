# Source pause and stop lifecycle

Comparison baseline: Apache Arrow 26 RC, commit
`446167169a0dc547e00e1bc29f417a40c0ca267a`.
Upstream reference: [acero/source_node.cc](https://github.com/apache/arrow/blob/446167169a0dc547e00e1bc29f417a40c0ca267a/cpp/src/arrow/acero/source_node.cc).

## Differences from upstream

- Acier records stop and releases any pause future under the same mutex, then
  completes the released future outside that mutex. It ignores subsequent pauses.
- In the upstream source, pause release precedes a separate stop-state update;
  `PauseProducing` does not check the stop flag. Acier closes that late-pause window.
- This behavior is shared by `acier_source`, `acier_table_source`,
  `acier_record_batch_source`, `acier_record_batch_reader_source`,
  `acier_exec_batch_source` and `acier_array_vector_source`.
- Acier uses installed Arrow interfaces and omits private fine-grained tracing
  spans. It does not add a named-table serialization stub.

## Shared behavior and boundaries

- Ordinary source options and batching remain Arrow-compatible; reader ordering
  uses the [`acier::RecordBatchReaderSourceNodeOptions`](reader_source.md).
- Stop does not resolve a caller-owned generator future. Outstanding generator
  futures must eventually complete so the plan can finish.
- Tests exercise pending-generator stop with late pauses/stale resumes, iterator
  adapters and error propagation in serial/threaded plans. These deterministic
  cases do not establish that every concurrent interleaving has been tested.

## Implementation and coverage

- Acier: [src/core/source_node.cc](../../src/core/source_node.cc).
- Regression coverage: [tests/core_test.cc](../../tests/core_test.cc).

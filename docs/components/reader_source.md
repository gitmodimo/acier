# RecordBatchReader source ordering

Comparison baseline: Apache Arrow 26 RC, commit
`446167169a0dc547e00e1bc29f417a40c0ca267a`.
Upstream reference: [acero/source_node.cc](https://github.com/apache/arrow/blob/446167169a0dc547e00e1bc29f417a40c0ca267a/cpp/src/arrow/acero/source_node.cc).

## Differences from upstream

- `acier_record_batch_reader_source` accepts
  `acier::RecordBatchReaderSourceNodeOptions` with a third
  `implicit_ordering` argument, defaulting to `false`. Explicit `true` advertises
  implicit ordering; false/default advertises unordered input.
- `arrow::acero::RecordBatchReaderSourceNodeOptions` takes reader and executor
  arguments; the upstream reader source always
  advertises implicit ordering.
- Acier uses its shared source lifecycle implementation; see
  [source lifecycle](source_lifecycle.md) for stop/pause differences.
- With no explicit executor, Acier obtains the executor from the plan's public
  IOContext rather than including Arrow's private I/O utility header.

## Coverage

Reader row sequence, ordering for both flag values and the default, null-reader
rejection, and serial/threaded execution are covered by the core suite.

## Implementation and coverage

- Acier: [include/acier/core_options.h](../../include/acier/core_options.h), [src/core/source_node.cc](../../src/core/source_node.cc).
- Regression coverage: [tests/core_test.cc](../../tests/core_test.cc).

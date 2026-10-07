# Dataset writing

Comparison baseline: Apache Arrow 26 RC, commit
`446167169a0dc547e00e1bc29f417a40c0ca267a`.
Upstream reference: [dataset/file_base.cc](https://github.com/apache/arrow/blob/446167169a0dc547e00e1bc29f417a40c0ca267a/cpp/src/arrow/dataset/file_base.cc).

## Differences from upstream

- `acier_write` uses `acier::dataset::WriteNodeOptions` exposing `max_rows_queued`,
  default
  8,388,608 rows. Zero is rejected. `arrow::dataset::WriteNodeOptions` does not expose
  this writer threshold.
- Acier owns the asynchronous writer helper and terminal node. Stopping the node
  reaches an explicit abort path that releases row/open-file waiters with an error.
- Writer task failures preserve the originating status, release throttled work,
  and drop writer task-group ownership. Error cleanup is chained into task
  completion so the task cannot retire before cleanup finishes.
- The owned writer does not call Arrow's internal DatasetWriter class; it uses
  installed filesystem, format and scheduling interfaces.

## Shared behavior and boundaries

- Partitioning, row-group/file limits, custom schema/metadata and ordered writes
  remain available. The threshold is flow control, not a strict allocation bound;
  an oversized batch is accepted without requiring it to fit below the threshold.
- Cancellation does not certify partially written files as complete. Outstanding
  filesystem operations must finish before plan destruction.
- IPC readback tests cover partition order, schema/metadata, small thresholds,
  oversized input, writer-finalization failures, cancellation and observer lifetime.
  These tests do not cover every file format or filesystem backend.

## Implementation and coverage

- Acier: [include/acier/dataset/write_options.h](../../include/acier/dataset/write_options.h), [src/dataset/write_node.cc](../../src/dataset/write_node.cc), [src/dataset/writer.cc](../../src/dataset/writer.cc), [src/dataset/writer_task.h](../../src/dataset/writer_task.h).
- Regression coverage: [tests/dataset_test.cc](../../tests/dataset_test.cc).

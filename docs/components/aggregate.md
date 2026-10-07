# Ordered and segmented aggregation

Comparison baseline: Apache Arrow 26 RC, commit
`446167169a0dc547e00e1bc29f417a40c0ca267a`.
Upstream reference: [acero/groupby_aggregate_node.cc](https://github.com/apache/arrow/blob/446167169a0dc547e00e1bc29f417a40c0ca267a/cpp/src/arrow/acero/groupby_aggregate_node.cc).

## Differences from upstream

- `acier_aggregate` supports segmented and ordered aggregation in a threaded plan.
  Upstream rejects segmented aggregation and ordered aggregate kernels when the
  execution context has multiple threads.
- Acier serializes ordered processing into one state slot, preserving input batch
  order when processing moves between executor workers. Unordered aggregation can
  still use worker-local state.
- Acier connects sequenced processing to upstream pause/resume through its
  backpressure helpers. These are cooperative thresholds, not a hard memory limit.
- Segment output batches have contiguous indices; segment sort fields refer to
  their positions in the output schema.

## Shared behavior and boundaries

- `acier::AggregateNodeOptions` aliases `arrow::acero::AggregateNodeOptions`; kernels,
  grouping and segmentation
  primitives come from Arrow. The original `aggregate` factory remains available.
- Empty segmented scalar input produces no rows; ordinary scalar aggregation
  retains its one-row empty-input result.
- Tests cover multi-batch grouped output, first/last across segment boundaries,
  empty input and serial/threaded execution.

## Implementation and coverage

- Acier: [src/pipeline/groupby_aggregate_node.cc](../../src/pipeline/groupby_aggregate_node.cc), [src/pipeline/scalar_aggregate_node.cc](../../src/pipeline/scalar_aggregate_node.cc), [src/pipeline/aggregate_internal.h](../../src/pipeline/aggregate_internal.h).
- Regression coverage: [tests/pipeline_test.cc](../../tests/pipeline_test.cc).

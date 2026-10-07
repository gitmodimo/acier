# Scheduler integration

Comparison baseline: Apache Arrow 26 RC, commit `446167169a0dc547e00e1bc29f417a40c0ca267a`.
Upstream reference: [util/async_util.cc](https://github.com/apache/arrow/blob/446167169a0dc547e00e1bc29f417a40c0ca267a/cpp/src/arrow/util/async_util.cc).

## Relationship to upstream

- Acier uses Arrow's scheduler and QueryContext; it does not replace or patch
  Arrow's scheduler implementation or add a separate task-tracking API.
- Node-specific scheduling, error handling, pause and stop behavior live in the
  corresponding Acier components. See [sequencing](sequencing.md),
  [AsofJoin](asofjoin.md) and [SortedMerge](sorted_merge.md).
- Component tests cover task-driven completion and errors. This is not a separate
  scheduler conformance suite or a claim that all scheduler races are covered.

## Implementation and coverage

- Acier: [src/pipeline/accumulation_queue.cc](../../src/pipeline/accumulation_queue.cc), [src/join/asof_join_node.cc](../../src/join/asof_join_node.cc), [src/join/sorted_merge_node.cc](../../src/join/sorted_merge_node.cc).
- Regression coverage: [tests/pipeline_test.cc](../../tests/pipeline_test.cc), [tests/join_test.cc](../../tests/join_test.cc).

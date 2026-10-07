# SortedMerge

Comparison baseline: Apache Arrow 26 RC, commit
`446167169a0dc547e00e1bc29f417a40c0ca267a`.
Upstream reference: [acero/sorted_merge_node.cc](https://github.com/apache/arrow/blob/446167169a0dc547e00e1bc29f417a40c0ca267a/cpp/src/arrow/acero/sorted_merge_node.cc).

## Differences from upstream

- `acier_sorted_merge` uses plan-scheduled work and implements downstream
  pause/resume. Upstream uses a dedicated processing thread when threading is
  enabled and has empty downstream pause/resume handlers.
- Acier validates actual values and null ordering across batches, batch indices
  and completion counts. Input ordering metadata need not advertise a sort order
  when the data and indices satisfy the requested order.
- Acier limits output batches to `ExecPlan::kMaxBatchSize` and assigns consecutive
  output indices. Upstream does not impose that output batch cap.
- Acier preserves nested and sliced payloads using installed Arrow array APIs,
  without depending on Arrow's private composite-table helper. Scalar payloads
  are materialized as arrays.
- Acier honors effective null placement, including an ordering-wide override where
  the Arrow ordering API provides one.

## Shared behavior and boundaries

- `acier::SortedMergeNodeOptions` aliases `arrow::acero::OrderByNodeOptions`. Exactly
  one ascending top-level
  integer/date/time/timestamp key is supported, and input schemas must match.
- Every input must be sorted. Acier rejects out-of-order values; it does not repair
  or sort an invalid input stream. Equal-key rows across inputs have no stable
  relative-order guarantee.
- Backpressure counts sequenced non-empty batches, with low/high thresholds of
  four/eight. Batches waiting for missing indices are outside that count.
- In-flight output can finish after pause. Once all declared input batches arrive
  and are sequenced, the remaining tail drains without requiring resume.
- Tests cover null placement, signed limits, nested/scalar data, empty input,
  oversized batches, malformed ordering/indices/counts and lifecycle errors.

## Implementation and coverage

- Acier: [include/acier/join_options.h](../../include/acier/join_options.h), [src/join/sorted_merge_node.cc](../../src/join/sorted_merge_node.cc), [src/join/join_internal.h](../../src/join/join_internal.h).
- Regression coverage: [tests/join_test.cc](../../tests/join_test.cc).

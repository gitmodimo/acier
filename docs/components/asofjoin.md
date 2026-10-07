# AsofJoin

Comparison baseline: Apache Arrow 26 RC, commit
`446167169a0dc547e00e1bc29f417a40c0ca267a`.
Upstream reference: [acero/asof_join_node.cc](https://github.com/apache/arrow/blob/446167169a0dc547e00e1bc29f417a40c0ca267a/cpp/src/arrow/acero/asof_join_node.cc).

## Differences from upstream

- `acier_asofjoin` uses `acier::AsofJoinNodeOptions`. In addition to a signed
  one-sided tolerance,
  it accepts an inclusive `[lower, upper]` interval and a preference for the earlier
  or later match when distances are equal. `arrow::acero::AsofJoinNodeOptions` exposes a
  single signed tolerance.
- Appended right-side fields are nullable, including fields declared non-nullable
  on input. Upstream copies the input field declarations into its output schema.
- Acier preserves left-side batches, indices and ordering, including empty batches
  and scalar values. It emits one output per left batch rather than splitting a
  left batch into currently joinable fragments.
- Acier uses plan-scheduled work and implements downstream pause/resume. Upstream
  uses a dedicated processing thread when threading is enabled and has empty
  downstream pause/resume handlers.
- Null left times never match; null right times are skipped. Acier checks ordering
  and handles signed time limits without overflow. Explicit ordering's null policy
  is respected; implicitly ordered inputs can contain null times anywhere.
- Acier materializes nested/sliced payloads through installed Arrow array APIs and
  uses public scalar hashing plus exact key comparison, without private Arrow
  hashing or composite-table implementation headers.

## Contract boundaries

- The signed tolerance constructor includes both endpoints and uses the on-key's
  units. Interval matching chooses the nearest eligible time.
- For an interval ending at or before zero, duplicate eligible right times select
  the last row. With a future interval, exact/future duplicates select the first
  row; strictly past duplicates select the last.
- A pause permits the active left batch to finish. Once all left batches arrive,
  remaining output can drain without resume. Each input batch is limited to
  `INT32_MAX` rows; there is no smaller output batch cap.
- Tests cover matching, ties, nulls, nested payloads, ordering, schemas, batch
  indices, pause/stop/error paths and serial/threaded plans. They do not establish
  a performance advantage over upstream.

## Implementation and coverage

- Acier: [include/acier/join_options.h](../../include/acier/join_options.h), [src/join/asof_join_node.cc](../../src/join/asof_join_node.cc), [src/join/join_internal.h](../../src/join/join_internal.h).
- Regression coverage: [tests/join_test.cc](../../tests/join_test.cc).

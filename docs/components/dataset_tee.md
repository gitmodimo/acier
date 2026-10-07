# Dataset Tee

Comparison baseline: Apache Arrow 26 RC, commit
`446167169a0dc547e00e1bc29f417a40c0ca267a`.
Upstream reference: [dataset/file_base.cc](https://github.com/apache/arrow/blob/446167169a0dc547e00e1bc29f417a40c0ca267a/cpp/src/arrow/dataset/file_base.cc).

## Differences from upstream

- `acier_tee` uses `acier::dataset::WriteNodeOptions` for custom schema/metadata and the
  configurable queued-row threshold. The upstream Tee uses the input schema
  directly and does not apply the custom schema/metadata fields in
  `arrow::dataset::WriteNodeOptions`.
- Acier shares the [owned writer and abort behavior](dataset_write.md) with
  `acier_write`; stopping Tee aborts pending writer work.
- Writer pressure and downstream pressure are combined independently. A resume
  from one side does not cancel an outstanding pause from the other.
- Acier reads `preserve_order` from stored options after construction, avoiding
  dependence on a moved-from constructor argument.

## Shared behavior and boundaries

- Upstream already supports ordered Tee processing and rejects preserve-order
  requests on unordered input; those are shared capabilities.
- Tee forwards batches while writing them. Tests check forwarded/written values,
  partitioning, schema metadata, ordering, cancellation and finalization errors.
- Neither cancellation nor downstream forwarding guarantees a completed file.

## Implementation and coverage

- Acier: [include/acier/dataset/write_options.h](../../include/acier/dataset/write_options.h), [src/dataset/write_node.cc](../../src/dataset/write_node.cc).
- Regression coverage: [tests/dataset_test.cc](../../tests/dataset_test.cc).

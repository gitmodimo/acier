# PivotLonger scalar features

Comparison baseline: Apache Arrow 26 RC, commit
`446167169a0dc547e00e1bc29f417a40c0ca267a`.
Upstream reference: [acero/pivot_longer_node.cc](https://github.com/apache/arrow/blob/446167169a0dc547e00e1bc29f417a40c0ca267a/cpp/src/arrow/acero/pivot_longer_node.cc).

## Differences from upstream

- `acier_pivot_longer` uses `acier::PivotLongerRowTemplate` and
  `acier::PivotLongerNodeOptions`. Arrow 26 RC already supports scalar feature
  values; this capability is shared, not an Acier-only extension.
- Acier supplies the scalar feature interface with older supported Arrow builds
  too. The CI-pinned Arrow 25.0.1 has string-only feature vectors.
- Mixed feature types return `Invalid` in Acier, whereas the Arrow 26 RC node
  returns `TypeError`. Acier checks for `acier::PivotLongerNodeOptions` before reading
  the options.
- `arrow::acero::PivotLongerRowTemplate` additionally provides a legacy
  string-vector constructor; `acier::PivotLongerRowTemplate` accepts scalar vectors.

## Shared behavior and boundaries

- Feature columns retain their scalar types, including typed null values.
  A null scalar pointer is rejected. Output is unordered.
- Tests cover numeric/typed-null features, missing measurements, row multiplicity,
  schema validity and null-pointer rejection in serial/threaded plans.

## Implementation and coverage

- Acier: [include/acier/core_options.h](../../include/acier/core_options.h), [src/core/pivot_node.cc](../../src/core/pivot_node.cc).
- Regression coverage: [tests/core_test.cc](../../tests/core_test.cc).

# Non-null filter

Comparison baseline: Apache Arrow 26 RC, commit
`446167169a0dc547e00e1bc29f417a40c0ca267a`.
Upstream reference: [acero/filter_node.cc](https://github.com/apache/arrow/blob/446167169a0dc547e00e1bc29f417a40c0ca267a/cpp/src/arrow/acero/filter_node.cc).

## Differences from upstream

- `acier_filter` accepts `acier::FilterNodeOptions` with a `filter_not_null`
  vector of top-level field names. `arrow::acero::FilterNodeOptions` contains only an
  expression.
- Acier combines the expression with non-null predicates for those fields and
  marks the selected output fields non-nullable. An ordinary filter expression
  alone does not supply this schema refinement.
- Missing or ambiguous names return `KeyError`; passing
  `arrow::acero::FilterNodeOptions` to `acier_filter` returns `Invalid`.

## Shared behavior and boundaries

- An empty name vector retains ordinary expression filtering. Arrow's public
  expression/filter APIs perform evaluation; schema metadata is preserved.
- The name vector does not address nested fields. Typed null predicates, scalar
  values, row selection and output nullability are covered by the core suite.

## Implementation and coverage

- Acier: [include/acier/core_options.h](../../include/acier/core_options.h), [src/core/filter_node.cc](../../src/core/filter_node.cc).
- Regression coverage: [tests/core_test.cc](../../tests/core_test.cc).

# STL and optional conversion

Comparison baseline: Apache Arrow 26 RC, commit `446167169a0dc547e00e1bc29f417a40c0ca267a`.
Upstream reference: [stl.h](https://github.com/apache/arrow/blob/446167169a0dc547e00e1bc29f417a40c0ca267a/cpp/src/arrow/stl.h).

## Differences from upstream

- Acier adds optional extraction: null cells become empty optionals and valid
  cells use the wrapped conversion. Upstream supplies optional append support
  but no corresponding optional `GetEntry` implementation.
- Acier derives vector/fixed-size-list child nullability from the C++ element
  type: nonoptional children are non-nullable and optional children are nullable.
  Upstream constructs these list children as nullable regardless of element type.
- Fixed-size-list appends use recursive cell conversion, supporting nested and
  nonprimitive children rather than requiring primitive-style `AppendValues`.
- Acier validates tuple width against the number of supplied column names.

## Shared behavior and boundaries

- Ordinary Arrow primitive/user-defined traits remain available through fallback.
  Acier does not redefine Arrow templates; callers select `acier::stl` explicitly.
- Custom converters that explicitly invoke Arrow conversion for their children
  continue to use Arrow behavior.
- Tests cover nested vectors/arrays, optional children, custom structs, sliced
  tuple/table round trips and exact child/outer nullability, including the
  [TDigest](tdigest.md) centroid schema.

## Implementation and coverage

- Acier: [include/acier/stl.h](../../include/acier/stl.h), [include/acier/type_traits.h](../../include/acier/type_traits.h).
- Regression coverage: [tests/stl_test.cc](../../tests/stl_test.cc).

# TDigest Compute extension

Comparison baseline: Apache Arrow 26 RC, commit
`446167169a0dc547e00e1bc29f417a40c0ca267a`.
Upstream reference: [compute/kernels/aggregate_tdigest.cc](https://github.com/apache/arrow/blob/446167169a0dc547e00e1bc29f417a40c0ca267a/cpp/src/arrow/compute/kernels/aggregate_tdigest.cc).

## Differences from upstream

- Acier adds digest-producing and digest-consuming functions alongside numeric
  quantiles: `acier_tdigest_map`, `acier_tdigest_reduce`, `acier_tdigest_quantile`
  and `acier_tdigest_quantile_element_wise`.
- `acier_tdigest` and `acier_approximate_median` use the same owned algorithm,
  exposing selectable K0/K1 scaling. Original Arrow functions remain registered.
- Acier tests `min_count` against accepted total weight. Upstream counts non-null
  array elements, including NaNs, and counts a scalar span once. These differ for
  NaNs, weighted digests and repeated scalar spans.
- Acier accepts positive compression delta values below 10; upstream internally
  clamps delta to at least 10. Zero delta/buffer size, invalid probabilities/scalers
  and malformed digest weights/endpoints return errors in Acier.
- `acier::compute::TDigestOptions`, `acier::compute::TDigestMapOptions`,
  `acier::compute::TDigestReduceOptions` and `acier::compute::TDigestQuantileOptions`
  have distinct registry names and versioned little-endian serialization. Acier
  registers through Arrow's public Compute kernel interfaces.

## Digest schema and results

```text
struct<
  centroids: list<
    item: struct<mean: double not null, weight: double not null> not null
  > not null,
  min: double nullable,
  max: double nullable
>
```

- Aggregate quantiles return a double array of length `q.size()`; elementwise
  quantiles return one fixed-size list of that length per input digest.
- An empty valid digest has no centroids and null endpoints. Map with
  `skip_nulls=false` returns a null digest when input contains nulls. Reduce and
  aggregate quantile propagate null digests. Elementwise quantile returns a null
  list for a null digest, and a valid list of nulls below `min_count`.
- Tests cover options/registration, numeric and decimal inputs, output shapes,
  weights, null/empty/NaN cases, invalid data, an IPC schema round trip and Acero
  aggregation. Function documentation is explicitly validated in the suite.

## Implementation and coverage

- Acier: [include/acier/compute/api_aggregate.h](../../include/acier/compute/api_aggregate.h), [src/compute/aggregate_tdigest.cc](../../src/compute/aggregate_tdigest.cc), [src/compute/tdigest_internal.cc](../../src/compute/tdigest_internal.cc).
- Regression coverage: [tests/compute_test.cc](../../tests/compute_test.cc).

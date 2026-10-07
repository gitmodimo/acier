// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <acier/visibility.h>
#include <arrow/compute/api_aggregate.h>
#include <arrow/datum.h>
#include <cstdint>
#include <memory>
#include <vector>

namespace acier::compute {

/// Numeric TDigest quantiles. Probabilities q default to {0.5}, are dimensionless,
/// and must be finite and in [0, 1]. delta controls compression and buffer_size
/// counts buffered samples; both must be positive and default to 100 and 500.
/// K0 uses linear compression; K1 (the default) concentrates resolution near the
/// tails. NaNs are ignored. skip_nulls defaults to true; if false, any null makes
/// every result null. min_count is the minimum accepted sample weight (default
/// zero). Empty inputs and weight below min_count produce null quantiles.
class ACIER_EXPORT TDigestOptions : public arrow::compute::FunctionOptions {
 public:
  enum Scaler { K0 = 0, K1 };
  explicit TDigestOptions(double q = 0.5, uint32_t delta = 100,
                          uint32_t buffer_size = 500, bool skip_nulls = true,
                          uint32_t min_count = 0, Scaler scaler = K1);
  explicit TDigestOptions(std::vector<double> q, uint32_t delta = 100,
                          uint32_t buffer_size = 500, bool skip_nulls = true,
                          uint32_t min_count = 0, Scaler scaler = K1);
  static constexpr const char* kTypeName = "AcierTDigestOptions";
  static TDigestOptions Defaults() { return TDigestOptions{}; }
  std::vector<double> q;
  uint32_t delta;
  uint32_t buffer_size;
  bool skip_nulls;
  uint32_t min_count;
  Scaler scaler;
};

/// Compress numeric samples into centroids. delta and buffer_size must be positive
/// and default to 100 and 500. K1 is the default scaler. NaNs are ignored. With
/// skip_nulls=true nulls are ignored; otherwise any null produces a null digest.
class ACIER_EXPORT TDigestMapOptions : public arrow::compute::FunctionOptions {
 public:
  using Scaler = TDigestOptions::Scaler;
  explicit TDigestMapOptions(uint32_t delta = 100, uint32_t buffer_size = 500,
                             bool skip_nulls = true, Scaler scaler = Scaler::K1);
  static constexpr const char* kTypeName = "AcierTDigestMapOptions";
  static TDigestMapOptions Defaults() { return TDigestMapOptions{}; }
  uint32_t delta;
  uint32_t buffer_size;
  bool skip_nulls;
  Scaler scaler;
};

/// Merge centroid sets with positive compression parameter delta (default 100)
/// and K0 or K1 scaling (default K1). Any null input digest makes the result null.
class ACIER_EXPORT TDigestReduceOptions : public arrow::compute::FunctionOptions {
 public:
  using Scaler = TDigestOptions::Scaler;
  explicit TDigestReduceOptions(uint32_t delta = 100, Scaler scaler = Scaler::K1);
  static constexpr const char* kTypeName = "AcierTDigestReduceOptions";
  static TDigestReduceOptions Defaults() { return TDigestReduceOptions{}; }
  uint32_t delta;
  Scaler scaler;
};

/// Quantiles from centroid sets. q defaults to {0.5}; probabilities must be finite
/// and in [0, 1]. delta must be positive (default 100), scaler defaults to K1, and
/// min_count is a minimum total centroid weight (default zero). Empty digests or
/// weight below min_count produce null quantiles. Quantiles have the mean's units.
class ACIER_EXPORT TDigestQuantileOptions : public arrow::compute::FunctionOptions {
 public:
  using Scaler = TDigestOptions::Scaler;
  explicit TDigestQuantileOptions(double q = 0.5, uint32_t delta = 100,
                                  uint32_t min_count = 0, Scaler scaler = Scaler::K1);
  explicit TDigestQuantileOptions(std::vector<double> q, uint32_t delta = 100,
                                  uint32_t min_count = 0, Scaler scaler = Scaler::K1);
  static constexpr const char* kTypeName = "AcierTDigestQuantileOptions";
  static TDigestQuantileOptions Defaults() { return TDigestQuantileOptions{}; }
  std::vector<double> q;
  uint32_t delta;
  uint32_t min_count;
  Scaler scaler;
};

/// The digest type is struct<centroids: list<item: struct<mean: double not null,
/// weight: double not null> not null> not null, min: double, max: double>.
/// min and max are nullable. Empty valid digests have no centroids and null
/// min/max. Nonempty digests require valid non-NaN min/max with min <= max and
/// finite positive weights. Mean, min, and max retain the input units; weights
/// count samples.
ACIER_EXPORT std::shared_ptr<arrow::DataType> TDigestCentroidType();

/// Call Initialize before using these functions. ctx selects the function
/// registry and allocator; nullptr uses Arrow's defaults. Arrays, chunked arrays,
/// and scalars are accepted. TDigest and Map accept integer, floating-point,
/// Decimal128, and Decimal256 samples; Reduce and Quantile accept centroid
/// structs with TDigestCentroidType(). TDigest/TDigestQuantile return a double
/// array with one value per requested probability, in the requested order and
/// the input's units. Map/Reduce return one digest scalar. A null input digest
/// makes an aggregate Reduce result null or all aggregate Quantile values null.
ACIER_EXPORT arrow::Result<arrow::Datum> TDigest(
    const arrow::Datum&, const TDigestOptions& = TDigestOptions::Defaults(),
    arrow::compute::ExecContext* ctx = nullptr);
ACIER_EXPORT arrow::Result<arrow::Datum> TDigestMap(
    const arrow::Datum&, const TDigestMapOptions& = TDigestMapOptions::Defaults(),
    arrow::compute::ExecContext* ctx = nullptr);
ACIER_EXPORT arrow::Result<arrow::Datum> TDigestReduce(
    const arrow::Datum&, const TDigestReduceOptions& = TDigestReduceOptions::Defaults(),
    arrow::compute::ExecContext* ctx = nullptr);
ACIER_EXPORT arrow::Result<arrow::Datum> TDigestQuantile(
    const arrow::Datum&,
    const TDigestQuantileOptions& = TDigestQuantileOptions::Defaults(),
    arrow::compute::ExecContext* ctx = nullptr);

/// Return one fixed-size list of q.size() nullable doubles per input digest.
/// A null digest produces a null list; an empty/insufficient-weight valid digest
/// produces a valid list of nulls. A scalar input produces a list scalar.
ACIER_EXPORT arrow::Result<arrow::Datum> TDigestQuantileElementWise(
    const arrow::Datum&,
    const TDigestQuantileOptions& = TDigestQuantileOptions::Defaults(),
    arrow::compute::ExecContext* ctx = nullptr);

/// Return a scalar median using K1, delta 100, and buffer size 500. NaNs are
/// ignored and do not count toward min_count; null handling follows options.
ACIER_EXPORT arrow::Result<arrow::Datum> ApproximateMedian(
    const arrow::Datum&,
    const arrow::compute::ScalarAggregateOptions& =
        arrow::compute::ScalarAggregateOptions::Defaults(),
    arrow::compute::ExecContext* ctx = nullptr);
}  // namespace acier::compute

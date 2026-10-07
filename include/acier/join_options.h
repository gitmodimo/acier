// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include <arrow/acero/options.h>
#include <arrow/result.h>
#include <arrow/type.h>

#include <acier/visibility.h>

namespace acier {

/// Options for acier_asofjoin, a left join on ordered streams.
///
/// Each input must have a meaningful ordering and sequenced batches. Non-null
/// on-key values must increase or remain equal. An explicit ordering must lead
/// with the ascending on-key and obey its null placement. Implicit ordering
/// permits null on-key values anywhere. Null left times never match, and null
/// right times are skipped. The output preserves the left input's ordering,
/// batches, indices and values, and adds nullable non-key columns from each right
/// input. A right row can match more than one left row. Each input batch may
/// contain at most INT32_MAX rows. There is no smaller output batch limit.
/// A downstream pause allows the active left batch to finish; once every left
/// batch has arrived, remaining output is delivered without waiting for resume.
class ACIER_EXPORT AsofJoinNodeOptions : public arrow::acero::ExecNodeOptions {
 public:
  struct Keys {
    /// A top-level integer, date, time or timestamp field. Its type and units
    /// must agree across inputs; tolerance offsets use these same units.
    arrow::FieldRef on_key;
    /// Top-level fields matched by exact equality, with null equal to null.
    /// Supported types are boolean, integer, date, time, timestamp, string,
    /// binary, fixed-size binary and decimal. Dictionary keys are unsupported.
    /// Corresponding fields must have the same types across inputs.
    std::vector<arrow::FieldRef> by_key;
  };

  /// Inclusive bounds on right.on - left.on. lower must not exceed upper.
  struct ToleranceRange {
    int64_t lower;
    int64_t upper;
  };

  /// A negative tolerance selects a past match, a positive tolerance a future
  /// match, and zero an exact-time match. Both endpoints are included.
  AsofJoinNodeOptions(std::vector<Keys> input_keys, int64_t tolerance)
      : AsofJoinNodeOptions(std::move(input_keys),
                            ToleranceRange{tolerance < 0 ? tolerance : 0,
                                           tolerance > 0 ? tolerance : 0}) {}

  /// Select the nearest eligible time. Equidistant times on opposite sides
  /// prefer the earlier time by default. For an interval ending at or before
  /// zero, duplicate eligible times select the last right row. For an interval
  /// extending after zero, an exact/future duplicate selects the first right
  /// row and a strictly past duplicate selects the last right row.
  AsofJoinNodeOptions(std::vector<Keys> input_keys, ToleranceRange tolerance,
                      bool prefer_earlier_on_tie = true)
      : input_keys(std::move(input_keys)),
        tolerance(tolerance),
        prefer_earlier_on_tie(prefer_earlier_on_tie) {}

  /// One key specification per input, with the left input first. At least two
  /// inputs are required, each with the same number of by-key fields.
  std::vector<Keys> input_keys;
  ToleranceRange tolerance;
  bool prefer_earlier_on_tie;
};

/// Options for acier_sorted_merge, which merges inputs with identical schemas.
/// Exactly one ascending top-level integer, date, time or timestamp sort key is
/// supported. Input batches must be sequenced, and each input must obey the
/// requested value and null ordering. Invalid ordering returns an error.
/// Equal-time rows from different inputs have no guaranteed relative order.
/// Output schema and field nullability match the first input; scalar payloads
/// become arrays. Empty input batches produce no rows. Output batches have at
/// most arrow::acero::ExecPlan::kMaxBatchSize rows and consecutive indices.
/// A pause allows in-flight output; after all inputs have arrived, remaining
/// output is delivered without waiting for resume. Set null placement explicitly
/// when the same application must retain that policy across Arrow versions.
using SortedMergeNodeOptions = arrow::acero::OrderByNodeOptions;

namespace asofjoin {
using AsofJoinKeys = AsofJoinNodeOptions::Keys;

/// Return the acier_asofjoin output schema for the given input schemas and keys.
/// Left fields retain their nullability; non-key right fields become nullable.
ACIER_EXPORT arrow::Result<std::shared_ptr<arrow::Schema>> MakeOutputSchema(
    const std::vector<std::shared_ptr<arrow::Schema>>& input_schema,
    const std::vector<AsofJoinKeys>& input_keys);
}  // namespace asofjoin

}  // namespace acier

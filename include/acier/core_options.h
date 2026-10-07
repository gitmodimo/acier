// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <acier/type_fwd.h>
#include <arrow/acero/options.h>

namespace acier {
/// Options for acier_filter. Rows must satisfy the expression and contain valid
/// values in every named field. Those fields are non-nullable in the output.
/// Names must identify unique top-level fields; missing/ambiguous names fail.
class FilterNodeOptions : public arrow::acero::ExecNodeOptions {
 public:
  explicit FilterNodeOptions(arrow::compute::Expression expression,
                             std::vector<std::string> not_null = {})
      : filter_expression(std::move(expression)), filter_not_null(std::move(not_null)) {}
  arrow::compute::Expression filter_expression;
  std::vector<std::string> filter_not_null;
};

/// Options for acier_record_batch_reader_source. A null executor uses the
/// plan's I/O executor. With implicit_ordering=true output indices follow
/// reader order; otherwise the source reports unordered output. The default is
/// false.
class RecordBatchReaderSourceNodeOptions : public arrow::acero::ExecNodeOptions {
 public:
  explicit RecordBatchReaderSourceNodeOptions(
      std::shared_ptr<arrow::RecordBatchReader> reader,
      arrow::internal::Executor* io_executor = nullptr, bool implicit_ordering = false)
      : reader(std::move(reader)),
        io_executor(io_executor),
        implicit_ordering(implicit_ordering) {}
  std::shared_ptr<arrow::RecordBatchReader> reader;
  arrow::internal::Executor* io_executor;
  bool implicit_ordering;
};
/// A row to append for each input row in acier_pivot_longer. Each feature
/// is a non-null shared pointer to an Arrow Scalar, which may hold a typed
/// null. A missing measurement reference emits a null of that measurement's
/// type.
struct PivotLongerRowTemplate {
  PivotLongerRowTemplate(std::vector<std::shared_ptr<arrow::Scalar>> feature_values,
                         std::vector<std::optional<arrow::FieldRef>> measurement_values)
      : feature_values(std::move(feature_values)),
        measurement_values(std::move(measurement_values)) {}
  std::vector<std::shared_ptr<arrow::Scalar>> feature_values;
  std::vector<std::optional<arrow::FieldRef>> measurement_values;
};

/// Options for acier_pivot_longer. At least one template, feature and
/// measurement name are required. Every template must supply one value per
/// named column. Feature types and referenced measurement types must agree
/// across templates; each measurement must be supplied by at least one
/// template. Only top-level measurement references are supported. Output
/// retains input columns and appends feature columns followed by measurements;
/// row count is multiplied by the number of templates. Output is unordered.
class PivotLongerNodeOptions : public arrow::acero::ExecNodeOptions {
 public:
  static constexpr std::string_view kName = "acier_pivot_longer";
  std::vector<PivotLongerRowTemplate> row_templates;
  std::vector<std::string> feature_field_names;
  std::vector<std::string> measurement_field_names;
};
}  // namespace acier

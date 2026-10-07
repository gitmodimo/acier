// SPDX-License-Identifier: Apache-2.0

#include <acier/api.h>
#include <acier/dataset/api.h>
#include <acier/dataset/plan.h>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/compute/initialize.h>

#include <iostream>
#include <memory>
#include <type_traits>

static_assert(std::is_same_v<acier::ExecPlan, arrow::acero::ExecPlan>);
static_assert(std::is_same_v<acier::Declaration, arrow::acero::Declaration>);
static_assert(std::is_same_v<acier::ExecNodeOptions, arrow::acero::ExecNodeOptions>);
static_assert(std::is_same_v<acier::dataset::Dataset, arrow::dataset::Dataset>);
static_assert(
    std::is_same_v<acier::dataset::ScanNodeOptions, arrow::dataset::ScanNodeOptions>);

arrow::Status Run() {
  ARROW_RETURN_NOT_OK(acier::Initialize());

  arrow::Int64Builder builder;
  ARROW_RETURN_NOT_OK(builder.AppendValues({1, 2, 3, 4}));
  ARROW_ASSIGN_OR_RAISE(auto values, builder.Finish());
  auto schema = arrow::schema({arrow::field("value", arrow::int64())});
  auto table = arrow::Table::Make(schema, {values});
  auto dataset = std::make_shared<acier::dataset::InMemoryDataset>(table);

  for (bool use_threads : {false, true}) {
    auto scan_options = std::make_shared<acier::dataset::ScanOptions>();
    scan_options->dataset_schema = schema;
    auto declaration = acier::Declaration::Sequence({
        {"scan", acier::dataset::ScanNodeOptions{dataset, scan_options}},
        {"acier_filter", acier::FilterNodeOptions{arrow::compute::call(
                             "greater", {arrow::compute::field_ref("value"),
                                         arrow::compute::literal(int64_t{2})})}},
        {"aggregate", acier::AggregateNodeOptions{{arrow::compute::Aggregate{
                          "sum", nullptr, "value", "total"}}}},
    });
    ARROW_ASSIGN_OR_RAISE(auto result,
                          acier::DeclarationToTable(std::move(declaration), use_threads));
    ARROW_RETURN_NOT_OK(result->ValidateFull());
    if (result->num_rows() != 1 || result->num_columns() != 1) {
      return arrow::Status::Invalid("Unexpected aggregate shape: ", result->ToString());
    }
    ARROW_ASSIGN_OR_RAISE(auto total, result->column(0)->GetScalar(0));
    if (!total->Equals(arrow::Int64Scalar{7})) {
      return arrow::Status::Invalid("Expected sum 7, got ", total->ToString());
    }
  }
  return arrow::Status::OK();
}

int main() {
  auto status = Run();
  if (!status.ok()) {
    std::cerr << status << '\n';
    return 1;
  }
  return 0;
}

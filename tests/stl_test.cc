// SPDX-License-Identifier: Apache-2.0
#include <acier/compute/api.h>
#include <acier/stl.h>
#include <array>
#include <iostream>
#include <optional>
#include <tuple>
#include <vector>

struct Centroid {
  double mean, weight;
};
namespace acier {
template <>
struct CTypeTraits<Centroid> : arrow::TypeTraits<arrow::StructType> {
  using ArrowType = arrow::StructType;
  static std::shared_ptr<arrow::DataType> type_singleton() {
    return arrow::struct_({arrow::field("mean", arrow::float64(), false),
                           arrow::field("weight", arrow::float64(), false)});
  }
};
namespace stl {
template <>
struct ConversionTraits<Centroid> : acier::CTypeTraits<Centroid> {
  static arrow::Status AppendRow(arrow::StructBuilder& builder, const Centroid& value) {
    ARROW_RETURN_NOT_OK(builder.Append());
    ARROW_RETURN_NOT_OK(
        static_cast<arrow::DoubleBuilder*>(builder.field_builder(0))->Append(value.mean));
    return static_cast<arrow::DoubleBuilder*>(builder.field_builder(1))
        ->Append(value.weight);
  }
  static Centroid GetEntry(const arrow::StructArray& array, size_t i) {
    return {static_cast<const arrow::DoubleArray&>(*array.field(0)).Value(i),
            static_cast<const arrow::DoubleArray&>(*array.field(1)).Value(i)};
  }
};
}  // namespace stl
}  // namespace acier
arrow::Status Run() {
  using Row = std::tuple<std::optional<double>, std::vector<std::optional<double>>,
                         std::array<std::vector<double>, 2>, std::vector<Centroid>>;
  std::vector<Row> rows{
      {1., {1., std::nullopt, 3.}, {{{1., 2.}, {3.}}}, {{2., 3.}}},
      {std::nullopt, {}, {{{}, {}}}, {}},
      {4., {std::nullopt, 7.}, {{{5.}, {6., 7.}}}, {{1., 2.}, {4., 6.}}}};
  std::shared_ptr<arrow::Table> table;
  ARROW_RETURN_NOT_OK(acier::stl::TableFromTupleRange(
      arrow::default_memory_pool(), rows,
      {"value", "optional_list", "nested", "centroids"}, &table));
  ARROW_RETURN_NOT_OK(table->ValidateFull());
  const auto optional_list =
      std::static_pointer_cast<arrow::ListType>(table->schema()->field(1)->type());
  const auto nested = std::static_pointer_cast<arrow::FixedSizeListType>(
      table->schema()->field(2)->type());
  const auto centroids =
      std::static_pointer_cast<arrow::ListType>(table->schema()->field(3)->type());
  if (!table->schema()->field(0)->nullable() ||
      !optional_list->value_field()->nullable() || nested->value_field()->nullable() ||
      centroids->value_field()->nullable()) {
    return arrow::Status::Invalid("Child/outer nullability changed");
  }
  if (std::static_pointer_cast<arrow::ListType>(nested->value_type())
          ->value_field()
          ->nullable()) {
    return arrow::Status::Invalid("Nested primitive nullability changed");
  }
  auto slice = table->Slice(1, 2);
  std::vector<Row> restored(2);
  ARROW_RETURN_NOT_OK(acier::stl::TupleRangeFromTable(
      *slice, arrow::compute::CastOptions::Safe(), nullptr, &restored));
  for (size_t i = 0; i < restored.size(); ++i) {
    const auto& expected = rows[i + 1];
    const auto& actual = restored[i];
    if (std::get<0>(actual) != std::get<0>(expected) ||
        std::get<1>(actual) != std::get<1>(expected) ||
        std::get<2>(actual) != std::get<2>(expected) ||
        std::get<3>(actual).size() != std::get<3>(expected).size()) {
      return arrow::Status::Invalid("Nested/optional sliced round trip differs");
    }
    for (size_t j = 0; j < std::get<3>(actual).size(); ++j) {
      if (std::get<3>(actual)[j].mean != std::get<3>(expected)[j].mean ||
          std::get<3>(actual)[j].weight != std::get<3>(expected)[j].weight) {
        return arrow::Status::Invalid("Custom struct round trip differs");
      }
    }
  }
  using DigestRow =
      std::tuple<std::vector<Centroid>, std::optional<double>, std::optional<double>>;
  auto digest_schema = acier::stl::SchemaFromTuple<DigestRow>::MakeSchema(
      std::vector<std::string>{"centroids", "min", "max"});
  if (!arrow::struct_(digest_schema->fields())
           ->Equals(acier::compute::TDigestCentroidType())) {
    return arrow::Status::Invalid("STL centroid schema differs from Compute contract");
  }
  if (acier::stl::TableFromTupleRange(arrow::default_memory_pool(), rows, {"missing"},
                                      &table)
          .ok()) {
    return arrow::Status::Invalid("Invalid name count accepted");
  }
  return arrow::Status::OK();
}
int main() {
  const auto status = Run();
  if (!status.ok()) {
    std::cerr << status << '\n';
    return 1;
  }
  return 0;
}

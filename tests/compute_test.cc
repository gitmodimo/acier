// SPDX-License-Identifier: Apache-2.0
#include <acier/compute/api.h>
#include <arrow/acero/exec_plan.h>
#include <arrow/acero/options.h>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/compute/function.h>
#include <arrow/compute/registry.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>

#include <cmath>
#include <iostream>
#include <limits>
#include <optional>
#include <vector>

namespace ac = acier::compute;
namespace cp = arrow::compute;
using arrow::Datum;
using arrow::Status;

Status Check(bool ok, const char* message) {
  return ok ? Status::OK() : Status::Invalid(message);
}
Status Equal(const Datum& actual, const Datum& expected, const char* label) {
  if (!actual.Equals(expected)) {
    return Status::Invalid(label, ": actual ", actual.ToString(), " expected ",
                           expected.ToString());
  }
  return Status::OK();
}
arrow::Result<std::shared_ptr<arrow::Array>> Values(
    std::vector<std::optional<double>> values) {
  arrow::DoubleBuilder builder;
  for (auto value : values) {
    if (value)
      ARROW_RETURN_NOT_OK(builder.Append(*value));
    else
      ARROW_RETURN_NOT_OK(builder.AppendNull());
  }
  return builder.Finish();
}
arrow::Result<Datum> Digest(std::vector<std::pair<double, double>> values,
                            std::optional<double> min, std::optional<double> max) {
  arrow::DoubleBuilder means, weights;
  for (auto [mean, weight] : values) {
    ARROW_RETURN_NOT_OK(means.Append(mean));
    ARROW_RETURN_NOT_OK(weights.Append(weight));
  }
  ARROW_ASSIGN_OR_RAISE(auto mean_array, means.Finish());
  ARROW_ASSIGN_OR_RAISE(auto weight_array, weights.Finish());
  ARROW_ASSIGN_OR_RAISE(
      auto centroids,
      arrow::StructArray::Make({mean_array, weight_array},
                               {arrow::field("mean", arrow::float64(), false),
                                arrow::field("weight", arrow::float64(), false)}));
  auto type = ac::TDigestCentroidType();
  auto list_type = std::static_pointer_cast<arrow::StructType>(type)->field(0)->type();
  auto low = min ? std::make_shared<arrow::DoubleScalar>(*min)
                 : std::make_shared<arrow::DoubleScalar>();
  auto high = max ? std::make_shared<arrow::DoubleScalar>(*max)
                  : std::make_shared<arrow::DoubleScalar>();
  return Datum(std::make_shared<arrow::StructScalar>(
      arrow::ScalarVector{std::make_shared<arrow::ListScalar>(centroids, list_type), low,
                          high},
      type));
}
Status OptionsAndRegistry() {
  ARROW_RETURN_NOT_OK(ac::Initialize());
  ARROW_RETURN_NOT_OK(ac::Initialize());
  auto* global = cp::GetFunctionRegistry();
  ARROW_ASSIGN_OR_RAISE(auto stock, global->GetFunction("tdigest"));
  ARROW_ASSIGN_OR_RAISE(auto owned, global->GetFunction("acier_tdigest"));
  ARROW_RETURN_NOT_OK(Check(stock != owned, "stock function overwritten"));
  for (const auto* name :
       {"acier_tdigest", "acier_tdigest_map", "acier_tdigest_reduce",
        "acier_tdigest_quantile", "acier_tdigest_quantile_element_wise",
        "acier_approximate_median"}) {
    ARROW_ASSIGN_OR_RAISE(auto function, global->GetFunction(name));
    ARROW_RETURN_NOT_OK(function->Validate());
  }
  ac::TDigestOptions numeric({0.1, 0.5, 0.9}, 128, 17, false, 3, ac::TDigestOptions::K0);
  ac::TDigestMapOptions map(128, 17, false, ac::TDigestOptions::K0);
  ac::TDigestReduceOptions reduce(128, ac::TDigestOptions::K0);
  ac::TDigestQuantileOptions quantile({0.1, 0.5}, 128, 3, ac::TDigestOptions::K0);
  for (const cp::FunctionOptions* options :
       std::vector<const cp::FunctionOptions*>{&numeric, &map, &reduce, &quantile}) {
    ARROW_RETURN_NOT_OK(Check(options->Equals(*options->Copy()), "option copy differs"));
    ARROW_ASSIGN_OR_RAISE(auto serialized, options->Serialize());
    ARROW_ASSIGN_OR_RAISE(auto restored, cp::FunctionOptions::Deserialize(
                                             options->type_name(), *serialized));
    ARROW_RETURN_NOT_OK(
        Check(restored->Equals(*options), "option serialization differs"));
    auto broken = arrow::Buffer::FromString("invalid");
    ARROW_RETURN_NOT_OK(Check(!options->options_type()->Deserialize(*broken).ok(),
                              "invalid options accepted"));
  }
  auto isolated = cp::FunctionRegistry::Make();
  ARROW_RETURN_NOT_OK(ac::Initialize(isolated.get()));
  ARROW_RETURN_NOT_OK(Check(!ac::Initialize(isolated.get()).ok(),
                            "custom duplicate registration accepted"));
  auto parent = cp::FunctionRegistry::Make();
  ARROW_RETURN_NOT_OK(ac::Initialize(parent.get()));
  auto child = cp::FunctionRegistry::Make(parent.get());
  ARROW_RETURN_NOT_OK(
      Check(!ac::Initialize(child.get()).ok(), "parent options collision accepted"));
  cp::ExecContext ctx(arrow::default_memory_pool(), nullptr, isolated.get());
  ARROW_ASSIGN_OR_RAISE(auto data, Values({1, 2, 3}));
  ARROW_ASSIGN_OR_RAISE(auto result, ac::TDigestMap(data, map, &ctx));
  ARROW_RETURN_NOT_OK(result.scalar()->ValidateFull());
  return Status::OK();
}
Status ShapesAndBoundaries() {
  ARROW_ASSIGN_OR_RAISE(auto input, Values({1, 2, 3, 4, 5, 6}));
  ARROW_ASSIGN_OR_RAISE(
      auto mapped,
      ac::TDigestMap(input, ac::TDigestMapOptions(5, 500, true, ac::TDigestOptions::K0)));
  ARROW_ASSIGN_OR_RAISE(auto expected, Digest({{1.5, 2}, {3.5, 2}, {5.5, 2}}, 1, 6));
  ARROW_RETURN_NOT_OK(Equal(mapped, expected, "K0 centroids"));
  ARROW_ASSIGN_OR_RAISE(auto array, arrow::MakeArrayFromScalar(*mapped.scalar(), 3));
  ARROW_ASSIGN_OR_RAISE(
      auto reduced,
      ac::TDigestReduce(array, ac::TDigestReduceOptions(5, ac::TDigestOptions::K0)));
  ARROW_ASSIGN_OR_RAISE(auto triple, Digest({{1.5, 6}, {3.5, 6}, {5.5, 6}}, 1, 6));
  ARROW_RETURN_NOT_OK(Equal(reduced, triple, "weighted reduction"));
  ac::TDigestQuantileOptions q({0, 0.5, 1}, 128, 6, ac::TDigestOptions::K0);
  ARROW_ASSIGN_OR_RAISE(auto quantile, ac::TDigestQuantile(mapped, q));
  ARROW_ASSIGN_OR_RAISE(auto expected_q, Values({1, 3.5, 6}));
  ARROW_RETURN_NOT_OK(Equal(quantile, expected_q, "aggregate quantile shape"));
  ARROW_ASSIGN_OR_RAISE(auto elementwise, ac::TDigestQuantileElementWise(array, q));
  ARROW_RETURN_NOT_OK(elementwise.make_array()->ValidateFull());
  ARROW_RETURN_NOT_OK(
      Check(elementwise.type()->Equals(arrow::fixed_size_list(arrow::float64(), 3)),
            "elementwise type"));
  for (int i = 0; i < 3; ++i) {
    ARROW_ASSIGN_OR_RAISE(auto scalar, elementwise.make_array()->GetScalar(i));
    ARROW_RETURN_NOT_OK(
        Equal(std::static_pointer_cast<arrow::FixedSizeListScalar>(scalar)->value,
              expected_q, "elementwise row"));
  }
  ARROW_ASSIGN_OR_RAISE(auto element_scalar, ac::TDigestQuantileElementWise(mapped, q));
  ARROW_RETURN_NOT_OK(Check(element_scalar.is_scalar(), "elementwise scalar shape"));
  ARROW_ASSIGN_OR_RAISE(auto empty_values, Values({}));
  ARROW_ASSIGN_OR_RAISE(auto empty, ac::TDigestMap(empty_values));
  ARROW_ASSIGN_OR_RAISE(auto empty_expected, Digest({}, {}, {}));
  ARROW_RETURN_NOT_OK(Equal(empty, empty_expected, "empty digest"));
  ARROW_ASSIGN_OR_RAISE(auto empty_q, ac::TDigestQuantileElementWise(empty, q));
  const auto& empty_list = empty_q.scalar_as<arrow::FixedSizeListScalar>();
  ARROW_RETURN_NOT_OK(Check(empty_list.is_valid && empty_list.value->null_count() == 3,
                            "empty quantile list"));
  ARROW_ASSIGN_OR_RAISE(auto null_q,
                        ac::TDigestQuantileElementWise(
                            arrow::MakeNullScalar(ac::TDigestCentroidType()), q));
  ARROW_RETURN_NOT_OK(
      Check(!null_q.scalar()->is_valid, "null digest must yield null list"));
  ARROW_ASSIGN_OR_RAISE(
      auto nan_values,
      Values({1, std::numeric_limits<double>::quiet_NaN(), std::nullopt}));
  ARROW_ASSIGN_OR_RAISE(
      auto limited, ac::TDigest(nan_values, ac::TDigestOptions(0.5, 100, 500, true, 2)));
  ARROW_RETURN_NOT_OK(
      Check(limited.make_array()->null_count() == 1, "NaN counts toward minimum"));
  ARROW_ASSIGN_OR_RAISE(
      auto median,
      ac::ApproximateMedian(nan_values, cp::ScalarAggregateOptions(true, 2)));
  ARROW_RETURN_NOT_OK(Check(!median.scalar()->is_valid, "median NaN threshold"));
  ARROW_ASSIGN_OR_RAISE(
      auto keep_nulls,
      ac::TDigestMap(nan_values, ac::TDigestMapOptions(100, 500, false)));
  ARROW_RETURN_NOT_OK(Check(!keep_nulls.scalar()->is_valid, "map null propagation"));
  ARROW_ASSIGN_OR_RAISE(auto null_array,
                        arrow::MakeArrayFromScalar(*keep_nulls.scalar(), 2));
  ARROW_ASSIGN_OR_RAISE(auto null_reduce, ac::TDigestReduce(null_array));
  ARROW_RETURN_NOT_OK(Check(!null_reduce.scalar()->is_valid, "reduce null propagation"));
  ARROW_ASSIGN_OR_RAISE(auto bad_weight, Digest({{1, -1}}, 1, 1));
  ARROW_RETURN_NOT_OK(
      Check(!ac::TDigestReduce(bad_weight).ok(), "negative centroid weight accepted"));
  ARROW_RETURN_NOT_OK(Check(!ac::TDigest(input, ac::TDigestOptions(-0.1)).ok(),
                            "invalid probability accepted"));
  ARROW_RETURN_NOT_OK(Check(!ac::TDigestMap(input, ac::TDigestMapOptions(0)).ok(),
                            "zero delta accepted"));
  ARROW_RETURN_NOT_OK(Check(!cp::CallFunction("acier_tdigest_map", {input}, &q).ok(),
                            "wrong option type accepted"));
  ARROW_ASSIGN_OR_RAISE(auto no_probabilities,
                        ac::TDigestQuantileElementWise(
                            array, ac::TDigestQuantileOptions(std::vector<double>{})));
  ARROW_RETURN_NOT_OK(no_probabilities.make_array()->ValidateFull());
  // IPC round trip fixes both the shape and all nested field nullability flags.
  auto batch = arrow::RecordBatch::Make(
      arrow::schema({arrow::field("digest", ac::TDigestCentroidType(), false)}), 3,
      {array});
  ARROW_ASSIGN_OR_RAISE(auto sink, arrow::io::BufferOutputStream::Create());
  ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeStreamWriter(sink, batch->schema()));
  ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
  ARROW_RETURN_NOT_OK(writer->Close());
  ARROW_ASSIGN_OR_RAISE(auto buffer, sink->Finish());
  ARROW_ASSIGN_OR_RAISE(auto reader,
                        arrow::ipc::RecordBatchStreamReader::Open(
                            std::make_shared<arrow::io::BufferReader>(buffer)));
  ARROW_ASSIGN_OR_RAISE(auto restored, reader->Next());
  ARROW_RETURN_NOT_OK(Check(restored->Equals(*batch), "digest IPC round trip"));
  return Status::OK();
}
Status NumericInputs() {
  ARROW_ASSIGN_OR_RAISE(auto input, Values({1, 2, 3, 4}));
  ARROW_ASSIGN_OR_RAISE(auto expected, Values({1, 2.5, 4}));
  auto types = arrow::NumericTypes();
  types.push_back(arrow::decimal128(10, 2));
  types.push_back(arrow::decimal256(10, 2));
  for (const auto& type : types) {
    ARROW_ASSIGN_OR_RAISE(auto typed, cp::Cast(input, type));
    ARROW_ASSIGN_OR_RAISE(auto digest, ac::TDigestMap(typed));
    ARROW_ASSIGN_OR_RAISE(
        auto quantiles, ac::TDigestQuantile(digest, ac::TDigestQuantileOptions(
                                                        std::vector<double>{0, 0.5, 1})));
    ARROW_RETURN_NOT_OK(Equal(quantiles, expected, "numeric/decimal quantiles"));
  }
  // Acero can represent many logical input rows with one scalar ExecSpan value.
  cp::ExecBatch batch({Datum(3.0)}, 8);
  ac::TDigestOptions options(std::vector<double>{0, 0.5, 1}, 128, 17, true, 8,
                             ac::TDigestOptions::K0);
  ARROW_ASSIGN_OR_RAISE(auto function,
                        cp::GetFunctionRegistry()->GetFunction("acier_tdigest"));
  std::vector<arrow::TypeHolder> input_types{arrow::float64()};
  ARROW_ASSIGN_OR_RAISE(auto dispatched, function->DispatchExact(input_types));
  const auto* kernel = static_cast<const cp::ScalarAggregateKernel*>(dispatched);
  cp::ExecContext exec_context;
  cp::KernelContext kernel_context(&exec_context, kernel);
  ARROW_ASSIGN_OR_RAISE(auto state,
                        kernel->init(&kernel_context, {kernel, input_types, &options}));
  kernel_context.SetState(state.get());
  ARROW_RETURN_NOT_OK(kernel->consume(&kernel_context, cp::ExecSpan(batch)));
  Datum repeated;
  ARROW_RETURN_NOT_OK(kernel->finalize(&kernel_context, &repeated));
  ARROW_ASSIGN_OR_RAISE(auto expected_repeated, Values({3, 3, 3}));
  ARROW_RETURN_NOT_OK(Equal(repeated, expected_repeated, "scalar span total weight"));
  return Status::OK();
}
Status PlanIntegration() {
  ARROW_ASSIGN_OR_RAISE(auto input, Values({1, 2, 3, 4}));
  ARROW_ASSIGN_OR_RAISE(auto digest, ac::TDigestMap(input));
  ARROW_ASSIGN_OR_RAISE(auto array, arrow::MakeArrayFromScalar(*digest.scalar(), 5));
  auto table = arrow::Table::Make(
      arrow::schema({arrow::field("digest", ac::TDigestCentroidType())}), {array});
  ARROW_ASSIGN_OR_RAISE(auto expected, ac::TDigestReduce(array));
  for (bool threaded : {false, true}) {
    auto declaration = arrow::acero::Declaration::Sequence(
        {{"table_source", arrow::acero::TableSourceNodeOptions(table, 2)},
         {"aggregate",
          arrow::acero::AggregateNodeOptions({cp::Aggregate(
              "acier_tdigest_reduce", std::make_shared<ac::TDigestReduceOptions>(),
              "digest", "merged")})}});
    ARROW_ASSIGN_OR_RAISE(auto output,
                          arrow::acero::DeclarationToTable(declaration, threaded));
    ARROW_ASSIGN_OR_RAISE(auto scalar, output->column(0)->GetScalar(0));
    ARROW_RETURN_NOT_OK(Equal(scalar, expected, "Acero aggregate"));
  }
  return Status::OK();
}
Status Run() {
  ARROW_RETURN_NOT_OK(OptionsAndRegistry());
  ARROW_RETURN_NOT_OK(ShapesAndBoundaries());
  ARROW_RETURN_NOT_OK(NumericInputs());
  ARROW_RETURN_NOT_OK(PlanIntegration());
  return Status::OK();
}
int main() {
  const auto status = Run();
  if (!status.ok()) {
    std::cerr << status << '\n';
    return 1;
  }
  return 0;
}

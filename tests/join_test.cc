// SPDX-License-Identifier: Apache-2.0

#include <acier/join_options.h>

#include "../src/join/registry.h"

#include <arrow/acero/exec_plan.h>
#include <arrow/acero/options.h>
#include <arrow/acero/query_context.h>
#include <arrow/api.h>
#include <arrow/compute/initialize.h>
#include <arrow/util/async_generator.h>
#include <arrow/util/thread_pool.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <functional>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace {
namespace ac = arrow::acero;
namespace cp = arrow::compute;
using arrow::Array;
using arrow::Datum;
using arrow::ExecBatch;
using arrow::Result;
using arrow::Status;
using OptInt = std::optional<int64_t>;

#define REQUIRE(condition, message)                    \
  do {                                                 \
    if (!(condition)) return Status::Invalid(message); \
  } while (false)

Result<std::shared_ptr<Array>> Ints(const std::vector<OptInt>& values) {
  arrow::Int64Builder builder;
  for (auto value : values) {
    if (value) {
      ARROW_RETURN_NOT_OK(builder.Append(*value));
    } else {
      ARROW_RETURN_NOT_OK(builder.AppendNull());
    }
  }
  return builder.Finish();
}

Result<std::shared_ptr<Array>> Lists(const std::vector<std::vector<int64_t>>& rows) {
  auto values = std::make_shared<arrow::Int64Builder>();
  arrow::ListBuilder lists(arrow::default_memory_pool(), values);
  for (const auto& row : rows) {
    ARROW_RETURN_NOT_OK(lists.Append());
    ARROW_RETURN_NOT_OK(values->AppendValues(row));
  }
  return lists.Finish();
}

Result<ExecBatch> Batch(const std::vector<OptInt>& times) {
  ARROW_ASSIGN_OR_RAISE(auto values, Ints(times));
  return ExecBatch({values}, static_cast<int64_t>(times.size()));
}

Result<ExecBatch> PayloadBatch(const std::vector<OptInt>& times,
                               const std::vector<OptInt>& payload) {
  ARROW_ASSIGN_OR_RAISE(auto on, Ints(times));
  ARROW_ASSIGN_OR_RAISE(auto values, Ints(payload));
  return ExecBatch({on, values}, static_cast<int64_t>(times.size()));
}

Result<std::shared_ptr<arrow::Scalar>> Cell(const ExecBatch& batch, int column,
                                            int64_t row) {
  const auto& datum = batch.values[column];
  return datum.is_scalar() ? Result<std::shared_ptr<arrow::Scalar>>(datum.scalar())
                           : datum.make_array()->GetScalar(row);
}

Status CheckColumn(const ac::BatchesWithCommonSchema& result, int column,
                   const std::vector<OptInt>& expected) {
  size_t row_index = 0;
  for (const auto& batch : result.batches) {
    ARROW_ASSIGN_OR_RAISE(auto record_batch, batch.ToRecordBatch(result.schema));
    ARROW_RETURN_NOT_OK(record_batch->ValidateFull());
    for (int64_t row = 0; row < batch.length; ++row) {
      REQUIRE(row_index < expected.size(), "Unexpected extra result row");
      ARROW_ASSIGN_OR_RAISE(auto value, Cell(batch, column, row));
      if (expected[row_index]) {
        REQUIRE(value->Equals(arrow::Int64Scalar(*expected[row_index])),
                "Unexpected integer result: " + value->ToString());
      } else {
        REQUIRE(!value->is_valid, "Expected a null result");
      }
      ++row_index;
    }
  }
  REQUIRE(row_index == expected.size(), "Missing result rows");
  return Status::OK();
}

// Build the same explicit ordering on both supported Arrow APIs.
template <typename Ordering = cp::Ordering>
Ordering TimeOrder(cp::NullPlacement placement = cp::NullPlacement::AtStart) {
  auto keys = Ordering({cp::SortKey("on")}).sort_keys();
  if constexpr (std::is_same_v<decltype(std::declval<Ordering>().null_placement()),
                               cp::NullPlacement>) {
    return Ordering(std::move(keys), placement);
  } else {
    keys[0].null_placement = placement;
    return Ordering(std::move(keys));
  }
}

ac::Declaration Source(const std::shared_ptr<arrow::Schema>& schema,
                       std::vector<ExecBatch> batches) {
  return {"exec_batch_source",
          ac::ExecBatchSourceNodeOptions(schema, std::move(batches))};
}

acier::AsofJoinNodeOptions AsofOptions(int64_t tolerance = 0) {
  return {{{"on", {}}, {"on", {}}}, tolerance};
}

Result<ac::BatchesWithCommonSchema> Run(ac::Declaration declaration, bool threaded) {
  ac::QueryOptions options;
  options.use_threads = threaded;
  options.sequence_output = false;
  return ac::DeclarationToExecBatches(std::move(declaration), options);
}

// A bounded source for testing physical arrival order, input metadata and batches
// larger than the built-in source's slicing limit. Work is owned by the plan.
struct TestSourceOptions : ac::ExecNodeOptions {
  TestSourceOptions(std::shared_ptr<arrow::Schema> schema, std::vector<ExecBatch> batches,
                    bool reverse = false,
                    cp::Ordering ordering = cp::Ordering::Implicit(),
                    bool unsequenced = false, bool invalid_total = false)
      : schema(std::move(schema)),
        batches(std::move(batches)),
        reverse(reverse),
        ordering(std::move(ordering)),
        unsequenced(unsequenced),
        invalid_total(invalid_total) {}
  std::shared_ptr<arrow::Schema> schema;
  std::vector<ExecBatch> batches;
  bool reverse;
  cp::Ordering ordering;
  bool unsequenced;
  bool invalid_total;
};

class TestSource : public ac::ExecNode {
 public:
  TestSource(ac::ExecPlan* plan, TestSourceOptions options)
      : ExecNode(plan, {}, {}, options.schema), options_(std::move(options)) {}
  const char* kind_name() const override { return "AcierTestSource"; }
  const cp::Ordering& ordering() const override { return options_.ordering; }
  Status InputReceived(ac::ExecNode*, ExecBatch) override {
    return Status::Invalid("Test source has no input");
  }
  Status InputFinished(ac::ExecNode*, int) override {
    return Status::Invalid("Test source has no input");
  }
  Status StartProducing() override {
    plan()->query_context()->ScheduleTask(
        [this] {
          const int count = static_cast<int>(options_.batches.size());
          ARROW_RETURN_NOT_OK(
              output_->InputFinished(this, options_.invalid_total ? -1 : count));
          for (int n = 0; n < count && !stopped_.load(); ++n) {
            const int index = options_.reverse ? count - 1 - n : n;
            auto batch = options_.batches[index];
            batch.index = options_.unsequenced ? cp::kUnsequencedIndex : index;
            ARROW_RETURN_NOT_OK(output_->InputReceived(this, std::move(batch)));
          }
          return Status::OK();
        },
        "AcierTestSource::Produce");
    return Status::OK();
  }
  void PauseProducing(ac::ExecNode*, int32_t) override {}
  void ResumeProducing(ac::ExecNode*, int32_t) override {}
  Status StopProducingImpl() override {
    stopped_.store(true);
    return Status::OK();
  }

 private:
  TestSourceOptions options_;
  std::atomic<bool> stopped_{false};
};

Status RegisterTestSource() {
  return ac::default_exec_factory_registry()->AddFactory(
      "acier_test_source",
      [](ac::ExecPlan* plan, std::vector<ac::ExecNode*> inputs,
         const ac::ExecNodeOptions& options) -> Result<ac::ExecNode*> {
        if (!inputs.empty()) return Status::Invalid("Test source cannot have inputs");
        return plan->EmplaceNode<TestSource>(
            plan, dynamic_cast<const TestSourceOptions&>(options));
      });
}

ac::Declaration SpecialSource(const std::shared_ptr<arrow::Schema>& schema,
                              std::vector<ExecBatch> batches, bool reverse = false,
                              cp::Ordering ordering = cp::Ordering::Implicit(),
                              bool unsequenced = false, bool invalid_total = false) {
  return {"acier_test_source",
          TestSourceOptions(schema, std::move(batches), reverse, std::move(ordering),
                            unsequenced, invalid_total)};
}

Status TestAsofPayloadAndBatches(bool threaded) {
  auto left_schema = arrow::schema({arrow::field("on", arrow::int64(), false),
                                    arrow::field("key", arrow::int64(), false),
                                    arrow::field("label", arrow::utf8(), false)});
  auto right_schema =
      arrow::schema({arrow::field("on", arrow::int64(), false),
                     arrow::field("key", arrow::int64(), false),
                     arrow::field("value", arrow::int64(), false),
                     arrow::field("list", arrow::list(arrow::int64()), false)});
  std::vector<ExecBatch> left;
  for (const auto& times :
       {std::vector<OptInt>{-1, 0}, std::vector<OptInt>{}, std::vector<OptInt>{2, 4}}) {
    ARROW_ASSIGN_OR_RAISE(auto batch, Batch(times));
    batch.values.emplace_back(std::make_shared<arrow::Int64Scalar>(1));
    batch.values.emplace_back(std::make_shared<arrow::StringScalar>("left"));
    left.push_back(std::move(batch));
  }
  ARROW_ASSIGN_OR_RAISE(auto right, PayloadBatch({-2, 0, 3}, {10, 20, 30}));
  ARROW_ASSIGN_OR_RAISE(auto nested, Lists({{99}, {10, 11}, {20}, {30, 31}, {99}}));
  right.values.insert(right.values.begin() + 1,
                      Datum(std::make_shared<arrow::Int64Scalar>(1)));
  right.values.emplace_back(nested->Slice(1, 3));
  acier::AsofJoinNodeOptions options({{"on", {"key"}}, {"on", {"key"}}}, -2);
  ac::Declaration join{"acier_asofjoin",
                       {Source(left_schema, left), Source(right_schema, {right})},
                       options};
  ARROW_ASSIGN_OR_RAISE(auto result, Run(std::move(join), threaded));
  REQUIRE(result.batches.size() == 3, "AsofJoin must preserve empty left batches");
  const std::vector<int64_t> lengths{2, 0, 2};
  for (size_t i = 0; i < result.batches.size(); ++i) {
    REQUIRE(
        result.batches[i].length == lengths[i] && result.batches[i].index == int64_t(i),
        "AsofJoin changed left batch boundaries or indices");
    REQUIRE(result.batches[i].values[1].is_scalar() &&
                result.batches[i].values[2].is_scalar(),
            "AsofJoin must preserve left scalar values");
  }
  REQUIRE(!result.schema->field(0)->nullable() && result.schema->field(3)->nullable() &&
              result.schema->field(4)->nullable(),
          "Incorrect AsofJoin output nullability");
  ARROW_RETURN_NOT_OK(CheckColumn(result, 3, {10, 20, 20, 30}));
  ARROW_ASSIGN_OR_RAISE(auto expected_lists, Lists({{10, 11}, {20}, {20}, {30, 31}}));
  int64_t output_row = 0;
  for (const auto& batch : result.batches) {
    for (int64_t row = 0; row < batch.length; ++row) {
      ARROW_ASSIGN_OR_RAISE(auto actual, Cell(batch, 4, row));
      ARROW_ASSIGN_OR_RAISE(auto expected, expected_lists->GetScalar(output_row++));
      REQUIRE(actual->Equals(*expected), "Nested sliced RHS payload changed");
    }
  }
  ARROW_ASSIGN_OR_RAISE(
      auto schema,
      acier::asofjoin::MakeOutputSchema({left_schema, right_schema}, options.input_keys));
  REQUIRE(schema->Equals(*result.schema), "Schema helper disagrees with the node");
  return Status::OK();
}

Status TestAsofKeysAndNulls(bool threaded) {
  auto left_schema = arrow::schema({arrow::field("on", arrow::int64()),
                                    arrow::field("key", arrow::int64()),
                                    arrow::field("key2", arrow::utf8())});
  auto right_schema = arrow::schema(
      {arrow::field("on", arrow::int64()), arrow::field("key", arrow::int64()),
       arrow::field("key2", arrow::utf8()), arrow::field("value", arrow::int64())});
  ARROW_ASSIGN_OR_RAISE(auto left,
                        PayloadBatch({1, 1, 1, std::nullopt}, {std::nullopt, 1, 2, 1}));
  ARROW_ASSIGN_OR_RAISE(auto right,
                        PayloadBatch({1, 1, 1, std::nullopt}, {2, std::nullopt, 1, 1}));
  ARROW_ASSIGN_OR_RAISE(auto payload, Ints({200, 999, 100, 666}));
  left.values.emplace_back(std::make_shared<arrow::StringScalar>("key"));
  right.values.emplace_back(std::make_shared<arrow::StringScalar>("key"));
  right.values.emplace_back(payload);
  acier::AsofJoinNodeOptions options({{"on", {"key", "key2"}}, {"on", {"key", "key2"}}},
                                     0);
  ARROW_ASSIGN_OR_RAISE(auto result,
                        Run({"acier_asofjoin",
                             {Source(left_schema, {left}), Source(right_schema, {right})},
                             options},
                            threaded));
  return CheckColumn(result, 3, {999, 100, 200, std::nullopt});
}

Status TestAsofTolerance(bool threaded) {
  auto left_schema = arrow::schema({arrow::field("on", arrow::int64())});
  auto right_schema = arrow::schema(
      {arrow::field("on", arrow::int64()), arrow::field("value", arrow::int64())});
  ARROW_ASSIGN_OR_RAISE(auto left, Batch({10, 20, 30, 40}));
  ARROW_ASSIGN_OR_RAISE(auto right,
                        PayloadBatch({8, 12, 18, 22, 30, 41}, {8, 12, 18, 22, 30, 41}));
  for (bool prefer_earlier : {true, false}) {
    acier::AsofJoinNodeOptions options({{"on", {}}, {"on", {}}}, {-2, 2}, prefer_earlier);
    ARROW_ASSIGN_OR_RAISE(
        auto result, Run({"acier_asofjoin",
                          {Source(left_schema, {left}), Source(right_schema, {right})},
                          options},
                         threaded));
    ARROW_RETURN_NOT_OK(CheckColumn(result, 1,
                                    prefer_earlier
                                        ? std::vector<OptInt>{8, 18, 30, 41}
                                        : std::vector<OptInt>{12, 22, 30, 41}));
  }
  // Both tolerance windows include exactly their past endpoint and reject
  // the next older row. Distinct by-key groups prevent a nearer candidate.
  for (int64_t tolerance : {-10LL, -10000000000LL}) {
    auto key_schema = arrow::schema(
        {arrow::field("on", arrow::int64()), arrow::field("key", arrow::int64())});
    auto payload_schema = arrow::schema({arrow::field("on", arrow::int64()),
                                         arrow::field("key", arrow::int64()),
                                         arrow::field("value", arrow::int64())});
    ARROW_ASSIGN_OR_RAISE(auto lhs, PayloadBatch({0, 0}, {0, 1}));
    ARROW_ASSIGN_OR_RAISE(auto rhs, PayloadBatch({tolerance - 1, tolerance}, {1, 0}));
    ARROW_ASSIGN_OR_RAISE(auto payload, Ints({99, 42}));
    rhs.values.emplace_back(payload);
    acier::AsofJoinNodeOptions options({{"on", {"key"}}, {"on", {"key"}}}, tolerance);
    ARROW_ASSIGN_OR_RAISE(auto result,
                          Run({"acier_asofjoin",
                               {Source(key_schema, {lhs}), Source(payload_schema, {rhs})},
                               options},
                              threaded));
    ARROW_RETURN_NOT_OK(CheckColumn(result, 2, {42, std::nullopt}));
  }
  ARROW_ASSIGN_OR_RAISE(auto extreme_left, Batch({0}));
  ARROW_ASSIGN_OR_RAISE(auto extreme_right,
                        PayloadBatch({std::numeric_limits<int64_t>::min()}, {42}));
  ARROW_ASSIGN_OR_RAISE(
      auto extreme_result,
      Run({"acier_asofjoin",
           {Source(left_schema, {extreme_left}), Source(right_schema, {extreme_right})},
           AsofOptions(std::numeric_limits<int64_t>::min())},
          threaded));
  ARROW_RETURN_NOT_OK(CheckColumn(extreme_result, 1, {42}));
  // Repeated timestamps span RHS batches. Backward/exact keeps the last row;
  // forward mode keeps the first exact row, including scalar on-key inputs.
  ExecBatch scalar_left({std::make_shared<arrow::Int64Scalar>(1)}, 2);
  ARROW_ASSIGN_OR_RAISE(auto duplicate1, PayloadBatch({1}, {10}));
  ARROW_ASSIGN_OR_RAISE(auto duplicate2, PayloadBatch({1}, {20}));
  for (int64_t tolerance : {0, 1}) {
    ARROW_ASSIGN_OR_RAISE(auto duplicate_result,
                          Run({"acier_asofjoin",
                               {Source(left_schema, {scalar_left}),
                                Source(right_schema, {duplicate1, duplicate2})},
                               AsofOptions(tolerance)},
                              threaded));
    const int64_t expected = tolerance == 0 ? 20 : 10;
    ARROW_RETURN_NOT_OK(CheckColumn(duplicate_result, 1, {expected, expected}));
  }
  return Status::OK();
}

Status TestSortedNullsAndPayload(bool threaded) {
  auto schema = arrow::schema({arrow::field("on", arrow::int64()),
                               arrow::field("payload", arrow::utf8()),
                               arrow::field("nested", arrow::list(arrow::int64()))});
  for (auto placement : {cp::NullPlacement::AtStart, cp::NullPlacement::AtEnd}) {
    const bool first = placement == cp::NullPlacement::AtStart;
    ARROW_ASSIGN_OR_RAISE(auto left,
                          Batch(first ? std::vector<OptInt>{std::nullopt, -4, 0}
                                      : std::vector<OptInt>{-4, 0, std::nullopt}));
    ARROW_ASSIGN_OR_RAISE(auto right,
                          Batch(first ? std::vector<OptInt>{std::nullopt, -3, 1}
                                      : std::vector<OptInt>{-3, 1, std::nullopt}));
    ARROW_ASSIGN_OR_RAISE(auto nested, Lists({{99}, {1, 2}, {3}, {4, 5}, {99}}));
    left.values.emplace_back(std::make_shared<arrow::StringScalar>("left"));
    right.values.emplace_back(std::make_shared<arrow::StringScalar>("right"));
    left.values.emplace_back(nested->Slice(1, 3));
    right.values.emplace_back(nested->Slice(1, 3));
    ARROW_ASSIGN_OR_RAISE(auto result,
                          Run({"acier_sorted_merge",
                               {Source(schema, {left}), Source(schema, {right})},
                               ac::OrderByNodeOptions(TimeOrder(placement))},
                              threaded));
    ARROW_RETURN_NOT_OK(CheckColumn(
        result, 0,
        first ? std::vector<OptInt>{std::nullopt, std::nullopt, -4, -3, 0, 1}
              : std::vector<OptInt>{-4, -3, 0, 1, std::nullopt, std::nullopt}));
    REQUIRE(result.schema->Equals(*schema), "SortedMerge changed its schema");
    for (const auto& batch : result.batches) {
      REQUIRE(batch.values[1].is_array(), "SortedMerge payload was not materialized");
      for (int64_t row = 0; row < batch.length; ++row) {
        ARROW_ASSIGN_OR_RAISE(auto on, Cell(batch, 0, row));
        ARROW_ASSIGN_OR_RAISE(auto label, Cell(batch, 1, row));
        ARROW_ASSIGN_OR_RAISE(auto list, Cell(batch, 2, row));
        int64_t expected_row = 0;
        if (on->is_valid) {
          auto time = static_cast<const arrow::Int64Scalar&>(*on).value;
          expected_row = first ? (time < 0 ? 0 : 1) + 1 : (time < 0 ? 0 : 1);
          REQUIRE(label->Equals(
                      arrow::StringScalar(time == -4 || time == 0 ? "left" : "right")),
                  "SortedMerge detached scalar payload from its row");
        } else {
          expected_row = first ? 0 : 2;
        }
        ARROW_ASSIGN_OR_RAISE(auto expected, nested->GetScalar(expected_row + 1));
        REQUIRE(list->Equals(*expected), "SortedMerge changed nested sliced payload");
      }
    }
  }
  // Explicitly exercise main's still-supported ordering-wide legacy override.
  // On main SortKey defaults to nulls last; this override must win.
  auto one_column = arrow::schema({arrow::field("on", arrow::int64())});
  ARROW_ASSIGN_OR_RAISE(auto left, Batch({std::nullopt, 0}));
  ARROW_ASSIGN_OR_RAISE(auto right, Batch({std::nullopt, 1}));
#if defined(__GNUC__)
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
  cp::Ordering legacy({cp::SortKey("on")}, cp::NullPlacement::AtStart);
#if defined(__GNUC__)
#  pragma GCC diagnostic pop
#endif
  ARROW_ASSIGN_OR_RAISE(auto result,
                        Run({"acier_sorted_merge",
                             {Source(one_column, {left}), Source(one_column, {right})},
                             ac::OrderByNodeOptions(legacy)},
                            threaded));
  return CheckColumn(result, 0, {std::nullopt, std::nullopt, 0, 1});
}

Status TestSequencingAndBatchLimits(bool threaded) {
  auto schema = arrow::schema({arrow::field("on", arrow::int64())});
  auto payload_schema = arrow::schema(
      {arrow::field("on", arrow::int64()), arrow::field("value", arrow::int64())});
  std::vector<ExecBatch> even, odd, left, right;
  std::vector<OptInt> merged, joined;
  for (int i = 0; i < 64; ++i) {
    ARROW_ASSIGN_OR_RAISE(auto e, Batch({2 * i}));
    ARROW_ASSIGN_OR_RAISE(auto o, Batch({2 * i + 1}));
    ARROW_ASSIGN_OR_RAISE(auto l, Batch({i}));
    ARROW_ASSIGN_OR_RAISE(auto r, PayloadBatch({i}, {i * 10}));
    even.push_back(std::move(e));
    odd.push_back(std::move(o));
    left.push_back(std::move(l));
    right.push_back(std::move(r));
    merged.push_back(2 * i);
    merged.push_back(2 * i + 1);
    joined.push_back(i * 10);
  }
  ARROW_ASSIGN_OR_RAISE(
      auto merge_result,
      Run({"acier_sorted_merge",
           {SpecialSource(schema, even, true), SpecialSource(schema, odd, true)},
           ac::OrderByNodeOptions(TimeOrder())},
          threaded));
  ARROW_RETURN_NOT_OK(CheckColumn(merge_result, 0, merged));
  ARROW_ASSIGN_OR_RAISE(auto join_result,
                        Run({"acier_asofjoin",
                             {SpecialSource(schema, left, true),
                              SpecialSource(payload_schema, right, true)},
                             AsofOptions()},
                            threaded));
  ARROW_RETURN_NOT_OK(CheckColumn(join_result, 1, joined));
  for (size_t i = 0; i < join_result.batches.size(); ++i) {
    REQUIRE(join_result.batches[i].index == int64_t(i), "Jitter changed join indices");
  }
  constexpr int64_t rows = 2 * ac::ExecPlan::kMaxBatchSize + 3;
  ExecBatch large({std::make_shared<arrow::Int64Scalar>(1)}, rows);
  ARROW_ASSIGN_OR_RAISE(auto bounded, Run({"acier_sorted_merge",
                                           {SpecialSource(schema, {large})},
                                           ac::OrderByNodeOptions(TimeOrder())},
                                          threaded));
  REQUIRE(bounded.batches.size() == 3, "SortedMerge did not split an oversized input");
  for (size_t i = 0; i < bounded.batches.size(); ++i) {
    const auto& batch = bounded.batches[i];
    REQUIRE(batch.length <= ac::ExecPlan::kMaxBatchSize && batch.index == int64_t(i),
            "SortedMerge output exceeded the cap or lost contiguous indices");
  }
  REQUIRE(bounded.batches.back().length == 3, "Incorrect bounded output tail");
  ARROW_ASSIGN_OR_RAISE(auto rhs, PayloadBatch({1}, {5}));
  ARROW_ASSIGN_OR_RAISE(
      auto unbounded,
      Run({"acier_asofjoin",
           {SpecialSource(schema, {large}), Source(payload_schema, {rhs})},
           AsofOptions()},
          threaded));
  REQUIRE(unbounded.batches.size() == 1 && unbounded.batches[0].length == rows,
          "AsofJoin did not preserve the oversized LHS batch");
  return Status::OK();
}

Status TestOrderingAndErrors(bool threaded) {
  auto schema = arrow::schema({arrow::field("on", arrow::int64())});
  ARROW_ASSIGN_OR_RAISE(auto good, Batch({0, 1}));
  ARROW_ASSIGN_OR_RAISE(auto bad, Batch({1, 0}));
  for (const std::string factory : {"acier_asofjoin", "acier_sorted_merge"}) {
    auto make = [&](ac::Declaration left, ac::Declaration right) {
      if (factory == "acier_asofjoin") {
        return ac::Declaration(factory, {std::move(left), std::move(right)},
                               AsofOptions());
      }
      return ac::Declaration(factory, {std::move(left), std::move(right)},
                             ac::OrderByNodeOptions(TimeOrder()));
    };
    for (int failure = 0; failure < 3; ++failure) {
      auto left = SpecialSource(schema, {failure == 0 ? bad : good}, false,
                                cp::Ordering::Implicit(), failure == 1, failure == 2);
      auto result = Run(make(std::move(left), Source(schema, {good})), threaded);
      REQUIRE(!result.ok() && result.status().IsInvalid(),
              "Join did not reject malformed ordering/index/count");
    }
    ARROW_ASSIGN_OR_RAISE(auto empty,
                          Run(make(Source(schema, {}), Source(schema, {})), threaded));
    REQUIRE(empty.batches.empty(), "Empty inputs produced rows");
  }
  auto unordered = Run({"acier_asofjoin",
                        {SpecialSource(schema, {good}, false, cp::Ordering::Unordered()),
                         Source(schema, {good})},
                        AsofOptions()},
                       threaded);
  REQUIRE(!unordered.ok() && unordered.status().IsInvalid(),
          "AsofJoin accepted missing input ordering");
  auto invalid_range =
      Run({"acier_asofjoin",
           {Source(schema, {good}), Source(schema, {good})},
           acier::AsofJoinNodeOptions({{"on", {}}, {"on", {}}}, {1, -1})},
          threaded);
  REQUIRE(!invalid_range.ok() && invalid_range.status().IsInvalid(),
          "AsofJoin accepted an inverted tolerance interval");
  for (auto placement : {cp::NullPlacement::AtStart, cp::NullPlacement::AtEnd}) {
    ARROW_ASSIGN_OR_RAISE(auto invalid_nulls,
                          Batch(placement == cp::NullPlacement::AtStart
                                    ? std::vector<OptInt>{0, std::nullopt}
                                    : std::vector<OptInt>{std::nullopt, 0}));
    for (const std::string factory : {"acier_asofjoin", "acier_sorted_merge"}) {
      ac::Declaration declaration;
      if (factory == "acier_asofjoin") {
        declaration = {
            factory,
            {SpecialSource(schema, {invalid_nulls}, false, TimeOrder(placement)),
             Source(schema, {good})},
            AsofOptions()};
      } else {
        declaration = {factory,
                       {Source(schema, {invalid_nulls})},
                       ac::OrderByNodeOptions(TimeOrder(placement))};
      }
      auto result = Run(std::move(declaration), threaded);
      REQUIRE(!result.ok() && result.status().IsInvalid(),
              "Join accepted invalid declared null placement");
    }
  }
  // The LHS on-key's field reference remains meaningful after RHS columns are
  // appended; implicit ordering remains implicit.
  ARROW_ASSIGN_OR_RAISE(auto plan, ac::ExecPlan::Make());
  ARROW_ASSIGN_OR_RAISE(
      auto implicit_node,
      ac::Declaration("acier_asofjoin", {Source(schema, {good}), Source(schema, {good})},
                      AsofOptions())
          .AddToPlan(plan.get()));
  REQUIRE(implicit_node->ordering().is_implicit(), "AsofJoin invented explicit ordering");
  ARROW_ASSIGN_OR_RAISE(
      auto explicit_node,
      ac::Declaration(
          "acier_asofjoin",
          {SpecialSource(schema, {good}, false, TimeOrder()), Source(schema, {good})},
          AsofOptions())
          .AddToPlan(plan.get()));
  REQUIRE(!explicit_node->ordering().is_implicit() &&
              explicit_node->ordering().sort_keys().size() == 1,
          "AsofJoin lost explicit ordering");
  auto wide_schema = arrow::schema(
      {arrow::field("on", arrow::int64()), arrow::field("secondary", arrow::int64())});
  ARROW_ASSIGN_OR_RAISE(auto wide_batch, PayloadBatch({0, 1}, {2, 3}));
  cp::Ordering secondary_order({cp::SortKey("on"), cp::SortKey("secondary")});
  ARROW_ASSIGN_OR_RAISE(
      auto secondary_node,
      ac::Declaration("acier_asofjoin",
                      {SpecialSource(wide_schema, {wide_batch}, false, secondary_order),
                       Source(wide_schema, {wide_batch})},
                      AsofOptions())
          .AddToPlan(plan.get()));
  REQUIRE(secondary_node->ordering().sort_keys().size() == 2,
          "AsofJoin discarded secondary ordering keys");
  ARROW_ASSIGN_OR_RAISE(auto secondary_path,
                        secondary_node->ordering().sort_keys()[1].target.FindOne(
                            *secondary_node->output_schema()));
  REQUIRE(secondary_path.indices() == std::vector<int>{1},
          "Duplicate RHS names made output ordering ambiguous");
  return Status::OK();
}

class PausingConsumer : public ac::SinkNodeConsumer {
 public:
  Status Init(const std::shared_ptr<arrow::Schema>&, ac::BackpressureControl* control,
              ac::ExecPlan*) override {
    control_.store(control);
    return Status::OK();
  }
  Status Consume(ExecBatch batch) override {
    rows_.fetch_add(batch.length);
    if (batches_.fetch_add(1) == 0) {
      control_.load()->Pause();
      paused_.store(true);
    }
    return Status::OK();
  }
  arrow::Future<> Finish() override { return arrow::Future<>::MakeFinished(); }
  void Resume() {
    control_.load()->Resume();
    paused_.store(false);
  }
  bool paused() const { return paused_.load(); }
  int batches() const { return batches_.load(); }
  int64_t rows() const { return rows_.load(); }

 private:
  std::atomic<ac::BackpressureControl*> control_{nullptr};
  std::atomic<int> batches_{0};
  std::atomic<int64_t> rows_{0};
  std::atomic<bool> paused_{false};
};

Status Until(const std::function<bool()>& condition) {
  const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!condition()) {
    if (std::chrono::steady_clock::now() >= end)
      return Status::IOError("Join test timed out");
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return Status::OK();
}

// Release pending source futures even when an assertion exits the test early.
struct PlanGuard {
  std::shared_ptr<ac::ExecPlan> plan;
  std::function<void()> close;
  ~PlanGuard() {
    if (!plan->finished().is_finished()) {
      plan->StopProducing();
      close();
      plan->finished().Wait(5.0);
    }
  }
};

// Model a source backed by another plan: stopping it waits for its completion
// callback to return. Bound the wait so a reentrant stop fails instead of hanging.
class CompletionWaitingSource : public TestSource {
 public:
  CompletionWaitingSource(ac::ExecPlan* plan, std::shared_ptr<arrow::Schema> schema)
      : TestSource(plan, TestSourceOptions(std::move(schema), {})) {}

  Status StartProducing() override {
    plan()->query_context()->ScheduleTask(
        [this] {
          auto status = output_->InputFinished(this, 0);
          completion_returned_.MarkFinished();
          return status;
        },
        "CompletionWaitingSource::Finish");
    return Status::OK();
  }

  Status StopProducingImpl() override {
    stopped_.store(true);
    return completion_returned_.Wait(2.0)
               ? Status::OK()
               : Status::Invalid("Upstream stop waited on its own completion callback");
  }

  bool stopped() const { return stopped_.load(); }

 private:
  arrow::Future<> completion_returned_ = arrow::Future<>::Make();
  std::atomic<bool> stopped_{false};
};

Status TestAsofCompletionStopsUpstream() {
  auto schema = arrow::schema({arrow::field("on", arrow::int64())});
  ARROW_ASSIGN_OR_RAISE(auto plan, ac::ExecPlan::Make());
  auto* left = plan->EmplaceNode<CompletionWaitingSource>(plan.get(), schema);
  ARROW_ASSIGN_OR_RAISE(auto right, Source(schema, {}).AddToPlan(plan.get()));
  ARROW_ASSIGN_OR_RAISE(auto join, ac::MakeExecNode("acier_asofjoin", plan.get(),
                                                    {left, right}, AsofOptions()));
  auto consumer = std::make_shared<PausingConsumer>();
  ARROW_RETURN_NOT_OK(ac::MakeExecNode("consuming_sink", plan.get(), {join},
                                       ac::ConsumingSinkNodeOptions(consumer)));
  ARROW_RETURN_NOT_OK(plan->Validate());
  PlanGuard guard{plan, [] {}};
  plan->StartProducing();
  REQUIRE(plan->finished().Wait(5.0), "Completion did not finish the plan");
  ARROW_RETURN_NOT_OK(plan->finished().status());
  REQUIRE(left->stopped(), "Completion did not stop the upstream source");
  return Status::OK();
}

Status TestPauseResumeTailAndStop() {
  auto schema = arrow::schema({arrow::field("on", arrow::int64())});
  for (bool asof : {false, true}) {
    for (int action = 0; action < 3; ++action) {  // resume, flush while paused, stop
      auto consumer = std::make_shared<PausingConsumer>();
      arrow::PushGenerator<std::optional<ExecBatch>> left_generator, right_generator;
      ac::Declaration left{"source", ac::SourceNodeOptions(schema, left_generator,
                                                           cp::Ordering::Implicit())};
      ac::Declaration right{"source", ac::SourceNodeOptions(schema, right_generator,
                                                            cp::Ordering::Implicit())};
      ac::Declaration node =
          asof ? ac::Declaration("acier_asofjoin", {left, right}, AsofOptions())
               : ac::Declaration("acier_sorted_merge", {left, right},
                                 ac::OrderByNodeOptions(TimeOrder()));
      ac::Declaration sink{
          "consuming_sink", {node}, ac::ConsumingSinkNodeOptions(consumer, {}, false)};
      arrow::compute::ExecContext context(arrow::default_memory_pool(),
                                          arrow::internal::GetCpuThreadPool());
      ARROW_ASSIGN_OR_RAISE(auto plan, ac::ExecPlan::Make(context));
      ARROW_RETURN_NOT_OK(sink.AddToPlan(plan.get()).status());
      ARROW_RETURN_NOT_OK(plan->Validate());
      plan->StartProducing();
      auto close = [&] {
        left_generator.producer().Close();
        right_generator.producer().Close();
      };
      PlanGuard guard{plan, close};
      ARROW_ASSIGN_OR_RAISE(auto first_left, Batch({1}));
      ARROW_ASSIGN_OR_RAISE(auto first_right, Batch(asof ? std::vector<OptInt>{1, 2, 3, 4}
                                                         : std::vector<OptInt>{2}));
      left_generator.producer().Push(first_left);
      right_generator.producer().Push(first_right);
      ARROW_RETURN_NOT_OK(Until([&] { return consumer->paused(); }));
      const int paused_count = consumer->batches();
      ARROW_ASSIGN_OR_RAISE(auto second_left, Batch({3}));
      left_generator.producer().Push(second_left);
      if (!asof) {
        ARROW_ASSIGN_OR_RAISE(auto second_right, Batch({4}));
        right_generator.producer().Push(second_right);
      }
      arrow::internal::GetCpuThreadPool()->WaitForIdle();
      REQUIRE(consumer->batches() == paused_count, "Pause did not hold the next output");
      if (action == 2) {
        plan->StopProducing();
        close();
        REQUIRE(plan->finished().Wait(5.0), "Stop while paused did not finish");
        REQUIRE(plan->finished().status().IsCancelled(), "Stop lost cancellation status");
      } else {
        if (action == 0) consumer->Resume();
        close();
        REQUIRE(plan->finished().Wait(5.0), "Paused tail did not finish");
        ARROW_RETURN_NOT_OK(plan->finished().status());
        REQUIRE(consumer->rows() == (asof ? 2 : 4), "Resume/flush lost rows");
      }
      // The plan keeps the consumer's control alive until this late callback returns.
      consumer->Resume();
    }
  }
  return Status::OK();
}

class FailingConsumer : public ac::SinkNodeConsumer {
 public:
  Status Init(const std::shared_ptr<arrow::Schema>&, ac::BackpressureControl*,
              ac::ExecPlan*) override {
    return Status::OK();
  }
  Status Consume(ExecBatch) override {
    return Status::Invalid("intentional join sink error");
  }
  arrow::Future<> Finish() override { return arrow::Future<>::MakeFinished(); }
};

Status TestDownstreamError(bool threaded) {
  auto schema = arrow::schema({arrow::field("on", arrow::int64())});
  ARROW_ASSIGN_OR_RAISE(auto batch, Batch({1, 2}));
  for (bool asof : {false, true}) {
    auto node = asof ? ac::Declaration("acier_asofjoin",
                                       {Source(schema, {batch}), Source(schema, {batch})},
                                       AsofOptions())
                     : ac::Declaration("acier_sorted_merge", {Source(schema, {batch})},
                                       ac::OrderByNodeOptions(TimeOrder()));
    auto status = ac::DeclarationToStatus(
        {"consuming_sink",
         {node},
         ac::ConsumingSinkNodeOptions(std::make_shared<FailingConsumer>())},
        threaded);
    REQUIRE(status.IsInvalid() &&
                status.message().find("intentional join sink error") != std::string::npos,
            "Join lost a downstream error");
  }
  return Status::OK();
}

class LocalRegistry : public ac::ExecFactoryRegistry {
 public:
  Result<Factory> GetFactory(const std::string& name) override {
    auto it = factories_.find(name);
    return it == factories_.end() ? ac::default_exec_factory_registry()->GetFactory(name)
                                  : Result<Factory>(it->second);
  }
  Status AddFactory(std::string name, Factory factory) override {
    if (!factories_.emplace(std::move(name), std::move(factory)).second)
      return Status::AlreadyExists("Duplicate factory");
    return Status::OK();
  }

 private:
  std::map<std::string, Factory> factories_;
};

Status TestRegistryCoexistence() {
  LocalRegistry registry;
  ARROW_RETURN_NOT_OK(acier::internal::RegisterJoinNodes(&registry));
  REQUIRE(!acier::internal::RegisterJoinNodes(&registry).ok(),
          "Duplicate registration error was discarded");
  REQUIRE(!acier::internal::RegisterJoinNodes(nullptr).ok(),
          "Null registry was accepted");
  auto schema = arrow::schema({arrow::field("on", arrow::int64())});
  ARROW_ASSIGN_OR_RAISE(auto batch, Batch({1}));
  ARROW_ASSIGN_OR_RAISE(auto plan, ac::ExecPlan::Make());
  ARROW_ASSIGN_OR_RAISE(auto left,
                        Source(schema, {batch}).AddToPlan(plan.get(), &registry));
  ARROW_ASSIGN_OR_RAISE(auto right,
                        Source(schema, {batch}).AddToPlan(plan.get(), &registry));
  ARROW_ASSIGN_OR_RAISE(auto owned,
                        ac::MakeExecNode("acier_asofjoin", plan.get(), {left, right},
                                         AsofOptions(), &registry));
  REQUIRE(std::string(owned->kind_name()) == "AcierAsofJoinNode",
          "Wrong owned join factory");
  ARROW_ASSIGN_OR_RAISE(auto builtin_left,
                        Source(schema, {batch}).AddToPlan(plan.get(), &registry));
  ARROW_ASSIGN_OR_RAISE(auto builtin_right,
                        Source(schema, {batch}).AddToPlan(plan.get(), &registry));
  arrow::acero::AsofJoinNodeOptions original({{"on", {}}, {"on", {}}}, 0);
  ARROW_ASSIGN_OR_RAISE(
      auto builtin, ac::MakeExecNode("asofjoin", plan.get(),
                                     {builtin_left, builtin_right}, original, &registry));
  REQUIRE(std::string(builtin->kind_name()) == "AsofJoinNode",
          "Built-in asofjoin was replaced");
  REQUIRE(
      !ac::MakeExecNode("acier_asofjoin", plan.get(), {left, right}, original, &registry)
           .ok(),
      "Owned join accepted incompatible Arrow options");
  ARROW_ASSIGN_OR_RAISE(auto merge_source,
                        Source(schema, {batch}).AddToPlan(plan.get(), &registry));
  ARROW_ASSIGN_OR_RAISE(auto merge,
                        ac::MakeExecNode("acier_sorted_merge", plan.get(), {merge_source},
                                         ac::OrderByNodeOptions(TimeOrder()), &registry));
  REQUIRE(std::string(merge->kind_name()) == "AcierSortedMergeNode",
          "Wrong owned merge factory");
  ARROW_ASSIGN_OR_RAISE(auto builtin_merge_source,
                        Source(schema, {batch}).AddToPlan(plan.get(), &registry));
  ARROW_ASSIGN_OR_RAISE(
      auto builtin_merge,
      ac::MakeExecNode("sorted_merge", plan.get(), {builtin_merge_source},
                       ac::OrderByNodeOptions(TimeOrder()), &registry));
  REQUIRE(std::string(builtin_merge->kind_name()) == "SortedMergeNode",
          "Built-in sorted_merge was replaced");
  return Status::OK();
}

Status RunTests() {
  ARROW_RETURN_NOT_OK(cp::Initialize());
  ARROW_RETURN_NOT_OK(
      acier::internal::RegisterJoinNodes(ac::default_exec_factory_registry()));
  ARROW_RETURN_NOT_OK(RegisterTestSource());
  const std::vector<std::pair<const char*, std::function<Status(bool)>>> tests{
      {"asof payload, schema and batches", TestAsofPayloadAndBatches},
      {"asof keys and nulls", TestAsofKeysAndNulls},
      {"asof tolerance and duplicates", TestAsofTolerance},
      {"sorted nulls and payload", TestSortedNullsAndPayload},
      {"sequencing and output bounds", TestSequencingAndBatchLimits},
      {"ordering and errors", TestOrderingAndErrors},
      {"downstream errors", TestDownstreamError}};
  for (bool threaded : {false, true}) {
    for (const auto& test : tests) {
      std::cout << test.first << (threaded ? " [threaded]" : " [serial]") << std::endl;
      ARROW_RETURN_NOT_OK(test.second(threaded));
    }
  }
  std::cout << "asof completion stops upstream without reentry" << std::endl;
  ARROW_RETURN_NOT_OK(TestAsofCompletionStopsUpstream());
  std::cout << "pause, resume, tail flush and stop" << std::endl;
  ARROW_RETURN_NOT_OK(TestPauseResumeTailAndStop());
  std::cout << "factory coexistence and registration errors" << std::endl;
  return TestRegistryCoexistence();
}
}  // namespace

int main() {
  const auto status = RunTests();
  if (!status.ok()) {
    std::cerr << status << '\n';
    return 1;
  }
  return 0;
}

// SPDX-License-Identifier: Apache-2.0
#include <acier/api.h>
#include <arrow/acero/util.h>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/util/async_generator.h>
#include <arrow/util/key_value_metadata.h>

#include <iostream>
#include <optional>
#include <unordered_map>

#define CHECK(condition)                                                           \
  do {                                                                             \
    if (!(condition)) return arrow::Status::Invalid("Check failed: ", #condition); \
  } while (false)

using arrow::Status;
using arrow::acero::Declaration;
using arrow::compute::call;
using arrow::compute::field_ref;
using arrow::compute::literal;

arrow::Result<std::shared_ptr<arrow::Array>> Integers(
    std::initializer_list<std::optional<int64_t>> values) {
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

class Registry : public arrow::acero::ExecFactoryRegistry {
 public:
  arrow::Result<Factory> GetFactory(const std::string& name) override {
    auto it = factories.find(name);
    if (it == factories.end()) return arrow::Status::KeyError(name);
    return it->second;
  }
  Status AddFactory(std::string name, Factory factory) override {
    if (!factories.emplace(name, std::move(factory)).second)
      return Status::KeyError("Duplicate ", name);
    return Status::OK();
  }
  std::unordered_map<std::string, Factory> factories;
};

Status Filter(bool threaded) {
  ARROW_ASSIGN_OR_RAISE(auto x, Integers({1, std::nullopt, 3, 4}));
  ARROW_ASSIGN_OR_RAISE(auto y, Integers({std::nullopt, 2, 3, 4}));
  auto metadata = arrow::key_value_metadata({"origin"}, {"core_test"});
  auto schema = arrow::schema(
      {arrow::field("x", arrow::int64()), arrow::field("y", arrow::int64())}, metadata);
  auto table = arrow::Table::Make(schema, {x, y});
  auto declaration = Declaration::Sequence(
      {{"acier_table_source", acier::TableSourceNodeOptions(table, 1)},
       {"acier_filter",
        acier::FilterNodeOptions(call("greater", {field_ref("x"), literal(int64_t{0})}),
                                 {"x", "y"})}});
  ARROW_ASSIGN_OR_RAISE(auto result, acier::DeclarationToTable(declaration, threaded));
  ARROW_RETURN_NOT_OK(result->ValidateFull());
  CHECK(result->num_rows() == 2);
  CHECK(!result->schema()->field(0)->nullable());
  CHECK(!result->schema()->field(1)->nullable());
  CHECK(result->schema()->metadata()->Equals(*metadata));
  CHECK(result->column(0)->GetScalar(0).ValueOrDie()->Equals(arrow::Int64Scalar(3)));

  auto wrong_options = Declaration::Sequence(
      {{"acier_table_source", acier::TableSourceNodeOptions(table)},
       {"acier_filter", arrow::acero::FilterNodeOptions(literal(true))}});
  CHECK(acier::DeclarationToSchema(wrong_options).status().IsInvalid());

  auto invalid = Declaration::Sequence(
      {{"acier_table_source", acier::TableSourceNodeOptions(table)},
       {"acier_filter", acier::FilterNodeOptions(literal(true), {"missing"})}});
  CHECK(acier::DeclarationToSchema(invalid).status().IsKeyError());
  auto passthrough =
      Declaration::Sequence({{"acier_table_source", acier::TableSourceNodeOptions(table)},
                             {"acier_filter", acier::FilterNodeOptions(literal(true))}});
  ARROW_ASSIGN_OR_RAISE(auto unchanged, acier::DeclarationToTable(passthrough, threaded));
  CHECK(unchanged->Equals(*table));
  // A null scalar predicate drops all rows. A scalar data column remains scalar
  // until the table sink materializes it.
  auto batch_schema = arrow::schema({arrow::field("x", arrow::int64())});
  arrow::compute::ExecBatch batch({arrow::Datum(int64_t{7})}, 4);
  auto generator =
      arrow::MakeVectorGenerator<std::optional<arrow::compute::ExecBatch>>({batch});
  auto scalar = Declaration::Sequence(
      {{"acier_source", acier::SourceNodeOptions(batch_schema, generator)},
       {"acier_filter",
        acier::FilterNodeOptions(literal(arrow::MakeNullScalar(arrow::boolean())))}});
  ARROW_ASSIGN_OR_RAISE(auto empty, acier::DeclarationToTable(scalar, threaded));
  CHECK(empty->num_rows() == 0);
  return Status::OK();
}

Status Readers(bool threaded) {
  ARROW_ASSIGN_OR_RAISE(auto x, Integers({1, 2, 3, 4}));
  auto schema = arrow::schema({arrow::field("x", arrow::int64())});
  auto batch = arrow::RecordBatch::Make(schema, 4, {x});
  for (bool ordered : {false, true}) {
    ARROW_ASSIGN_OR_RAISE(auto reader,
                          arrow::RecordBatchReader::Make({batch, batch}, schema));
    auto opts = acier::RecordBatchReaderSourceNodeOptions(reader, nullptr, ordered);
    ARROW_ASSIGN_OR_RAISE(auto plan, acier::ExecPlan::Make());
    ARROW_ASSIGN_OR_RAISE(
        auto node,
        acier::MakeExecNode("acier_record_batch_reader_source", plan.get(), {}, opts));
    CHECK(node->ordering().is_unordered() == !ordered);
    CHECK(acier::MakeExecNode("acier_record_batch_reader_source", plan.get(), {},
                              arrow::acero::RecordBatchReaderSourceNodeOptions(reader))
              .status()
              .IsInvalid());
    // Use a separate reader: constructing a source must not consume its input.
    ARROW_ASSIGN_OR_RAISE(
        auto out, acier::DeclarationToTable(
                      Declaration("acier_record_batch_reader_source", opts), threaded));
    CHECK(out->num_rows() == 8);
    if (ordered) {
      for (int i = 0; i < 8; ++i)
        CHECK(out->column(0)->GetScalar(i).ValueOrDie()->Equals(
            arrow::Int64Scalar(i % 4 + 1)));
    }
  }
  CHECK(!acier::RecordBatchReaderSourceNodeOptions(nullptr).implicit_ordering);
  CHECK(!acier::DeclarationToSchema(
             Declaration("acier_record_batch_reader_source",
                         acier::RecordBatchReaderSourceNodeOptions(nullptr)))
             .ok());
  return Status::OK();
}

Status IteratorSources(bool threaded) {
  ARROW_ASSIGN_OR_RAISE(auto array, Integers({1, 2, 3}));
  auto schema = arrow::schema({arrow::field("x", arrow::int64())});
  auto record = arrow::RecordBatch::Make(schema, 3, {array});
  auto exec = std::make_shared<arrow::compute::ExecBatch>(*record);
  auto columns = std::make_shared<arrow::ArrayVector>(arrow::ArrayVector{array});
  for (bool requires_io : {false, true}) {
    std::vector<Declaration> declarations = {
        {"acier_record_batch_source",
         acier::RecordBatchSourceNodeOptions(
             schema,
             [record] {
               return arrow::MakeVectorIterator<std::shared_ptr<arrow::RecordBatch>>(
                   {record});
             },
             requires_io)},
        {"acier_exec_batch_source", acier::ExecBatchSourceNodeOptions(
                                        schema,
                                        [exec] {
                                          return arrow::MakeVectorIterator<
                                              std::shared_ptr<arrow::compute::ExecBatch>>(
                                              {exec});
                                        },
                                        requires_io)},
        {"acier_array_vector_source",
         acier::ArrayVectorSourceNodeOptions(
             schema,
             [columns] {
               return arrow::MakeVectorIterator<std::shared_ptr<arrow::ArrayVector>>(
                   {columns});
             },
             requires_io)}};
    for (auto& declaration : declarations) {
      ARROW_ASSIGN_OR_RAISE(auto out, acier::DeclarationToTable(declaration, threaded));
      CHECK(out->num_rows() == 3);
      CHECK(out->column(0)->chunk(0)->Equals(array));
    }
    auto invalid =
        Declaration("acier_record_batch_source",
                    acier::RecordBatchSourceNodeOptions(
                        schema,
                        [] {
                          return arrow::MakeFunctionIterator(
                              []() -> arrow::Result<std::shared_ptr<arrow::RecordBatch>> {
                                return Status::Invalid("deliberate iterator failure");
                              });
                        },
                        requires_io));
    CHECK(acier::DeclarationToTable(invalid, threaded).status().IsInvalid());
  }
  return Status::OK();
}

Status Pivot(bool threaded) {
  ARROW_ASSIGN_OR_RAISE(auto a, Integers({1, 2}));
  ARROW_ASSIGN_OR_RAISE(auto b, Integers({10, 20}));
  auto schema = arrow::schema(
      {arrow::field("a", arrow::int64()), arrow::field("b", arrow::int64())});
  auto table = arrow::Table::Make(schema, {a, b});
  acier::PivotLongerNodeOptions opts;
  opts.feature_field_names = {"kind"};
  opts.measurement_field_names = {"measurement"};
  opts.row_templates = {
      acier::PivotLongerRowTemplate({std::make_shared<arrow::UInt64Scalar>(1)},
                                    {arrow::FieldRef("a")}),
      acier::PivotLongerRowTemplate({std::make_shared<arrow::UInt64Scalar>(2)},
                                    {arrow::FieldRef("b")}),
      acier::PivotLongerRowTemplate({arrow::MakeNullScalar(arrow::uint64())},
                                    {std::nullopt})};
  CHECK(acier::DeclarationToSchema(
            Declaration::Sequence(
                {{"acier_table_source", acier::TableSourceNodeOptions(table)},
                 {"acier_pivot_longer", arrow::acero::PivotLongerNodeOptions{}}}))
            .status()
            .IsInvalid());
  auto decl =
      Declaration::Sequence({{"acier_table_source", acier::TableSourceNodeOptions(table)},
                             {"acier_pivot_longer", opts}});
  ARROW_ASSIGN_OR_RAISE(auto out, acier::DeclarationToTable(decl, threaded));
  ARROW_RETURN_NOT_OK(out->ValidateFull());
  CHECK(out->num_rows() == 6);
  CHECK(out->column(2)->type()->Equals(arrow::uint64()));
  CHECK(out->column(2)->null_count() == 2);
  CHECK(out->column(3)->null_count() == 2);
  opts.row_templates[0].feature_values[0].reset();
  CHECK(!acier::DeclarationToSchema(
             Declaration::Sequence(
                 {{"acier_table_source", acier::TableSourceNodeOptions(table)},
                  {"acier_pivot_longer", opts}}))
             .ok());
  return Status::OK();
}

Status StopWhilePending() {
  for (int iteration = 0; iteration < 20; ++iteration) {
    auto pending = arrow::Future<std::optional<arrow::compute::ExecBatch>>::Make();
    auto schema = arrow::schema({arrow::field("x", arrow::int64())});
    ARROW_ASSIGN_OR_RAISE(auto plan, acier::ExecPlan::Make());
    ARROW_ASSIGN_OR_RAISE(
        auto source, acier::MakeExecNode("acier_source", plan.get(), {},
                                         acier::SourceNodeOptions(
                                             schema, [pending] { return pending; })));
    ARROW_ASSIGN_OR_RAISE(
        auto sink, acier::MakeExecNode("consuming_sink", plan.get(), {source},
                                       acier::ConsumingSinkNodeOptions(
                                           arrow::acero::NullSinkNodeConsumer::Make())));
    ARROW_RETURN_NOT_OK(plan->Validate());
    plan->StartProducing();
    source->PauseProducing(sink, 1);
    plan->StopProducing();
    source->PauseProducing(sink, 3);
    source->ResumeProducing(sink, 2);  // stale callback after stop
    pending.MarkFinished(std::nullopt);
    CHECK(plan->finished().Wait(5));
    // Cancellation may report Cancelled; the lifecycle invariant is completion.
    CHECK(plan->finished().status().ok() || plan->finished().status().IsCancelled());
  }
  return Status::OK();
}

Status Run() {
  ARROW_RETURN_NOT_OK(acier::Initialize());
  ARROW_RETURN_NOT_OK(acier::Initialize());
  Registry custom;
  ARROW_RETURN_NOT_OK(acier::RegisterNodeFactories(&custom));
  CHECK(custom.GetFactory("acier_filter").ok());
  CHECK(!acier::RegisterNodeFactories(&custom).ok());
  CHECK(!acier::RegisterNodeFactories(nullptr).ok());
  CHECK(acier::default_exec_factory_registry()->GetFactory("filter").ok());
  for (bool threaded : {false, true}) {
    ARROW_RETURN_NOT_OK(Filter(threaded));
    ARROW_RETURN_NOT_OK(Readers(threaded));
    ARROW_RETURN_NOT_OK(IteratorSources(threaded));
    ARROW_RETURN_NOT_OK(Pivot(threaded));
  }
  return StopWhilePending();
}
int main() {
  auto status = Run();
  if (!status.ok()) {
    std::cerr << status << '\n';
    return 1;
  }
  std::cout << "Core node checks passed\n";
}

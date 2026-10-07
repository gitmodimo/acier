// SPDX-License-Identifier: Apache-2.0
#include <acier/api.h>
#include <acier/dataset/api.h>
#include <arrow/api.h>
#include <arrow/dataset/file_ipc.h>
#include <arrow/dataset/partition.h>
#include <arrow/filesystem/mockfs.h>
#include <arrow/ipc/api.h>
#include <arrow/util/key_value_metadata.h>

#include <atomic>
#include <iostream>
#include <map>
#include <thread>

#include "../src/dataset/writer_task.h"

#define CHECK(condition)                                                           \
  do {                                                                             \
    if (!(condition)) return arrow::Status::Invalid("Check failed: ", #condition); \
  } while (false)
using arrow::Status;
using arrow::acero::Declaration;

arrow::Result<std::shared_ptr<arrow::Table>> ReadFile(
    const std::shared_ptr<arrow::fs::FileSystem>& filesystem, const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(auto file, filesystem->OpenInputFile(path));
  ARROW_ASSIGN_OR_RAISE(auto reader, arrow::ipc::RecordBatchFileReader::Open(file));
  arrow::RecordBatchVector batches;
  for (int i = 0; i < reader->num_record_batches(); ++i) {
    ARROW_ASSIGN_OR_RAISE(auto batch, reader->ReadRecordBatch(i));
    ARROW_RETURN_NOT_OK(batch->ValidateFull());
    batches.push_back(batch);
  }
  return arrow::Table::FromRecordBatches(reader->schema(), batches);
}

Status Write(bool tee, bool threaded, bool partitioned) {
  std::cerr << "Write tee=" << tee << " threaded=" << threaded
            << " partitioned=" << partitioned << std::endl;
  auto fs = std::make_shared<arrow::fs::internal::MockFileSystem>(arrow::fs::TimePoint{});
  auto format = std::make_shared<arrow::dataset::IpcFileFormat>();
  arrow::Int64Builder values, keys;
  for (int64_t i = 0; i < 20; ++i) {
    ARROW_RETURN_NOT_OK(values.Append(i));
    ARROW_RETURN_NOT_OK(keys.Append(i % 2));
  }
  ARROW_ASSIGN_OR_RAISE(auto x, values.Finish());
  ARROW_ASSIGN_OR_RAISE(auto k, keys.Finish());
  auto schema = arrow::schema(
      {arrow::field("x", arrow::int64()), arrow::field("group", arrow::int64())});
  auto table = arrow::Table::Make(schema, {x, k});
  arrow::dataset::FileSystemDatasetWriteOptions write;
  write.filesystem = fs;
  write.file_write_options = format->DefaultWriteOptions();
  write.base_dir = "output";
  write.basename_template = "part-{i}.arrow";
  write.partitioning = std::make_shared<arrow::dataset::DirectoryPartitioning>(
      partitioned ? arrow::schema({schema->field(1)}) : arrow::schema({}));
  write.preserve_order = true;
  write.max_rows_per_group = 3;
  if (partitioned) write.max_rows_per_file = 4;
  auto metadata = arrow::key_value_metadata({"acier-test"}, {"preserved"});
  acier::dataset::WriteNodeOptions options(write, metadata);
  options.max_rows_queued = 1;  // Every source batch exceeds the watermark.
  auto declaration = Declaration::Sequence(
      {{"acier_table_source", acier::TableSourceNodeOptions(table, 2)},
       {tee ? "acier_tee" : "acier_write", options}});
  if (tee) {
    ARROW_ASSIGN_OR_RAISE(auto forwarded,
                          acier::DeclarationToTable(declaration, threaded));
    ARROW_RETURN_NOT_OK(forwarded->ValidateFull());
    CHECK(forwarded->Equals(*table, false));
    CHECK(forwarded->schema()->metadata()->Equals(*metadata));
  } else {
    ARROW_RETURN_NOT_OK(acier::DeclarationToStatus(declaration, threaded));
  }
  auto files = fs->AllFiles();
  CHECK(files.size() == (partitioned ? 6U : 1U));
  int64_t rows = 0;
  std::map<int64_t, std::vector<int64_t>> actual;
  for (const auto& file : files) {
    ARROW_ASSIGN_OR_RAISE(auto stored, ReadFile(fs, file.full_path));
    rows += stored->num_rows();
    CHECK(stored->schema()->metadata()->Equals(*metadata));
    CHECK(stored->num_columns() == (partitioned ? 1 : 2));
    if (partitioned) CHECK(stored->num_rows() <= 4);
    int64_t group =
        partitioned ? (file.full_path.find("/1/") != std::string::npos ? 1 : 0) : 0;
    for (int64_t row = 0; row < stored->num_rows(); ++row) {
      ARROW_ASSIGN_OR_RAISE(auto value, stored->column(0)->GetScalar(row));
      actual[group].push_back(static_cast<const arrow::Int64Scalar&>(*value).value);
    }
  }
  CHECK(rows == 20);
  for (auto& [group, sequence] : actual) {
    for (size_t i = 0; i < sequence.size(); ++i)
      CHECK(sequence[i] == static_cast<int64_t>(partitioned ? i * 2 + group : i));
  }
  // Error propagation from asynchronous file finalization must reach the plan.
  write.base_dir = "failure";
  write.writer_pre_finish = [](arrow::dataset::FileWriter*) {
    return Status::IOError("deliberate writer failure");
  };
  options.write_options = write;
  auto failing =
      Declaration::Sequence({{"acier_table_source", acier::TableSourceNodeOptions(table)},
                             {tee ? "acier_tee" : "acier_write", options}});
  std::cerr << "  testing finalization error" << std::endl;
  auto error = tee ? acier::DeclarationToTable(failing, threaded).status()
                   : acier::DeclarationToStatus(failing, threaded);
  CHECK(error.IsIOError());
  return Status::OK();
}

Status Validation() {
  auto schema = arrow::schema({arrow::field("x", arrow::int64())});
  auto format = std::make_shared<arrow::dataset::IpcFileFormat>();
  arrow::dataset::FileSystemDatasetWriteOptions write;
  write.file_write_options = format->DefaultWriteOptions();
  write.partitioning =
      std::make_shared<arrow::dataset::DirectoryPartitioning>(arrow::schema({}));
  auto source = Declaration(
      "acier_source", acier::SourceNodeOptions(schema, [] {
        return arrow::Future<std::optional<arrow::compute::ExecBatch>>::MakeFinished(
            std::nullopt);
      }));
  for (const auto& name : {"acier_write", "acier_tee"}) {
    CHECK(acier::DeclarationToSchema(
              Declaration(name, {source}, arrow::dataset::WriteNodeOptions(write)))
              .status()
              .IsInvalid());
    acier::dataset::WriteNodeOptions opts(write);
    opts.max_rows_queued = 0;
    CHECK(!acier::DeclarationToSchema(Declaration(name, {source}, opts)).ok());
    opts.max_rows_queued = 1;
    opts.custom_schema = arrow::schema({arrow::field("x", arrow::float64())});
    CHECK(acier::DeclarationToSchema(Declaration(name, {source}, opts))
              .status()
              .IsTypeError());
    opts.custom_schema = schema;
    opts.custom_metadata = arrow::key_value_metadata({"a"}, {"b"});
    CHECK(acier::DeclarationToSchema(Declaration(name, {source}, opts))
              .status()
              .IsTypeError());
    opts.custom_metadata.reset();
    opts.write_options.partitioning.reset();
    CHECK(!acier::DeclarationToSchema(Declaration(name, {source}, opts)).ok());
  }
  acier::dataset::WriteNodeOptions opts(write);
  opts.write_options.preserve_order = true;
  CHECK(!acier::DeclarationToSchema(Declaration("acier_tee", {source}, opts)).ok());
  return Status::OK();
}

Status Cancellation(bool tee) {
  auto fs = std::make_shared<arrow::fs::internal::MockFileSystem>(arrow::fs::TimePoint{});
  auto format = std::make_shared<arrow::dataset::IpcFileFormat>();
  arrow::Int64Builder builder;
  for (int i = 0; i < 64; ++i) ARROW_RETURN_NOT_OK(builder.Append(i));
  ARROW_ASSIGN_OR_RAISE(auto values, builder.Finish());
  auto schema = arrow::schema({arrow::field("x", arrow::int64())});
  auto table = arrow::Table::Make(schema, {values});
  arrow::dataset::FileSystemDatasetWriteOptions write;
  write.filesystem = fs;
  write.file_write_options = format->DefaultWriteOptions();
  write.base_dir = "cancel";
  write.basename_template = "part-{i}.arrow";
  write.partitioning =
      std::make_shared<arrow::dataset::DirectoryPartitioning>(arrow::schema({}));
  write.preserve_order = true;
  write.max_rows_per_group = 2;
  write.max_rows_per_file = 2;
  auto entered = arrow::Future<>::Make();
  auto release = arrow::Future<>::Make();
  std::atomic<bool> first{true};
  write.writer_pre_finish = [&first, entered,
                             release](arrow::dataset::FileWriter*) mutable {
    if (first.exchange(false)) {
      entered.MarkFinished();
      release.Wait();
    }
    return Status::OK();
  };
  acier::dataset::WriteNodeOptions options(write);
  options.max_rows_queued = 1;
  auto declaration = Declaration::Sequence(
      {{"acier_table_source", acier::TableSourceNodeOptions(table, 2)},
       {tee ? "acier_tee" : "acier_write", options}});
  ARROW_ASSIGN_OR_RAISE(auto plan, acier::ExecPlan::Make());
  ARROW_ASSIGN_OR_RAISE(auto terminal, declaration.AddToPlan(plan.get()));
  std::shared_ptr<arrow::Table> output;
  if (tee) {
    ARROW_ASSIGN_OR_RAISE(auto sink,
                          acier::MakeExecNode("table_sink", plan.get(), {terminal},
                                              acier::TableSinkNodeOptions(&output)));
    (void)sink;
  }
  ARROW_RETURN_NOT_OK(plan->Validate());
  std::thread start([plan] { plan->StartProducing(); });
  bool reached_finish = entered.Wait(5);
  plan->StopProducing();
  release.MarkFinished();
  start.join();
  CHECK(reached_finish);
  CHECK(plan->finished().Wait(5));
  CHECK(plan->finished().status().ok() || plan->finished().status().IsCancelled());
  return Status::OK();
}

Status ObserverLifetime() {
  auto original = arrow::Future<>::Make();
  auto entered = arrow::Future<>::Make();
  auto release = arrow::Future<>::Make();
  auto observed = acier::dataset::internal::ObserveWriterTask(
      original, [entered, release](const Status&) mutable {
        entered.MarkFinished();
        release.Wait();
      });
  std::thread complete([original]() mutable {
    original.MarkFinished(Status::IOError("observed failure"));
  });
  bool observer_started = entered.Wait(5);
  bool finished_early = observed.is_finished();
  release.MarkFinished();
  complete.join();
  CHECK(observer_started);
  CHECK(!finished_early);
  CHECK(observed.Wait(5));
  CHECK(observed.status().IsIOError());
  bool called = false;
  auto success = acier::dataset::internal::ObserveWriterTask(
      arrow::Future<>::MakeFinished(), [&called](const Status&) { called = true; });
  CHECK(success.status().ok());
  CHECK(!called);
  return Status::OK();
}

Status Run() {
  ARROW_RETURN_NOT_OK(acier::Initialize());
  ARROW_RETURN_NOT_OK(Validation());
  ARROW_RETURN_NOT_OK(ObserverLifetime());
  ARROW_RETURN_NOT_OK(Cancellation(false));
  ARROW_RETURN_NOT_OK(Cancellation(true));
  for (bool tee : {false, true})
    for (bool threaded : {false, true})
      for (bool partitioned : {false, true})
        ARROW_RETURN_NOT_OK(Write(tee, threaded, partitioned));
  return Status::OK();
}
int main() {
  auto status = Run();
  if (!status.ok()) {
    std::cerr << status << '\n';
    return 1;
  }
  std::cout << "Dataset writer and tee checks passed\n";
}

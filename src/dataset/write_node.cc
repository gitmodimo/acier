// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
// Modified for Acier; see docs/components/dataset_write.md.

#include <atomic>
#include <mutex>

#include "acier/dataset/write_options.h"
#include "arrow/acero/accumulation_queue.h"
#include "arrow/acero/map_node.h"
#include "arrow/acero/query_context.h"
#include "arrow/acero/util.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/dataset/partition.h"
#include "arrow/record_batch.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "writer.h"

namespace acier::dataset {
using namespace arrow;
using namespace arrow::dataset;
using arrow::internal::checked_cast;
namespace {
Status WriteBatch(
    std::shared_ptr<RecordBatch> batch, compute::Expression guarantee,
    FileSystemDatasetWriteOptions write_options,
    std::function<Status(std::shared_ptr<RecordBatch>, const PartitionPathFormat&)>
        write) {
  ARROW_ASSIGN_OR_RAISE(auto groups, write_options.partitioning->Partition(batch));
  batch.reset();  // drop to hopefully conserve memory

  if (write_options.max_partitions <= 0) {
    return Status::Invalid("max_partitions must be positive (was ",
                           write_options.max_partitions, ")");
  }

  if (groups.batches.size() > static_cast<size_t>(write_options.max_partitions)) {
    return Status::Invalid("Fragment would be written into ", groups.batches.size(),
                           " partitions. This exceeds the maximum of ",
                           write_options.max_partitions);
  }

  for (std::size_t index = 0; index < groups.batches.size(); index++) {
    auto partition_expression = and_(groups.expressions[index], guarantee);
    auto next_batch = groups.batches[index];
    PartitionPathFormat destination;
    ARROW_ASSIGN_OR_RAISE(destination,
                          write_options.partitioning->Format(partition_expression));
    RETURN_NOT_OK(write(next_batch, destination));
  }
  return Status::OK();
}

Status ValidateAndPrepareSchema(const WriteNodeOptions& write_node_options,
                                const std::shared_ptr<Schema>& input_schema,
                                std::shared_ptr<Schema>& custom_schema) {
  if (write_node_options.max_rows_queued == 0) {
    return Status::Invalid("max_rows_queued must be positive");
  }
  custom_schema = write_node_options.custom_schema;
  const std::shared_ptr<const KeyValueMetadata>& custom_metadata =
      write_node_options.custom_metadata;
  const FileSystemDatasetWriteOptions& write_options = write_node_options.write_options;

  if (custom_schema != nullptr) {
    if (custom_metadata) {
      return Status::TypeError(
          "Do not provide both custom_metadata and custom_schema.  If "
          "custom_schema is "
          "used then custom_schema->metadata should be used instead of "
          "custom_metadata");
    }

    if (custom_schema->num_fields() != input_schema->num_fields()) {
      return Status::TypeError(
          "The provided custom_schema did not have the same number of fields "
          "as the "
          "data.  The custom schema can only be used to add metadata / "
          "nullability to "
          "fields and cannot change the type or number of fields.");
    }
    for (int field_idx = 0; field_idx < input_schema->num_fields(); field_idx++) {
      if (!input_schema->field(field_idx)->type()->Equals(
              custom_schema->field(field_idx)->type())) {
        return Status::TypeError("The provided custom_schema specified type ",
                                 custom_schema->field(field_idx)->type()->ToString(),
                                 " for field ", field_idx, "and the input data has type ",
                                 input_schema->field(field_idx),
                                 "The custom schema can only be used to add metadata / "
                                 "nullability to fields and "
                                 "cannot change the type or number of fields.");
      }
    }
  } else {
    custom_schema = input_schema;
  }

  if (custom_metadata) {
    custom_schema = input_schema->WithMetadata(custom_metadata);
  }

  if (!write_options.partitioning) {
    return Status::Invalid("Must provide partitioning");
  }

  return Status::OK();
}
class WriteNode : public acero::ExecNode,
                  public acero::util::SerialSequencingQueue::Processor {
 public:
  WriteNode(acero::ExecPlan* plan, std::vector<acero::ExecNode*> inputs,
            std::shared_ptr<Schema> schema, WriteNodeOptions options)
      : ExecNode(plan, std::move(inputs), {"input"}, nullptr),
        schema_(std::move(schema)),
        options_(std::move(options)) {
    if (options_.write_options.preserve_order)
      sequencer_ = acero::util::SerialSequencingQueue::Make(this);
  }
  static Result<acero::ExecNode*> Make(acero::ExecPlan* plan,
                                       std::vector<acero::ExecNode*> inputs,
                                       const acero::ExecNodeOptions& options) {
    ARROW_RETURN_NOT_OK(acero::ValidateExecNodeInputs(plan, inputs, 1, "acier_write"));
    const auto* checked_options = dynamic_cast<const WriteNodeOptions*>(&options);
    if (!checked_options)
      return Status::Invalid("acier_write requires acier::dataset::WriteNodeOptions");
    const auto& opts = *checked_options;
    if (opts.write_options.preserve_order && inputs[0]->ordering().is_unordered())
      return Status::Invalid("acier_write preserve_order requires ordered input");
    std::shared_ptr<Schema> schema;
    ARROW_RETURN_NOT_OK(
        ValidateAndPrepareSchema(opts, inputs[0]->output_schema(), schema));
    return plan->EmplaceNode<WriteNode>(plan, std::move(inputs), std::move(schema), opts);
  }
  const char* kind_name() const override { return "AcierWriteNode"; }
  Status Validate() const override {
    ARROW_RETURN_NOT_OK(ExecNode::Validate());
    if (output_) return Status::Invalid("acier_write must be a terminal node");
    return Status::OK();
  }
  Status StartProducing() override {
    std::lock_guard<std::mutex> lock(start_mutex_);
    if (stopped_) return Status::OK();
    ARROW_ASSIGN_OR_RAISE(
        writer_, internal::DatasetWriter::Make(
                     options_.write_options, plan_->query_context()->async_scheduler(),
                     [this] { inputs_[0]->PauseProducing(this, ++pause_counter_); },
                     [this] { inputs_[0]->ResumeProducing(this, ++pause_counter_); },
                     [] {}, options_.max_rows_queued));
    return Status::OK();
  }
  void PauseProducing(acero::ExecNode*, int32_t) override {}
  void ResumeProducing(acero::ExecNode*, int32_t) override {}
  Status StopProducingImpl() override {
    internal::DatasetWriter* writer;
    {
      std::lock_guard<std::mutex> lock(start_mutex_);
      stopped_ = true;
      writer = writer_.get();
    }
    if (writer) writer->Abort(Status::Cancelled("Dataset write stopped"));
    return Status::OK();
  }
  Status InputReceived(acero::ExecNode*, compute::ExecBatch batch) override {
    if (stopped_) return Status::OK();
    if (sequencer_) return sequencer_->InsertBatch(std::move(batch));
    return Process(std::move(batch));
  }
  Status Process(compute::ExecBatch batch) override {
    if (stopped_) return Status::OK();
    ARROW_ASSIGN_OR_RAISE(auto record_batch, batch.ToRecordBatch(schema_));
    ARROW_RETURN_NOT_OK(WriteBatch(
        record_batch, batch.guarantee, options_.write_options,
        [this](std::shared_ptr<RecordBatch> next, const PartitionPathFormat& path) {
          writer_->WriteRecordBatch(std::move(next), path.directory, path.filename);
          return Status::OK();
        }));
    if (counter_.Increment()) writer_->Finish();
    return Status::OK();
  }
  Status InputFinished(acero::ExecNode*, int total) override {
    if (!stopped_ && counter_.SetTotal(total)) writer_->Finish();
    return Status::OK();
  }

 private:
  std::shared_ptr<Schema> schema_;
  WriteNodeOptions options_;
  std::unique_ptr<internal::DatasetWriter> writer_;
  std::unique_ptr<acero::util::SerialSequencingQueue> sequencer_;
  acero::AtomicCounter counter_;
  std::atomic<int32_t> pause_counter_{0};
  std::atomic<bool> stopped_{false};
  std::mutex start_mutex_;
};

class TeeNode : public acero::MapNode,
                public arrow::acero::util::SerialSequencingQueue::Processor {
 public:
  TeeNode(acero::ExecPlan* plan, std::vector<acero::ExecNode*> inputs,
          std::shared_ptr<Schema> output_schema,
          FileSystemDatasetWriteOptions write_options, uint64_t max_rows_queued)
      : MapNode(plan, std::move(inputs), std::move(output_schema)),
        write_options_(std::move(write_options)),
        max_rows_queued_(max_rows_queued) {
    if (write_options_.preserve_order) {
      sequencer_ = acero::util::SerialSequencingQueue::Make(this);
    }
  }

  Status StartProducing() override {
    std::lock_guard<std::mutex> lock(start_mutex_);
    if (stopped_) return Status::OK();
    ARROW_ASSIGN_OR_RAISE(dataset_writer_,
                          acier::dataset::internal::DatasetWriter::Make(
                              write_options_, plan_->query_context()->async_scheduler(),
                              [this] { Pause(); }, [this] { Resume(); },
                              [this] { MapNode::Finish(); }, max_rows_queued_));
    return MapNode::StartProducing();
  }

  static Result<acero::ExecNode*> Make(acero::ExecPlan* plan,
                                       std::vector<acero::ExecNode*> inputs,
                                       const acero::ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "TeeNode"));

    const auto* checked_options = dynamic_cast<const WriteNodeOptions*>(&options);
    if (!checked_options)
      return Status::Invalid("acier_tee requires acier::dataset::WriteNodeOptions");
    const auto& write_node_options = *checked_options;
    if (write_node_options.write_options.preserve_order &&
        inputs[0]->ordering().is_unordered()) {
      return Status::Invalid("acier_tee preserve_order requires ordered input");
    }
    const std::shared_ptr<Schema>& input_schema = inputs[0]->output_schema();
    std::shared_ptr<Schema> custom_schema;

    ARROW_RETURN_NOT_OK(
        ValidateAndPrepareSchema(write_node_options, input_schema, custom_schema));

    return plan->EmplaceNode<TeeNode>(plan, std::move(inputs), std::move(custom_schema),
                                      std::move(write_node_options.write_options),
                                      write_node_options.max_rows_queued);
  }

  const char* kind_name() const override { return "TeeNode"; }

  Status InputReceived(ExecNode* input, ExecBatch batch) override {
    if (stopped_) return Status::OK();
    ARROW_DCHECK_EQ(input, inputs_[0]);
    if (sequencer_) {
      return sequencer_->InsertBatch(std::move(batch));
    }
    return Process(std::move(batch));
  }

  Status Process(ExecBatch batch) override {
    if (stopped_) return Status::OK();
    return acero::MapNode::InputReceived(inputs_[0], batch);
  }

  void Finish() override {
    if (!stopped_) dataset_writer_->Finish();
  }

  Status StopProducingImpl() override {
    internal::DatasetWriter* writer;
    {
      std::lock_guard<std::mutex> lock(start_mutex_);
      stopped_ = true;
      writer = dataset_writer_.get();
    }
    if (writer) writer->Abort(Status::Cancelled("Dataset tee stopped"));
    return Status::OK();
  }

  Result<compute::ExecBatch> ProcessBatch(compute::ExecBatch batch) override {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<RecordBatch> record_batch,
                          batch.ToRecordBatch(output_schema()));
    ARROW_RETURN_NOT_OK(WriteNextBatch(std::move(record_batch), batch.guarantee));
    return batch;
  }

  Status WriteNextBatch(std::shared_ptr<RecordBatch> batch,
                        compute::Expression guarantee) {
    return WriteBatch(batch, guarantee, write_options_,
                      [this](std::shared_ptr<RecordBatch> next_batch,
                             const PartitionPathFormat& destination) {
                        dataset_writer_->WriteRecordBatch(
                            next_batch, destination.directory, destination.filename);
                        return Status::OK();
                      });
  }

  void Pause() { UpdatePause(true, true, 0); }
  void Resume() { UpdatePause(true, false, 0); }
  void PauseProducing(acero::ExecNode*, int32_t counter) override {
    UpdatePause(false, true, counter);
  }
  void ResumeProducing(acero::ExecNode*, int32_t counter) override {
    UpdatePause(false, false, counter);
  }

  void UpdatePause(bool writer, bool paused, int32_t counter) {
    bool combined;
    int32_t upstream_counter;
    {
      std::lock_guard<std::mutex> lock(backpressure_mutex_);
      if (!writer && counter <= downstream_counter_) return;
      bool previous = writer_paused_ || downstream_paused_;
      if (writer)
        writer_paused_ = paused;
      else {
        downstream_counter_ = counter;
        downstream_paused_ = paused;
      }
      combined = writer_paused_ || downstream_paused_;
      if (combined == previous) return;
      upstream_counter = ++backpressure_counter_;
    }
    // Upstream may re-enter this node; invoke callbacks after releasing the
    // lock.
    if (combined)
      inputs_[0]->PauseProducing(this, upstream_counter);
    else
      inputs_[0]->ResumeProducing(this, upstream_counter);
  }

 protected:
  std::string ToStringExtra(int indent = 0) const override {
    return "base_dir=" + write_options_.base_dir;
  }

 private:
  std::unique_ptr<acier::dataset::internal::DatasetWriter> dataset_writer_;
  FileSystemDatasetWriteOptions write_options_;
  uint64_t max_rows_queued_;
  std::mutex start_mutex_;
  std::atomic<bool> stopped_{false};
  std::mutex backpressure_mutex_;
  int32_t backpressure_counter_ = 0;
  int32_t downstream_counter_ = 0;
  bool writer_paused_ = false;
  bool downstream_paused_ = false;
  std::unique_ptr<acero::util::SerialSequencingQueue> sequencer_{nullptr};
};

}  // namespace
}  // namespace acier::dataset

namespace acier::internal {
arrow::Status RegisterDatasetNodes(arrow::acero::ExecFactoryRegistry* registry) {
  ARROW_RETURN_NOT_OK(registry->AddFactory("acier_write", dataset::WriteNode::Make));
  return registry->AddFactory("acier_tee", dataset::TeeNode::Make);
}
}  // namespace acier::internal

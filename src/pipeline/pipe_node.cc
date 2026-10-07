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

// SPDX-License-Identifier: Apache-2.0
// Derived from Apache Arrow pipe_node.h/.cc.
#include <acier/pipe_node.h>
#include <arrow/acero/query_context.h>
#include <arrow/acero/util.h>
#include <arrow/type.h>
#include <atomic>
#include <utility>

namespace acier {
namespace {
using arrow::Result;
using arrow::Schema;
using arrow::Status;
using arrow::acero::ExecNode;
using arrow::acero::ExecNodeOptions;
using arrow::acero::ExecPlan;
using arrow::acero::ValidateExecNodeInputs;
using arrow::compute::ExecBatch;
using arrow::compute::Ordering;

class PipeSourceNode final : public PipeSource, public ExecNode {
 public:
  PipeSourceNode(ExecPlan* plan, std::shared_ptr<Schema> schema, std::string name,
                 Ordering ordering)
      : ExecNode(plan, {}, {}, std::move(schema)),
        name_(std::move(name)),
        ordering_(std::move(ordering)) {}
  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    ARROW_RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 0, kKindName));
    const auto* cast = dynamic_cast<const PipeSourceNodeOptions*>(&options);
    if (!cast)
      return Status::TypeError("acier_pipe_source requires PipeSourceNodeOptions");
    if (!cast->output_schema) return Status::Invalid("Pipe source requires a schema");
    return plan->EmplaceNode<PipeSourceNode>(plan, cast->output_schema, cast->pipe_name,
                                             cast->ordering);
  }
  Status InputReceived(ExecNode*, ExecBatch) override {
    return Status::Invalid("Pipe source has no direct input");
  }
  Status InputFinished(ExecNode*, int) override {
    return Status::Invalid("Pipe source has no direct input");
  }
  Status HandleInputReceived(ExecBatch batch) override {
    return output_->InputReceived(this, std::move(batch));
  }
  Status HandleInputFinished(int count) override {
    return output_->InputFinished(this, count);
  }
  const Ordering& ordering() const override { return ordering_; }
  Status StartProducing() override { return PipeSource::Validate(ordering_); }
  void PauseProducing(ExecNode*, int32_t counter) override { Pause(counter); }
  void ResumeProducing(ExecNode*, int32_t counter) override { Resume(counter); }
  Status StopProducingImpl() override { return PipeSource::StopProducing(); }
  const char* kind_name() const override { return kKindName; }
  static const char kKindName[];
  const std::string name_;

 private:
  Ordering ordering_;
};
const char PipeSourceNode::kKindName[] = "AcierPipeSourceNode";

class PipeSinkControl final : public arrow::acero::BackpressureControl {
 public:
  PipeSinkControl(ExecNode* input, ExecNode* output) : input_(input), output_(output) {}
  void Pause() override { input_->PauseProducing(output_, ++counter_); }
  void Resume() override { input_->ResumeProducing(output_, ++counter_); }

 private:
  ExecNode* input_;
  ExecNode* output_;
  std::atomic<int32_t> counter_{0};
};

class PipeSinkNode : public ExecNode {
 public:
  PipeSinkNode(ExecPlan* plan, std::vector<ExecNode*> inputs, std::string name,
               bool pause_on_any, bool stop_on_any)
      : ExecNode(plan, inputs, {name}, nullptr) {
    pipe_ = std::make_shared<Pipe>(
        plan, std::move(name), std::make_unique<PipeSinkControl>(inputs[0], this),
        [this] {
          pipe_->Stop();
          return ExecNode::StopProducing();
        },
        inputs[0]->ordering(), pause_on_any, stop_on_any);
  }
  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    ARROW_RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "AcierPipeSinkNode"));
    const auto* cast = dynamic_cast<const PipeSinkNodeOptions*>(&options);
    if (!cast) return Status::TypeError("acier_pipe_sink requires PipeSinkNodeOptions");
    return plan->EmplaceNode<PipeSinkNode>(plan, std::move(inputs), cast->pipe_name,
                                           cast->pause_on_any, cast->stop_on_any);
  }
  const char* kind_name() const override { return "AcierPipeSinkNode"; }
  Status InputReceived(ExecNode*, ExecBatch batch) override {
    return pipe_->InputReceived(std::move(batch));
  }
  Status InputFinished(ExecNode*, int count) override {
    return pipe_->InputFinished(count);
  }
  Status Init() override { return pipe_->Init(inputs_[0]->output_schema(), true); }
  Status StartProducing() override { return Status::OK(); }
  void PauseProducing(ExecNode*, int32_t) override {}
  void ResumeProducing(ExecNode*, int32_t) override {}

 protected:
  Status StopProducingImpl() override {
    pipe_->Stop();
    return Status::OK();
  }
  std::shared_ptr<Pipe> pipe_;
};

class PipeTeeNode final : public PipeSource, public PipeSinkNode {
 public:
  PipeTeeNode(ExecPlan* plan, std::vector<ExecNode*> inputs, std::string name,
              bool pause_on_any, bool stop_on_any)
      : PipeSinkNode(plan, inputs, std::move(name), pause_on_any, stop_on_any) {
    output_schema_ = inputs[0]->output_schema();
  }
  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    ARROW_RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "AcierPipeTeeNode"));
    const auto* cast = dynamic_cast<const PipeSinkNodeOptions*>(&options);
    if (!cast) return Status::TypeError("acier_pipe_tee requires PipeSinkNodeOptions");
    return plan->EmplaceNode<PipeTeeNode>(plan, std::move(inputs), cast->pipe_name,
                                          cast->pause_on_any, cast->stop_on_any);
  }
  Status Init() override {
    ARROW_RETURN_NOT_OK(PipeSinkNode::pipe_->addSyncSource(this));
    return PipeSinkNode::Init();
  }
  const char* kind_name() const override { return "AcierPipeTeeNode"; }
  const Ordering& ordering() const override { return inputs_[0]->ordering(); }
  Status StartProducing() override { return PipeSource::Validate(ordering()); }
  void PauseProducing(ExecNode*, int32_t counter) override { Pause(counter); }
  void ResumeProducing(ExecNode*, int32_t counter) override { Resume(counter); }
  // A stopped downstream branch must not stop other pipe consumers.
  Status StopProducing() override { return PipeSource::StopProducing(); }
  Status HandleInputReceived(ExecBatch batch) override {
    return output_->InputReceived(this, std::move(batch));
  }
  Status HandleInputFinished(int count) override {
    return output_->InputFinished(this, count);
  }
};
}  // namespace

arrow::Status PipeSource::Initialize(Pipe* pipe) {
  if (pipe_)
    return arrow::Status::Invalid("Pipe ", pipe->PipeName(), " has multiple sinks");
  pipe_ = pipe;
  backpressure_source_.AddController(pipe);
  return arrow::Status::OK();
}
void PipeSource::DeliverBackpressure() {
  std::unique_lock<std::mutex> lock(mutex_);
  if (delivering_) return;
  delivering_ = true;
  while (delivered_paused_ != (paused_ && !stopped_)) {
    delivered_paused_ = paused_ && !stopped_;
    const bool pause = delivered_paused_;
    lock.unlock();
    if (pause)
      backpressure_source_.Pause();
    else
      backpressure_source_.Resume();
    lock.lock();
  }
  delivering_ = false;
}
void PipeSource::Pause(int32_t counter) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (counter <= backpressure_counter_ || stopped_) return;
    backpressure_counter_ = counter;
    paused_ = true;
  }
  DeliverBackpressure();
}
void PipeSource::Resume(int32_t counter) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (counter <= backpressure_counter_) return;
    backpressure_counter_ = counter;
    paused_ = false;
  }
  DeliverBackpressure();
}
arrow::Status PipeSource::StopProducing() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopped_) return arrow::Status::OK();
    stopped_ = true;
    paused_ = false;
  }
  DeliverBackpressure();
  return pipe_ ? pipe_->StopProducing(this) : arrow::Status::OK();
}
arrow::Status PipeSource::DeliverBatch(arrow::compute::ExecBatch batch) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopped_) return arrow::Status::OK();
  }
  return HandleInputReceived(std::move(batch));
}
arrow::Status PipeSource::Validate(const arrow::compute::Ordering& ordering) {
  if (!pipe_) return arrow::Status::Invalid("Pipe source has no matching sink");
  if (!ordering.IsSuborderOf(pipe_->ordering()) &&
      !(ordering.is_implicit() && !pipe_->ordering().is_unordered())) {
    return arrow::Status::Invalid("Pipe source ordering ", ordering.ToString(),
                                  " is incompatible with sink ordering ",
                                  pipe_->ordering().ToString());
  }
  return arrow::Status::OK();
}
Pipe::Pipe(arrow::acero::ExecPlan* plan, std::string name,
           std::unique_ptr<arrow::acero::BackpressureControl> control,
           std::function<arrow::Status()> stop_producing,
           arrow::compute::Ordering ordering, bool pause_on_any, bool stop_on_any)
    : BackpressureCombiner(std::move(control), pause_on_any),
      plan_(plan),
      ordering_(std::move(ordering)),
      pipe_name_(std::move(name)),
      stop_producing_(std::move(stop_producing)),
      stop_on_any_(stop_on_any) {}
const arrow::compute::Ordering& Pipe::ordering() const { return ordering_; }
arrow::Status Pipe::StopProducing(PipeSource* source) {
  bool stop = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    bool& stopped = stopped_sources_[source];
    if (stopped) return arrow::Status::OK();
    stopped = true;
    ++stopped_count_;
    stop = stop_on_any_ ? stopped_count_ == 1 : stopped_count_ == CountSources();
  }
  return stop ? stop_producing_() : arrow::Status::OK();
}
arrow::Status Pipe::InputReceived(arrow::compute::ExecBatch batch) {
  for (auto* source : async_nodes_) {
    plan_->query_context()->ScheduleTask(
        [source, batch]() mutable { return source->DeliverBatch(std::move(batch)); },
        "AcierPipe::InputReceived");
  }
  return sync_node_ ? sync_node_->DeliverBatch(std::move(batch)) : arrow::Status::OK();
}
arrow::Status Pipe::InputFinished(int count) {
  for (auto* source : async_nodes_) {
    plan_->query_context()->ScheduleTask(
        [source, count] { return source->HandleInputFinished(count); },
        "AcierPipe::InputFinished");
  }
  return sync_node_ ? sync_node_->HandleInputFinished(count) : arrow::Status::OK();
}
arrow::Status Pipe::addAsyncSource(PipeSource* source, bool may_be_sync) {
  ARROW_RETURN_NOT_OK(source->Initialize(this));
  if (may_be_sync && !sync_node_)
    sync_node_ = source;
  else
    async_nodes_.push_back(source);
  return arrow::Status::OK();
}
arrow::Status Pipe::addSyncSource(PipeSource* source) {
  if (sync_node_)
    return arrow::Status::Invalid("Pipe already has a synchronous consumer");
  ARROW_RETURN_NOT_OK(source->Initialize(this));
  sync_node_ = source;
  return arrow::Status::OK();
}
arrow::Status Pipe::Init(const std::shared_ptr<arrow::Schema> schema, bool may_be_sync) {
  if (!schema) return arrow::Status::Invalid("Pipe requires a schema");
  for (auto* node : plan_->nodes()) {
    if (node->kind_name() != PipeSourceNode::kKindName) continue;
    auto* source = static_cast<PipeSourceNode*>(node);
    if (source->name_ != pipe_name_) continue;
    if (!schema->Equals(node->output_schema())) {
      return arrow::Status::Invalid("Pipe schema does not match for ", pipe_name_);
    }
    ARROW_RETURN_NOT_OK(addAsyncSource(source, may_be_sync));
  }
  return arrow::Status::OK();
}
bool Pipe::HasSources() const { return CountSources() != 0; }
size_t Pipe::CountSources() const { return async_nodes_.size() + (sync_node_ ? 1 : 0); }

namespace internal {
arrow::Status RegisterPipeNodes(arrow::acero::ExecFactoryRegistry* registry) {
  ARROW_RETURN_NOT_OK(registry->AddFactory("acier_pipe_source", PipeSourceNode::Make));
  ARROW_RETURN_NOT_OK(registry->AddFactory("acier_pipe_sink", PipeSinkNode::Make));
  return registry->AddFactory("acier_pipe_tee", PipeTeeNode::Make);
}
}  // namespace internal
}  // namespace acier

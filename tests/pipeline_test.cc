// SPDX-License-Identifier: Apache-2.0
#include <acier/accumulation_queue.h>
#include <acier/backpressure.h>
#include <acier/concurrent_queue.h>
#include <acier/initialize.h>
#include <acier/pipe_node.h>
#include <arrow/acero/api.h>
#include <arrow/acero/map_node.h>
#include <arrow/acero/query_context.h>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/util/thread_pool.h>
#include <algorithm>
#include <atomic>
#include <functional>
#include <iostream>
#include <mutex>
#include <numeric>
#include <type_traits>

namespace ac = arrow::acero;
using arrow::Result;
using arrow::Status;
using arrow::compute::ExecBatch;
using arrow::compute::Ordering;
static_assert(std::is_same_v<acier::BackpressureHandler, ac::BackpressureHandler>);

Status Require(bool condition, const char* message) {
  return condition ? Status::OK() : Status::Invalid(message);
}

Result<std::shared_ptr<arrow::Table>> MakeTable(int64_t count, int64_t segment_size) {
  arrow::Int64Builder segments, keys, values;
  for (int64_t i = 0; i < count; ++i) {
    ARROW_RETURN_NOT_OK(segments.Append(i / segment_size));
    ARROW_RETURN_NOT_OK(keys.Append(i));
    ARROW_RETURN_NOT_OK(values.Append(i));
  }
  ARROW_ASSIGN_OR_RAISE(auto s, segments.Finish());
  ARROW_ASSIGN_OR_RAISE(auto k, keys.Finish());
  ARROW_ASSIGN_OR_RAISE(auto v, values.Finish());
  return arrow::Table::Make(arrow::schema({arrow::field("segment", arrow::int64(), false),
                                           arrow::field("key", arrow::int64(), false),
                                           arrow::field("value", arrow::int64(), false)}),
                            {s, k, v});
}

struct Control final : ac::BackpressureControl {
  int pauses = 0, resumes = 0;
  std::function<void()> on_pause;
  void Pause() override {
    ++pauses;
    if (on_pause) on_pause();
  }
  void Resume() override { ++resumes; }
};

Status TestBackpressureAndQueue() {
  for (bool any : {false, true}) {
    auto control = std::make_unique<Control>();
    auto* counters = control.get();
    acier::BackpressureCombiner combiner(std::move(control), any);
    acier::BackpressureCombiner::Source a(&combiner), b(&combiner);
    a.Pause();
    ARROW_RETURN_NOT_OK(
        Require(counters->pauses == (any ? 1 : 0), "any/all first pause"));
    b.Pause();
    ARROW_RETURN_NOT_OK(Require(counters->pauses == 1, "any/all combined pause"));
    a.Resume();
    ARROW_RETURN_NOT_OK(
        Require(counters->resumes == (any ? 0 : 1), "any/all first resume"));
    b.Resume();
    ARROW_RETURN_NOT_OK(Require(counters->resumes == 1, "any/all combined resume"));
    a.Pause();
    b.Pause();
    combiner.Stop();
    ARROW_RETURN_NOT_OK(Require(counters->pauses == 2 && counters->resumes == 2,
                                "stop must release pause"));
    a.Resume();
    a.Pause();
    ARROW_RETURN_NOT_OK(Require(counters->pauses == 2, "pause after stop"));
  }
  // The callback reenters the combiner synchronously.
  auto control = std::make_unique<Control>();
  auto* counters = control.get();
  acier::BackpressureCombiner combiner(std::move(control));
  acier::BackpressureCombiner::Source source(&combiner);
  counters->on_pause = [&] { source.Resume(); };
  source.Pause();
  ARROW_RETURN_NOT_OK(Require(counters->pauses == 1 && counters->resumes == 1,
                              "reentrant backpressure transition"));

  auto queue_control = std::make_unique<Control>();
  auto* queue_counts = queue_control.get();
  ARROW_ASSIGN_OR_RAISE(auto handler,
                        ac::BackpressureHandler::Make(1, 3, std::move(queue_control)));
  acier::BackpressureConcurrentQueue<int> queue(std::move(handler));
  queue.Push(1);
  queue.Push(2);
  queue.Push(3);
  ARROW_RETURN_NOT_OK(Require(queue_counts->pauses == 1, "queue high watermark"));
  ARROW_RETURN_NOT_OK(Require(queue.TryPop() == 1 && queue.TryPop() == 2, "queue FIFO"));
  ARROW_RETURN_NOT_OK(Require(queue_counts->resumes == 1, "queue low watermark"));
  queue.Push(4);
  queue.Push(5);
  queue.ForceShutdown();
  queue.Push(6);
  ARROW_RETURN_NOT_OK(Require(queue.Empty() && queue_counts->resumes == 2,
                              "queue shutdown clears pause"));

  auto reentrant_control = std::make_unique<Control>();
  auto* reentrant_counts = reentrant_control.get();
  ARROW_ASSIGN_OR_RAISE(auto reentrant_handler, ac::BackpressureHandler::Make(
                                                    0, 1, std::move(reentrant_control)));
  acier::BackpressureConcurrentQueue<int> reentrant_queue(std::move(reentrant_handler));
  reentrant_counts->on_pause = [&] { reentrant_queue.Clear(); };
  reentrant_queue.Push(1);
  return Require(reentrant_queue.Empty() && reentrant_counts->pauses == 1 &&
                     reentrant_counts->resumes == 1,
                 "queue backpressure callback cannot reenter safely");
}

class Endpoint final : public acier::PipeSource {
 public:
  Status HandleInputReceived(ExecBatch) override { return Status::OK(); }
  Status HandleInputFinished(int) override { return Status::OK(); }
};

Status TestPipeStopPolicy() {
  for (bool any : {false, true}) {
    auto control = std::make_unique<Control>();
    auto* counts = control.get();
    int stops = 0;
    acier::Pipe pipe(
        nullptr, "policy", std::move(control),
        [&] {
          ++stops;
          return Status::OK();
        },
        Ordering::Implicit(), true, any);
    Endpoint a, b;
    ARROW_RETURN_NOT_OK(pipe.addSyncSource(&a));
    ARROW_RETURN_NOT_OK(pipe.addAsyncSource(&b));
    a.Pause(1);
    a.Pause(1);
    ARROW_RETURN_NOT_OK(a.StopProducing());
    a.Pause(2);
    ARROW_RETURN_NOT_OK(Require(stops == (any ? 1 : 0), "pipe first stopped consumer"));
    ARROW_RETURN_NOT_OK(Require(counts->pauses == 1 && counts->resumes == 1,
                                "stopped pipe consumer releases pause"));
    ARROW_RETURN_NOT_OK(a.StopProducing());
    ARROW_RETURN_NOT_OK(b.StopProducing());
    ARROW_RETURN_NOT_OK(Require(stops == 1, "pipe stop policy must notify once"));
  }
  return Status::OK();
}

struct QueueOptions : ac::ExecNodeOptions {
  QueueOptions(bool io, bool error = false) : io(io), error(error) {}
  bool io, error;
};

class QueueNode final : public ac::MapNode,
                        public acier::util::SerialSequencingQueue::Processor {
 public:
  QueueNode(ac::ExecPlan* plan, std::vector<ac::ExecNode*> inputs, QueueOptions options)
      : MapNode(plan, inputs, inputs[0]->output_schema()), options_(options) {}
  static Result<ac::ExecNode*> Make(ac::ExecPlan* plan, std::vector<ac::ExecNode*> inputs,
                                    const ac::ExecNodeOptions& options) {
    if (inputs.size() != 1) return Status::Invalid("queue test requires one input");
    return plan->EmplaceNode<QueueNode>(plan, std::move(inputs),
                                        dynamic_cast<const QueueOptions&>(options));
  }
  Status Init() override {
    auto control =
        std::make_unique<acier::BackpressureController>(inputs_[0], this, counter_);
    ARROW_ASSIGN_OR_RAISE(auto handler,
                          ac::BackpressureHandler::Make(2, 4, std::move(control)));
    processor_ =
        Processor::MakeBackpressureWrapper(this, std::move(handler), plan_, options_.io);
    sequencer_ = acier::util::SerialSequencingQueue::Make(processor_.get());
    return Status::OK();
  }
  const char* kind_name() const override { return "PipelineTestQueueNode"; }
  Status InputReceived(ac::ExecNode*, ExecBatch batch) override {
    return sequencer_->InsertBatch(std::move(batch));
  }
  Status Process(ExecBatch batch) override {
    auto* executor = options_.io ? plan_->query_context()->io_context()->executor()
                                 : plan_->query_context()->executor();
    ARROW_RETURN_NOT_OK(
        Require(executor->IsCurrentExecutor(), "wrong processing executor"));
    ARROW_RETURN_NOT_OK(
        Require(batch.index == next_++, "sequencer delivered wrong index"));
    if (options_.error && batch.index == 3) {
      return Status::Invalid("injected pipeline processor error");
    }
    return MapNode::InputReceived(inputs_[0], std::move(batch));
  }
  Result<ExecBatch> ProcessBatch(ExecBatch batch) override { return batch; }

 protected:
  Status StopProducingImpl() override {
    if (processor_) processor_->Stop();
    return Status::OK();
  }

 private:
  QueueOptions options_;
  int64_t next_ = 0;
  std::atomic<int32_t> counter_{0};
  std::unique_ptr<Processor> processor_;
  std::unique_ptr<acier::util::SerialSequencingQueue> sequencer_;
};

Status TestSequencing(bool threads) {
  ARROW_ASSIGN_OR_RAISE(auto table, MakeTable(513, 37));
  for (bool io : {false, true}) {
    auto declaration = ac::Declaration::Sequence({
        {"table_source", ac::TableSourceNodeOptions{table, 1}},
        {"pipeline_test_queue", QueueOptions{io}},
    });
    ARROW_ASSIGN_OR_RAISE(auto actual,
                          ac::DeclarationToTable(std::move(declaration), threads));
    ARROW_RETURN_NOT_OK(
        Require(actual->Equals(*table), "sequencer changed ordered data"));
  }
  auto failing = ac::Declaration::Sequence({
      {"table_source", ac::TableSourceNodeOptions{table, 1}},
      {"pipeline_test_queue", QueueOptions{false, true}},
  });
  auto result = ac::DeclarationToTable(std::move(failing), threads);
  return Require(!result.ok() && result.status().message().find("injected pipeline") !=
                                     std::string::npos,
                 "processor error was not propagated");
}

Status TestAggregate(bool threads) {
  constexpr int64_t rows = 65541;
  ARROW_ASSIGN_OR_RAISE(auto table, MakeTable(rows, rows));
  auto grouped = ac::Declaration::Sequence({
      {"table_source", ac::TableSourceNodeOptions{table, 127}},
      {"acier_aggregate",
       ac::AggregateNodeOptions{
           {arrow::compute::Aggregate{"hash_sum", nullptr, "value", "sum"}},
           {"key"},
           {"segment"}}},
  });
  ac::QueryOptions options;
  options.use_threads = threads;
  options.sequence_output = false;
  ARROW_ASSIGN_OR_RAISE(auto actual,
                        ac::DeclarationToExecBatches(std::move(grouped), options));
  ARROW_RETURN_NOT_OK(
      Require(actual.batches.size() >= 3, "test requires multi-batch output"));
  std::sort(actual.batches.begin(), actual.batches.end(),
            [](const ExecBatch& a, const ExecBatch& b) { return a.index < b.index; });
  int64_t row = 0;
  for (size_t i = 0; i < actual.batches.size(); ++i) {
    const auto& batch = actual.batches[i];
    ARROW_RETURN_NOT_OK(Require(batch.index == static_cast<int64_t>(i),
                                "aggregate indices must be contiguous from zero"));
    ARROW_ASSIGN_OR_RAISE(auto record, batch.ToRecordBatch(actual.schema));
    ARROW_RETURN_NOT_OK(record->ValidateFull());
    const auto& keys = static_cast<const arrow::Int64Array&>(*record->column(1));
    const auto& sums = static_cast<const arrow::Int64Array&>(*record->column(2));
    for (int64_t j = 0; j < record->num_rows(); ++j, ++row) {
      ARROW_RETURN_NOT_OK(Require(keys.Value(j) == row && sums.Value(j) == row,
                                  "ordered grouping changed values or group order"));
    }
  }
  ARROW_RETURN_NOT_OK(Require(row == rows, "grouped aggregation lost rows"));

  ARROW_ASSIGN_OR_RAISE(auto segments, MakeTable(2049, 113));
  auto scalar = ac::Declaration::Sequence({
      {"table_source", ac::TableSourceNodeOptions{segments, 3}},
      {"acier_aggregate",
       ac::AggregateNodeOptions{
           {arrow::compute::Aggregate{"first", nullptr, "value", "first"},
            arrow::compute::Aggregate{"last", nullptr, "value", "last"}},
           {},
           {"segment"}}},
  });
  ARROW_ASSIGN_OR_RAISE(auto output, ac::DeclarationToTable(std::move(scalar), threads));
  ARROW_RETURN_NOT_OK(output->ValidateFull());
  ARROW_RETURN_NOT_OK(Require(output->num_rows() == 19, "incorrect segment count"));
  for (int64_t i = 0; i < output->num_rows(); ++i) {
    ARROW_ASSIGN_OR_RAISE(auto first, output->column(1)->GetScalar(i));
    ARROW_ASSIGN_OR_RAISE(auto last, output->column(2)->GetScalar(i));
    ARROW_RETURN_NOT_OK(Require(first->Equals(arrow::Int64Scalar{i * 113}),
                                "ordered first aggregate changed"));
    ARROW_RETURN_NOT_OK(Require(
        last->Equals(arrow::Int64Scalar{std::min<int64_t>(2048, (i + 1) * 113 - 1)}),
        "ordered last aggregate changed"));
  }
  auto unsegmented = ac::Declaration::Sequence({
      {"table_source", ac::TableSourceNodeOptions{table, 127}},
      {"acier_aggregate", ac::AggregateNodeOptions{{arrow::compute::Aggregate{
                              "sum", nullptr, "value", "sum"}}}},
  });
  ARROW_ASSIGN_OR_RAISE(auto sum_output,
                        ac::DeclarationToTable(std::move(unsegmented), threads));
  ARROW_RETURN_NOT_OK(Require(sum_output->num_rows() == 1, "scalar result row count"));
  ARROW_ASSIGN_OR_RAISE(auto sum, sum_output->column(0)->GetScalar(0));
  ARROW_RETURN_NOT_OK(Require(sum->Equals(arrow::Int64Scalar{rows * (rows - 1) / 2}),
                              "unordered scalar aggregate changed values"));

  ARROW_ASSIGN_OR_RAISE(auto empty, MakeTable(0, 1));
  for (bool segmented : {false, true}) {
    auto empty_aggregate = ac::Declaration::Sequence({
        {"table_source", ac::TableSourceNodeOptions{empty, 3}},
        {"acier_aggregate",
         ac::AggregateNodeOptions{
             {arrow::compute::Aggregate{"sum", nullptr, "value", "sum"}},
             {},
             segmented ? std::vector<arrow::FieldRef>{"segment"}
                       : std::vector<arrow::FieldRef>{}}},
    });
    ARROW_ASSIGN_OR_RAISE(auto empty_output,
                          ac::DeclarationToTable(std::move(empty_aggregate), threads));
    ARROW_RETURN_NOT_OK(empty_output->ValidateFull());
    ARROW_RETURN_NOT_OK(Require(empty_output->num_rows() == (segmented ? 0 : 1),
                                "incorrect empty aggregate output count"));
  }
  return Status::OK();
}

Status TestPipeTee(bool threads) {
  ARROW_ASSIGN_OR_RAISE(auto table, MakeTable(97, 97));
  for (bool stop_branch : {false, true}) {
    auto primary = ac::Declaration::Sequence({
        {"table_source", ac::TableSourceNodeOptions{table, 1}},
        {"acier_pipe_tee", acier::PipeSinkNodeOptions{"broadcast"}},
    });
    if (stop_branch) {
      primary =
          ac::Declaration{"fetch", {std::move(primary)}, ac::FetchNodeOptions{0, 1}};
    }
    ac::Declaration auxiliary{
        "acier_pipe_source",
        acier::PipeSourceNodeOptions{"broadcast", table->schema(), Ordering::Implicit()}};
    ac::Declaration merged{
        "union", {std::move(primary), std::move(auxiliary)}, ac::ExecNodeOptions{}};
    ARROW_ASSIGN_OR_RAISE(auto output,
                          ac::DeclarationToTable(std::move(merged), threads));
    ARROW_RETURN_NOT_OK(output->ValidateFull());
    ARROW_RETURN_NOT_OK(Require(output->num_rows() == (stop_branch ? 98 : 194),
                                "pipe tee lost a consumer or stopped siblings"));
  }
  return Status::OK();
}

class CountingSink final : public ac::SinkNodeConsumer {
 public:
  Status Init(const std::shared_ptr<arrow::Schema>&, ac::BackpressureControl* control,
              ac::ExecPlan* plan) override {
    control_ = control;
    plan_ = plan;
    return Status::OK();
  }
  Status Consume(ExecBatch batch) override {
    rows.fetch_add(batch.length);
    if (!paused.exchange(true)) {
      control_->Pause();
      plan_->query_context()->ScheduleTask(
          [this] {
            control_->Resume();
            return Status::OK();
          },
          "PipelineTest::Resume");
    }
    return Status::OK();
  }
  arrow::Future<> Finish() override {
    finished = true;
    return arrow::Future<>::MakeFinished();
  }
  std::atomic<int64_t> rows{0};
  std::atomic<bool> paused{false}, finished{false};

 private:
  ac::BackpressureControl* control_ = nullptr;
  ac::ExecPlan* plan_ = nullptr;
};

Status TestPipeSink() {
  ARROW_ASSIGN_OR_RAISE(auto table, MakeTable(1025, 1025));
  ARROW_ASSIGN_OR_RAISE(auto plan, ac::ExecPlan::Make());
  ARROW_ASSIGN_OR_RAISE(auto source,
                        ac::MakeExecNode("table_source", plan.get(), {},
                                         ac::TableSourceNodeOptions{table, 1}));
  ARROW_ASSIGN_OR_RAISE(auto sink,
                        ac::MakeExecNode("acier_pipe_sink", plan.get(), {source},
                                         acier::PipeSinkNodeOptions{"sink"}));
  (void)sink;
  std::vector<std::shared_ptr<CountingSink>> consumers;
  for (int i = 0; i < 2; ++i) {
    ARROW_ASSIGN_OR_RAISE(
        auto receiver,
        ac::MakeExecNode(
            "acier_pipe_source", plan.get(), {},
            acier::PipeSourceNodeOptions{"sink", table->schema(), Ordering::Implicit()}));
    auto consumer = std::make_shared<CountingSink>();
    ARROW_ASSIGN_OR_RAISE(
        auto consumer_node,
        ac::MakeExecNode("consuming_sink", plan.get(), {receiver},
                         ac::ConsumingSinkNodeOptions{consumer, {}, true}));
    (void)consumer_node;
    consumers.push_back(consumer);
  }
  ARROW_RETURN_NOT_OK(plan->Validate());
  plan->StartProducing();
  if (!plan->finished().Wait(10)) {
    plan->StopProducing();
    plan->finished().Wait();
    return Status::Invalid("pipe sink plan timed out");
  }
  ARROW_RETURN_NOT_OK(plan->finished().status());
  for (const auto& consumer : consumers) {
    ARROW_RETURN_NOT_OK(
        Require(consumer->rows == 1025 && consumer->finished && consumer->paused,
                "pipe sink/backpressure lost data"));
  }
  return Status::OK();
}

Status Run() {
  ARROW_RETURN_NOT_OK(acier::Initialize());
  ARROW_RETURN_NOT_OK(ac::default_exec_factory_registry()->AddFactory(
      "pipeline_test_queue", QueueNode::Make));
  ARROW_RETURN_NOT_OK(TestBackpressureAndQueue());
  ARROW_RETURN_NOT_OK(TestPipeStopPolicy());
  for (bool threads : {false, true}) {
    ARROW_RETURN_NOT_OK(TestSequencing(threads));
    ARROW_RETURN_NOT_OK(TestAggregate(threads));
    ARROW_RETURN_NOT_OK(TestPipeTee(threads));
  }
  return TestPipeSink();
}
int main() {
  auto status = Run();
  if (!status.ok()) {
    std::cerr << status << '\n';
    return 1;
  }
  std::cout << "Pipeline integration checks passed\n";
  return 0;
}

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
// Derived from Apache Arrow accumulation_queue.h/.cc.
#include <acier/accumulation_queue.h>
#include <arrow/acero/query_context.h>
#include <mutex>
#include <queue>

namespace acier::util {
namespace {
using arrow::Status;
using arrow::compute::ExecBatch;

class Queue final : public SerialSequencingQueue {
 public:
  explicit Queue(Processor* processor)
      : queue_(arrow::acero::util::SerialSequencingQueue::Make(processor)) {}
  Status InsertBatch(ExecBatch batch) override {
    if (batch.index < 0) {
      return Status::Invalid("Acier sequencing requires nonnegative batch indices");
    }
    return queue_->InsertBatch(std::move(batch));
  }

 private:
  std::unique_ptr<arrow::acero::util::SerialSequencingQueue> queue_;
};

class BackpressureProcessor final : public SerialSequencingQueue::Processor {
 public:
  BackpressureProcessor(Processor* processor, arrow::acero::BackpressureHandler handler,
                        arrow::acero::ExecPlan* plan, bool requires_io)
      : processor_(processor),
        handler_(std::move(handler)),
        plan_(plan),
        requires_io_(requires_io) {}

  Status Process(ExecBatch batch) override {
    bool schedule = false;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!failure_.ok()) return failure_;
      if (stopped_) return Status::OK();
      queue_.push(std::move(batch));
      if (!running_) {
        running_ = true;
        schedule = true;
      }
    }
    ReportSize();
    if (schedule) {
      auto task = [this] { return Drain(); };
      if (requires_io_) {
        plan_->query_context()->ScheduleIOTask(std::move(task),
                                               "AcierSequencer::DrainIO");
      } else {
        plan_->query_context()->ScheduleTask(std::move(task), "AcierSequencer::Drain");
      }
    }
    return Status::OK();
  }

  void Stop() override {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      stopped_ = true;
      queue_ = {};
    }
    ReportSize();
  }

 private:
  void ReportSize() {
    std::unique_lock<std::mutex> lock(mutex_);
    if (reporting_) return;
    reporting_ = true;
    while (reported_size_ != queue_.size()) {
      const size_t before = reported_size_;
      const size_t after = queue_.size();
      reported_size_ = after;
      lock.unlock();
      handler_.Handle(before, after);
      lock.lock();
    }
    reporting_ = false;
  }

  Status Drain() {
    for (;;) {
      ExecBatch batch;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.empty()) {
          running_ = false;
          return failure_;
        }
        batch = std::move(queue_.front());
        queue_.pop();
      }
      ReportSize();
      auto status = processor_->Process(std::move(batch));
      if (!status.ok()) {
        {
          std::lock_guard<std::mutex> lock(mutex_);
          failure_ = status;
          queue_ = {};
          running_ = false;
        }
        ReportSize();
        return status;
      }
    }
  }

  Processor* processor_;
  arrow::acero::BackpressureHandler handler_;
  arrow::acero::ExecPlan* plan_;
  const bool requires_io_;
  std::mutex mutex_;
  std::queue<ExecBatch> queue_;
  Status failure_ = Status::OK();
  size_t reported_size_ = 0;
  bool running_ = false;
  bool stopped_ = false;
  bool reporting_ = false;
};
}  // namespace

std::unique_ptr<SerialSequencingQueue> SerialSequencingQueue::Make(Processor* processor) {
  return std::make_unique<Queue>(processor);
}
std::unique_ptr<SerialSequencingQueue::Processor>
SerialSequencingQueue::Processor::MakeBackpressureWrapper(
    Processor* processor, arrow::acero::BackpressureHandler handler,
    arrow::acero::ExecPlan* plan, bool requires_io) {
  return std::make_unique<BackpressureProcessor>(processor, std::move(handler), plan,
                                                 requires_io);
}
}  // namespace acier::util

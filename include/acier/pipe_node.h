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
#pragma once
#include <acier/backpressure.h>
#include <acier/pipeline_options.h>
#include <acier/visibility.h>
#include <functional>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace acier {
class Pipe;

/// A consumer endpoint attached to a Pipe. An endpoint has at most one producer.
/// Pipe and endpoint lifetimes must cover all tasks in the associated ExecPlan.
class ACIER_EXPORT PipeSource {
 public:
  PipeSource() = default;
  virtual ~PipeSource() = default;
  void Pause(int32_t counter);
  void Resume(int32_t counter);
  arrow::Status StopProducing();
  arrow::Status Validate(const arrow::compute::Ordering& ordering);

 private:
  friend class Pipe;
  arrow::Status Initialize(Pipe* pipe);
  void DeliverBackpressure();
  arrow::Status DeliverBatch(arrow::compute::ExecBatch batch);
  virtual arrow::Status HandleInputReceived(arrow::compute::ExecBatch batch) = 0;
  virtual arrow::Status HandleInputFinished(int total_batches) = 0;

  Pipe* pipe_ = nullptr;
  BackpressureCombiner::Source backpressure_source_;
  std::mutex mutex_;
  int32_t backpressure_counter_ = 0;
  bool stopped_ = false;
  bool paused_ = false;
  bool delivered_paused_ = false;
  bool delivering_ = false;
};

/// Broadcast batches to named sources in the same execution plan.
/// One endpoint can receive synchronously; other endpoints receive scheduled tasks.
class ACIER_EXPORT Pipe : public BackpressureCombiner {
 public:
  Pipe(arrow::acero::ExecPlan* plan, std::string pipe_name,
       std::unique_ptr<arrow::acero::BackpressureControl> control,
       std::function<arrow::Status()> stop_producing,
       arrow::compute::Ordering ordering = arrow::compute::Ordering::Unordered(),
       bool pause_on_any = true, bool stop_on_any = false);
  const arrow::compute::Ordering& ordering() const;
  /// Connect matching named sources in the execution plan before delivering batches.
  arrow::Status Init(const std::shared_ptr<arrow::Schema> schema,
                     bool may_be_sync = false);
  arrow::Status addAsyncSource(PipeSource* source, bool may_be_sync = false);
  arrow::Status addSyncSource(PipeSource* source);
  arrow::Status InputReceived(arrow::compute::ExecBatch batch);
  arrow::Status InputFinished(int total_batches);
  bool HasSources() const;
  size_t CountSources() const;
  std::string PipeName() const { return pipe_name_; }

 private:
  friend class PipeSource;
  arrow::Status StopProducing(PipeSource* source);
  arrow::acero::ExecPlan* plan_;
  arrow::compute::Ordering ordering_;
  std::string pipe_name_;
  std::vector<PipeSource*> async_nodes_;
  PipeSource* sync_node_ = nullptr;
  std::unordered_map<PipeSource*, bool> stopped_sources_;
  std::mutex mutex_;
  size_t stopped_count_ = 0;
  std::function<arrow::Status()> stop_producing_;
  const bool stop_on_any_;
};
}  // namespace acier

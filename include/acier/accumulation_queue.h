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
#pragma once
#include <acier/type_fwd.h>
#include <acier/visibility.h>
#include <arrow/acero/accumulation_queue.h>
#include <arrow/acero/backpressure_handler.h>

namespace acier::util {
using AccumulationQueue = arrow::acero::util::AccumulationQueue;
using SequencingQueue = arrow::acero::util::SequencingQueue;

/// Delivers indexed batches in sequence, with at most one Process call active.
/// Supply each index exactly once, starting at zero. Batches may arrive out of order;
/// a missing index prevents delivery of all later batches.
class ACIER_EXPORT SerialSequencingQueue {
 public:
  class ACIER_EXPORT Processor
      : public arrow::acero::util::SerialSequencingQueue::Processor {
   public:
    ~Processor() override = default;
    /// Stop accepting queued work. Already-running Process calls may finish.
    virtual void Stop() {}
    /// Schedule processing on the plan's CPU executor, or its I/O executor when
    /// requires_io=true. Watermarks apply to the queue of sequenced batches.
    /// The processor and plan must outlive all scheduled work.
    static std::unique_ptr<Processor> MakeBackpressureWrapper(
        Processor* processor, arrow::acero::BackpressureHandler handler,
        arrow::acero::ExecPlan* plan, bool requires_io = false);
  };
  virtual ~SerialSequencingQueue() = default;
  virtual arrow::Status InsertBatch(arrow::compute::ExecBatch batch) = 0;
  /// The processor must outlive the returned queue.
  static std::unique_ptr<SerialSequencingQueue> Make(Processor* processor);
};
}  // namespace acier::util

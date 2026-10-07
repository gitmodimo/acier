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
// Derived from Apache Arrow backpressure.h/.cc.
#include <acier/backpressure.h>
#include <arrow/acero/exec_plan.h>
#include <algorithm>
#include <cassert>
#include <utility>

namespace acier {

BackpressureController::BackpressureController(arrow::acero::ExecNode* input,
                                               arrow::acero::ExecNode* output,
                                               std::atomic<int32_t>& counter)
    : input_(input), output_(output), counter_(counter) {}
void BackpressureController::Pause() { input_->PauseProducing(output_, ++counter_); }
void BackpressureController::Resume() { input_->ResumeProducing(output_, ++counter_); }

BackpressureCombiner::BackpressureCombiner(
    std::unique_ptr<arrow::acero::BackpressureControl> control, bool pause_on_any)
    : pause_on_any_(pause_on_any), control_(std::move(control)) {
  assert(control_);
}

bool BackpressureCombiner::DesiredPaused() const {
  return !stopped_ && paused_count_ > 0 &&
         (pause_on_any_ || paused_count_ == sources_.size());
}

void BackpressureCombiner::SetPaused(Source* source, bool paused) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto inserted = sources_.emplace(source, false);
    bool& current = inserted.first->second;
    if (current != paused) {
      current = paused;
      if (paused)
        ++paused_count_;
      else
        --paused_count_;
    }
  }
  DeliverTransitions();
}

void BackpressureCombiner::DeliverTransitions() {
  std::unique_lock<std::mutex> lock(mutex_);
  if (delivering_) return;
  delivering_ = true;
  // One caller delivers transitions, including any transitions requested by a
  // callback. No external callback runs under the state lock.
  while (DesiredPaused() != delivered_paused_) {
    delivered_paused_ = DesiredPaused();
    const bool pause = delivered_paused_;
    lock.unlock();
    if (pause)
      control_->Pause();
    else
      control_->Resume();
    lock.lock();
  }
  delivering_ = false;
}

void BackpressureCombiner::Stop() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stopped_ = true;
  }
  DeliverTransitions();
}

BackpressureCombiner::Source::Source(BackpressureCombiner* controller) {
  if (controller) AddController(controller);
}
void BackpressureCombiner::Source::AddController(BackpressureCombiner* controller) {
  assert(controller);
  if (std::find(controllers_.begin(), controllers_.end(), controller) !=
      controllers_.end())
    return;
  controllers_.push_back(controller);
  controller->SetPaused(this, false);
}
void BackpressureCombiner::Source::Pause() {
  for (auto* controller : controllers_) controller->SetPaused(this, true);
}
void BackpressureCombiner::Source::Resume() {
  for (auto* controller : controllers_) controller->SetPaused(this, false);
}

}  // namespace acier

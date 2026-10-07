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
#pragma once
#include <acier/type_fwd.h>
#include <acier/visibility.h>
#include <arrow/acero/options.h>
#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace acier {

class ACIER_EXPORT BackpressureController : public arrow::acero::BackpressureControl {
 public:
  BackpressureController(arrow::acero::ExecNode* input, arrow::acero::ExecNode* output,
                         std::atomic<int32_t>& counter);
  void Pause() override;
  void Resume() override;

 private:
  arrow::acero::ExecNode* input_;
  arrow::acero::ExecNode* output_;
  std::atomic<int32_t>& counter_;
};

template <typename T>
class BackpressureControlWrapper : public arrow::acero::BackpressureControl {
 public:
  explicit BackpressureControlWrapper(T* object) : object_(object) {}
  void Pause() override { object_->Pause(); }
  void Resume() override { object_->Resume(); }

 private:
  T* object_;
};

/// Combines pause requests. By default any paused source pauses the control;
/// with pause_on_any=false all connected sources must be paused.
/// Connections must be established before concurrent use. Controllers must
/// outlive their sources; sources must remain alive while the combiner is used.
class ACIER_EXPORT BackpressureCombiner {
 public:
  explicit BackpressureCombiner(
      std::unique_ptr<arrow::acero::BackpressureControl> control,
      bool pause_on_any = true);

  class ACIER_EXPORT Source : public arrow::acero::BackpressureControl {
   public:
    explicit Source(BackpressureCombiner* controller = nullptr);
    void AddController(BackpressureCombiner* controller);
    void Pause() override;
    void Resume() override;

   private:
    std::vector<BackpressureCombiner*> controllers_;
  };

  /// Release the pause and ignore subsequent pause requests.
  void Stop();

 private:
  friend class Source;
  void SetPaused(Source* source, bool paused);
  void DeliverTransitions();
  bool DesiredPaused() const;
  const bool pause_on_any_;
  std::unique_ptr<arrow::acero::BackpressureControl> control_;
  std::mutex mutex_;
  std::unordered_map<Source*, bool> sources_;
  size_t paused_count_ = 0;
  bool stopped_ = false;
  bool delivered_paused_ = false;
  bool delivering_ = false;
};

}  // namespace acier

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

#pragma once
#include <arrow/acero/backpressure_handler.h>
#include <arrow/result.h>
#include <arrow/util/future.h>
#include <arrow/util/iterator.h>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>

namespace acier {
/// A producer-driven asynchronous queue with at most one waiting consumer.
/// Values queued before Close remain available; subsequent Push calls return
/// false. Destruction of all generator copies expires its Producers. A supplied
/// handler measures queued items, excludes direct delivery, and receives ordered
/// size transitions. Its callbacks may reenter the queue and must not throw.
template <typename T>
class PushGenerator {
  struct State {
    State() = default;
    explicit State(arrow::acero::BackpressureHandler handler)
        : handler(std::move(handler)) {}

    // Adapted from Arrow's PushGenerator. Drain notifications separately so
    // application callbacks never execute while holding the queue mutex.
    bool Changed(size_t before, size_t after) {
      if (!handler || before == after) return false;
      notifications.emplace_back(before, after);
      if (notifying) return false;
      notifying = true;
      return true;
    }
    void Notify() {
      for (;;) {
        std::pair<size_t, size_t> transition;
        {
          std::lock_guard<std::mutex> lock(mutex);
          if (notifications.empty()) {
            notifying = false;
            return;
          }
          transition = notifications.front();
          notifications.pop_front();
        }
        handler->Handle(transition.first, transition.second);
      }
    }
    bool Push(arrow::Result<T> value) {
      std::optional<arrow::Future<T>> delivery;
      bool notify = false;
      {
        std::lock_guard<std::mutex> lock(mutex);
        if (finished) return false;
        if (waiting) {
          delivery = std::move(waiting);
          waiting.reset();
        } else {
          const auto before = queue.size();
          queue.push_back(std::move(value));
          notify = Changed(before, queue.size());
        }
      }
      if (delivery) delivery->MarkFinished(std::move(value));
      if (notify) Notify();
      return true;
    }
    bool Close() {
      std::optional<arrow::Future<T>> delivery;
      {
        std::lock_guard<std::mutex> lock(mutex);
        if (finished) return false;
        finished = true;
        delivery = std::move(waiting);
        waiting.reset();
      }
      if (delivery) delivery->MarkFinished(arrow::IterationTraits<T>::End());
      return true;
    }
    bool IsClosed() const {
      std::lock_guard<std::mutex> lock(mutex);
      return finished;
    }
    arrow::Future<T> Pop() {
      arrow::Future<T> next;
      bool notify = false;
      {
        std::lock_guard<std::mutex> lock(mutex);
        if (waiting) {
          return arrow::Future<T>::MakeFinished(
              arrow::Status::Invalid("PushGenerator allows only one waiting consumer"));
        }
        if (!queue.empty()) {
          const auto before = queue.size();
          next = arrow::Future<T>::MakeFinished(std::move(queue.front()));
          queue.pop_front();
          notify = Changed(before, queue.size());
        } else if (finished) {
          next = arrow::Future<T>::MakeFinished(arrow::IterationTraits<T>::End());
        } else {
          next = arrow::Future<T>::Make();
          waiting = next;
        }
      }
      if (notify) Notify();
      return next;
    }
    mutable std::mutex mutex;
    std::deque<arrow::Result<T>> queue;
    std::optional<arrow::Future<T>> waiting;
    bool finished = false;
    std::optional<arrow::acero::BackpressureHandler> handler;
    std::deque<std::pair<size_t, size_t>> notifications;
    bool notifying = false;
  };

 public:
  class Producer {
   public:
    bool Push(arrow::Result<T> value) {
      auto state = state_.lock();
      return state && state->Push(std::move(value));
    }
    bool Close() {
      auto state = state_.lock();
      return state && state->Close();
    }
    bool is_closed() const {
      auto state = state_.lock();
      return !state || state->IsClosed();
    }

   private:
    friend class PushGenerator;
    explicit Producer(const std::shared_ptr<State>& state) : state_(state) {}
    std::weak_ptr<State> state_;
  };

  PushGenerator() : state_(std::make_shared<State>()) {}
  explicit PushGenerator(arrow::acero::BackpressureHandler handler)
      : state_(std::make_shared<State>(std::move(handler))) {}
  arrow::Future<T> operator()() const {
    auto state = state_;
    return state->Pop();
  }
  Producer producer() const { return Producer(state_); }

 private:
  std::shared_ptr<State> state_;
};
}  // namespace acier

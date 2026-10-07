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

#include <acier/type_fwd.h>
#include <arrow/acero/backpressure_handler.h>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <queue>

namespace acier {

/**
 * Simple implementation for a thread safe blocking unbound multi-consumer /
 * multi-producer concurrent queue
 */
template <class T>
class ConcurrentQueue {
 public:
  // Pops the oldest item from the queue but waits if the queue is empty until new items
  // are pushed.
  T WaitAndPop() {
    std::unique_lock<std::mutex> lock(mutex_);
    WaitUntilNonEmpty(lock);
    return PopUnlocked();
  }

  // Pops the oldest item from the queue, or returns a nullopt if empty
  std::optional<T> TryPop() {
    std::unique_lock<std::mutex> lock(mutex_);
    return TryPopUnlocked();
  }

  // Pushes an item to the queue
  void Push(const T& item) {
    std::unique_lock<std::mutex> lock(mutex_);
    return PushUnlocked(item);
  }

  // Clears the queue
  void Clear() {
    std::unique_lock<std::mutex> lock(mutex_);
    ClearUnlocked();
  }

  // Checks if the queue is empty
  bool Empty() const {
    std::unique_lock<std::mutex> lock(mutex_);
    return queue_.empty();
  }

  // Returns a reference to the next element in the queue. Must be called on a non-empty
  // queue. The caller must prevent removal of this element while using the reference.
  const T& Front() const {
    // Need to lock the queue because `front()` may be implemented in terms
    // of `begin()`, which isn't safe with concurrent calls to e.g. `push()`.
    // (see GH-44846)
    std::unique_lock<std::mutex> lock(mutex_);
    return queue_.front();
  }

 protected:
  std::mutex& GetMutex() { return mutex_; }

  size_t SizeUnlocked() const { return queue_.size(); }

  void WaitUntilNonEmpty(std::unique_lock<std::mutex>& lock) {
    cond_.wait(lock, [&] { return !queue_.empty(); });
  }

  T PopUnlocked() {
    auto item = queue_.front();
    queue_.pop();
    return item;
  }

  void PushUnlocked(const T& item) {
    queue_.push(item);
    cond_.notify_one();
  }

  void ClearUnlocked() { queue_ = std::queue<T>(); }

  std::optional<T> TryPopUnlocked() {
    // Try to pop the oldest value from the queue (or return nullopt if none)
    if (queue_.empty()) {
      return std::nullopt;
    } else {
      auto item = queue_.front();
      queue_.pop();
      return item;
    }
  }
  std::queue<T> queue_;

 private:
  mutable std::mutex mutex_;
  std::condition_variable cond_;
};

/// A FIFO queue that pauses producers at the high watermark and resumes them at
/// the low watermark. Backpressure callbacks may call queue methods synchronously.
template <typename T>
class BackpressureConcurrentQueue : private ConcurrentQueue<T> {
 public:
  explicit BackpressureConcurrentQueue(arrow::acero::BackpressureHandler handler)
      : handler_(std::move(handler)) {}

  using ConcurrentQueue<T>::Empty;
  using ConcurrentQueue<T>::Front;

  /// Waits for an item and removes the oldest item.
  T WaitAndPop() {
    T item = [this] {
      std::unique_lock<std::mutex> lock(ConcurrentQueue<T>::GetMutex());
      ConcurrentQueue<T>::WaitUntilNonEmpty(lock);
      return ConcurrentQueue<T>::PopUnlocked();
    }();
    ReportSize();
    return item;
  }

  /// Removes the oldest item, or returns nullopt if the queue is empty.
  std::optional<T> TryPop() {
    std::optional<T> item;
    {
      std::unique_lock<std::mutex> lock(ConcurrentQueue<T>::GetMutex());
      item = ConcurrentQueue<T>::TryPopUnlocked();
    }
    ReportSize();
    return item;
  }

  /// Appends an item unless ForceShutdown() has been called.
  void Push(const T& item) {
    {
      std::unique_lock<std::mutex> lock(ConcurrentQueue<T>::GetMutex());
      if (shutdown_) return;
      ConcurrentQueue<T>::PushUnlocked(item);
    }
    ReportSize();
  }

  /// Discards queued items and releases any queue-induced pause.
  void Clear() {
    {
      std::unique_lock<std::mutex> lock(ConcurrentQueue<T>::GetMutex());
      ConcurrentQueue<T>::ClearUnlocked();
    }
    ReportSize();
  }

  /// Discards queued items, releases any queue-induced pause, and ignores future pushes.
  /// This does not wake a blocked WaitAndPop(). Arrange consumer completion separately
  /// before calling this method.
  void ForceShutdown() {
    {
      std::unique_lock<std::mutex> lock(ConcurrentQueue<T>::GetMutex());
      shutdown_ = true;
      ConcurrentQueue<T>::ClearUnlocked();
    }
    ReportSize();
  }

 private:
  void ReportSize() {
    std::unique_lock<std::mutex> lock(ConcurrentQueue<T>::GetMutex());
    if (reporting_) return;
    reporting_ = true;
    while (reported_size_ != ConcurrentQueue<T>::SizeUnlocked()) {
      const size_t before = reported_size_;
      const size_t after = ConcurrentQueue<T>::SizeUnlocked();
      reported_size_ = after;
      lock.unlock();
      handler_.Handle(before, after);
      lock.lock();
    }
    reporting_ = false;
  }

  arrow::acero::BackpressureHandler handler_;
  size_t reported_size_ = 0;
  bool shutdown_ = false;
  bool reporting_ = false;
};

}  // namespace acier

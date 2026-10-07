// SPDX-License-Identifier: Apache-2.0
#include <acier/util/async_generator.h>
#include <atomic>
#include <functional>
#include <iostream>
#include <optional>
#include <thread>
#include <vector>

using Item = std::optional<int>;
using Generator = acier::PushGenerator<Item>;
struct Control : arrow::acero::BackpressureControl {
  explicit Control(std::function<void(bool)> callback) : callback(std::move(callback)) {}
  void Pause() override { callback(true); }
  void Resume() override { callback(false); }
  std::function<void(bool)> callback;
};
arrow::Status Run() {
  std::vector<bool> transitions;
  ARROW_ASSIGN_OR_RAISE(auto handler,
                        arrow::acero::BackpressureHandler::Make(
                            4, 8, std::make_unique<Control>([&](bool paused) {
                              transitions.push_back(paused);
                            })));
  Generator gen(std::move(handler));
  auto producer = gen.producer();
  auto pending = gen();
  if (pending.is_finished())
    return arrow::Status::Invalid("Unexpected completed consumer");
  if (!producer.Push(Item{42})) return arrow::Status::Invalid("Direct push rejected");
  ARROW_ASSIGN_OR_RAISE(auto direct, pending.result());
  if (direct != Item{42} || !transitions.empty())
    return arrow::Status::Invalid("Direct delivery changed queue size");
  for (int i = 0; i < 8; ++i) producer.Push(Item{i});
  if (transitions != std::vector<bool>{true})
    return arrow::Status::Invalid("High watermark");
  if (!producer.Close() || producer.Close() || producer.Push(Item{99}))
    return arrow::Status::Invalid("Close semantics");
  for (int i = 0; i < 8; ++i) {
    auto next_future = gen();
    ARROW_ASSIGN_OR_RAISE(auto next, next_future.result());
    if (next != Item{i}) return arrow::Status::Invalid("Queue order/drain");
  }
  if (transitions != std::vector<bool>{true, false})
    return arrow::Status::Invalid("Low watermark");
  auto end_future = gen();
  ARROW_ASSIGN_OR_RAISE(auto end, end_future.result());
  if (end) return arrow::Status::Invalid("Missing end marker");
  Generator errors;
  auto error_producer = errors.producer();
  auto waiting = errors();
  if (errors().result().ok())
    return arrow::Status::Invalid("Concurrent waiting consumers accepted");
  error_producer.Push(arrow::Status::Invalid("expected error"));
  if (waiting.result().ok()) return arrow::Status::Invalid("Error not delivered");
  waiting = errors();
  error_producer.Close();
  ARROW_ASSIGN_OR_RAISE(auto closed, waiting.result());
  if (closed) return arrow::Status::Invalid("Close failed to finish waiting consumer");
  auto expired = [] {
    Generator temporary;
    return temporary.producer();
  }();
  if (!expired.is_closed() || expired.Push(Item{1}) || expired.Close())
    return arrow::Status::Invalid("Expired producer");
  // Callback reentry must not deadlock and nested transitions must retain order.
  std::optional<Generator> reentrant;
  std::vector<bool> nested_transitions;
  ARROW_ASSIGN_OR_RAISE(auto nested_handler,
                        arrow::acero::BackpressureHandler::Make(
                            0, 1, std::make_unique<Control>([&](bool paused) {
                              nested_transitions.push_back(paused);
                              if (paused) {
                                auto value = (*reentrant)().result();
                              }
                            })));
  reentrant.emplace(std::move(nested_handler));
  auto nested_producer = reentrant->producer();
  nested_producer.Push(Item{1});
  if (nested_transitions != std::vector<bool>{true, false})
    return arrow::Status::Invalid("Reentrant callback order");
  // Concurrent delivery preserves sequence across pending futures and queued values.
  Generator concurrent;
  auto concurrent_producer = concurrent.producer();
  std::thread writer([concurrent_producer]() mutable {
    for (int i = 0; i < 10000; ++i) concurrent_producer.Push(Item{i});
    concurrent_producer.Close();
  });
  bool valid = true;
  for (int i = 0; i < 10000; ++i) {
    auto value = concurrent().result();
    if (!value.ok() || *value != Item{i}) valid = false;
  }
  auto final = concurrent().result();
  writer.join();
  if (!valid || !final.ok() || *final)
    return arrow::Status::Invalid("Concurrent generator sequence");
  return arrow::Status::OK();
}
int main() {
  const auto status = Run();
  if (!status.ok()) {
    std::cerr << status << '\n';
    return 1;
  }
  return 0;
}

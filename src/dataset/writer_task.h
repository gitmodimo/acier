// SPDX-License-Identifier: Apache-2.0
#pragma once
#include <arrow/util/future.h>

#include <functional>
#include <utility>

namespace acier::dataset::internal {
// The observed future cannot finish before error cleanup has returned. A
// separate AddCallback on the original future does not provide that lifetime
// guarantee.
inline arrow::Future<> ObserveWriterTask(
    arrow::Future<> task, std::function<void(const arrow::Status&)> on_error) {
  return task.Then([] { return arrow::Status::OK(); },
                   [on_error = std::move(on_error)](const arrow::Status& error) {
                     on_error(error);
                     return error;
                   });
}
}  // namespace acier::dataset::internal

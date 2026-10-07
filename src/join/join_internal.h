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

#include <cstdint>
#include <limits>
#include <type_traits>
#include <utility>

#include <arrow/compute/ordering.h>
#include <arrow/result.h>
#include <arrow/type.h>

namespace acier::join_internal {

// Adapted from PR #51094's time_series_util.h. Keep signed ordering across zero.
template <typename T, std::enable_if_t<std::is_integral_v<T> && !std::is_same_v<T, bool>,
                                       bool> = true>
uint64_t NormalizeTime(T value) {
  using U = std::make_unsigned_t<T>;
  U normalized = static_cast<U>(value);
  if constexpr (std::is_signed_v<T>) {
    normalized ^= U{1} << (std::numeric_limits<U>::digits - 1);
  }
  return static_cast<uint64_t>(normalized);
}

// Older Arrow keeps null placement on Ordering; newer Arrow keeps an optional
// legacy override there and the ordinary setting on each SortKey.
template <typename Ordering, typename SortKey>
arrow::compute::NullPlacement EffectiveNullPlacement(const Ordering& ordering,
                                                     const SortKey& key) {
  if constexpr (std::is_same_v<decltype(ordering.null_placement()),
                               arrow::compute::NullPlacement>) {
    return ordering.null_placement();
  } else {
    return ordering.null_placement().value_or(key.null_placement);
  }
}

template <typename Ordering>
arrow::Result<Ordering> NormalizeOrdering(const Ordering& ordering,
                                          const arrow::Schema& schema) {
  if (ordering.is_implicit() || ordering.is_unordered()) {
    return ordering;
  }
  auto keys = ordering.sort_keys();
  for (auto& key : keys) {
    ARROW_ASSIGN_OR_RAISE(auto path, key.target.FindOne(schema));
    key.target = arrow::FieldRef(std::move(path));
    if constexpr (!std::is_same_v<decltype(ordering.null_placement()),
                                  arrow::compute::NullPlacement>) {
      key.null_placement = EffectiveNullPlacement(ordering, key);
    }
  }
  if constexpr (std::is_same_v<decltype(ordering.null_placement()),
                               arrow::compute::NullPlacement>) {
    return Ordering(std::move(keys), ordering.null_placement());
  } else {
    return Ordering(std::move(keys));
  }
}

}  // namespace acier::join_internal

// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <arrow/type.h>
#include <arrow/type_traits.h>
#include <array>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

namespace acier {
namespace detail {
template <typename T, typename = void>
struct is_optional_like : std::false_type {};
template <typename T>
struct is_optional_like<T, std::void_t<decltype(*std::declval<T>())>>
    : std::bool_constant<std::is_constructible_v<bool, T> &&
                         !std::is_array_v<std::remove_reference_t<T>>> {};
}  // namespace detail

template <typename T, typename R = void>
using enable_if_optional_like = std::enable_if_t<detail::is_optional_like<T>::value, R>;

/// Map a C++ value to Arrow types; user-defined Arrow traits remain usable.
template <typename T, typename Enable = void>
struct CTypeTraits : arrow::CTypeTraits<T> {};

template <typename T>
struct CTypeTraits<T, enable_if_optional_like<T>>
    : CTypeTraits<std::decay_t<decltype(*std::declval<T>())>> {};

/// Nonoptional list elements are non-nullable; optional elements are nullable.
template <typename T>
struct CTypeTraits<std::vector<T>> : arrow::TypeTraits<arrow::ListType> {
  using ArrowType = arrow::ListType;
  static std::shared_ptr<arrow::DataType> type_singleton() {
    return arrow::list(arrow::field("item", CTypeTraits<T>::type_singleton(),
                                    detail::is_optional_like<T>::value));
  }
};

template <typename T, std::size_t N>
struct CTypeTraits<std::array<T, N>> : arrow::TypeTraits<arrow::FixedSizeListType> {
  using ArrowType = arrow::FixedSizeListType;
  static std::shared_ptr<arrow::DataType> type_singleton() {
    return arrow::fixed_size_list(arrow::field("item", CTypeTraits<T>::type_singleton(),
                                               detail::is_optional_like<T>::value),
                                  N);
  }
};
}  // namespace acier

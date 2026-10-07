// SPDX-License-Identifier: Apache-2.0
#include <acier/compute/api_aggregate.h>

#include <arrow/buffer.h>
#include <arrow/compute/api.h>
#include <arrow/compute/function_options.h>
#include <cstring>
#include <limits>
#include <sstream>
#include <string>
#include <tuple>
#include <type_traits>

namespace acier::compute {
namespace {
struct OptionData {
  uint32_t delta = 100, buffer_size = 500, min_count = 0;
  bool skip_nulls = true;
  TDigestOptions::Scaler scaler = TDigestOptions::K1;
  std::vector<double> q;
  auto Fields() const {
    return std::tie(delta, buffer_size, min_count, skip_nulls, scaler, q);
  }
};
template <typename T>
OptionData ReadOptions(const T& value) {
  OptionData out;
  out.delta = value.delta;
  out.scaler = value.scaler;
  if constexpr (std::is_same_v<T, TDigestOptions> ||
                std::is_same_v<T, TDigestMapOptions>) {
    out.buffer_size = value.buffer_size;
    out.skip_nulls = value.skip_nulls;
  }
  if constexpr (std::is_same_v<T, TDigestOptions> ||
                std::is_same_v<T, TDigestQuantileOptions>) {
    out.q = value.q;
    out.min_count = value.min_count;
  }
  return out;
}
template <typename T>
std::unique_ptr<arrow::compute::FunctionOptions> FromOptions(OptionData value) {
  if constexpr (std::is_same_v<T, TDigestOptions>) {
    return std::make_unique<T>(std::move(value.q), value.delta, value.buffer_size,
                               value.skip_nulls, value.min_count, value.scaler);
  } else if constexpr (std::is_same_v<T, TDigestMapOptions>) {
    return std::make_unique<T>(value.delta, value.buffer_size, value.skip_nulls,
                               value.scaler);
  } else if constexpr (std::is_same_v<T, TDigestReduceOptions>) {
    return std::make_unique<T>(value.delta, value.scaler);
  } else {
    return std::make_unique<T>(std::move(value.q), value.delta, value.min_count,
                               value.scaler);
  }
}
void Put(std::string* out, uint64_t value, unsigned bytes) {
  for (unsigned i = 0; i < bytes; ++i)
    out->push_back(static_cast<char>(value >> (8 * i)));
}
uint64_t Get(const uint8_t** input, unsigned bytes) {
  uint64_t value = 0;
  for (unsigned i = 0; i < bytes; ++i) value |= uint64_t{*(*input)++} << (8 * i);
  return value;
}
template <typename T>
class OptionsType final : public arrow::compute::FunctionOptionsType {
 public:
  const char* type_name() const override { return T::kTypeName; }
  std::string Stringify(const arrow::compute::FunctionOptions& value) const override {
    auto options = ReadOptions(static_cast<const T&>(value));
    std::ostringstream out;
    out << type_name() << "{delta=" << options.delta << ", scaler=";
    if (options.scaler == TDigestOptions::K0)
      out << "K0";
    else if (options.scaler == TDigestOptions::K1)
      out << "K1";
    else
      out << static_cast<int>(options.scaler);
    if constexpr (std::is_same_v<T, TDigestOptions> ||
                  std::is_same_v<T, TDigestMapOptions>) {
      out << ", buffer_size=" << options.buffer_size
          << ", skip_nulls=" << (options.skip_nulls ? "true" : "false");
    }
    if constexpr (std::is_same_v<T, TDigestOptions> ||
                  std::is_same_v<T, TDigestQuantileOptions>) {
      out << ", min_count=" << options.min_count << ", q=[";
      for (size_t i = 0; i < options.q.size(); ++i) {
        if (i) out << ", ";
        out << options.q[i];
      }
      out << ']';
    }
    return out.str() + '}';
  }
  bool Compare(const arrow::compute::FunctionOptions& l,
               const arrow::compute::FunctionOptions& r) const override {
    return ReadOptions(static_cast<const T&>(l)).Fields() ==
           ReadOptions(static_cast<const T&>(r)).Fields();
  }
  std::unique_ptr<arrow::compute::FunctionOptions> Copy(
      const arrow::compute::FunctionOptions& value) const override {
    return std::make_unique<T>(static_cast<const T&>(value));
  }
  arrow::Result<std::shared_ptr<arrow::Buffer>> Serialize(
      const arrow::compute::FunctionOptions& value) const override {
    const auto options = ReadOptions(static_cast<const T&>(value));
    if (options.q.size() > std::numeric_limits<uint32_t>::max()) {
      return arrow::Status::Invalid("Too many quantile probabilities");
    }
    std::string out = std::string("ACIER-TD1:") + type_name() + ':';
    Put(&out, options.delta, 4);
    Put(&out, options.buffer_size, 4);
    Put(&out, options.min_count, 4);
    Put(&out, options.skip_nulls ? 1 : 0, 4);
    Put(&out, static_cast<uint32_t>(options.scaler), 4);
    Put(&out, options.q.size(), 4);
    for (double q : options.q) {
      uint64_t bits;
      static_assert(sizeof(bits) == sizeof(q));
      std::memcpy(&bits, &q, sizeof(bits));
      Put(&out, bits, 8);
    }
    return arrow::Buffer::FromString(std::move(out));
  }
  arrow::Result<std::unique_ptr<arrow::compute::FunctionOptions>> Deserialize(
      const arrow::Buffer& buffer) const override {
    const auto prefix = std::string("ACIER-TD1:") + type_name() + ':';
    if (buffer.size() < static_cast<int64_t>(prefix.size() + 24) ||
        std::memcmp(buffer.data(), prefix.data(), prefix.size()) != 0) {
      return arrow::Status::Invalid("Invalid ", type_name(), " serialization");
    }
    const uint8_t* input = buffer.data() + prefix.size();
    OptionData options;
    options.delta = static_cast<uint32_t>(Get(&input, 4));
    options.buffer_size = static_cast<uint32_t>(Get(&input, 4));
    options.min_count = static_cast<uint32_t>(Get(&input, 4));
    const auto skip = Get(&input, 4);
    const auto scaler = Get(&input, 4);
    const auto n = Get(&input, 4);
    if (skip > 1 || scaler > TDigestOptions::K1 ||
        n != static_cast<uint64_t>(buffer.size() - prefix.size() - 24) / 8 ||
        (buffer.size() - prefix.size() - 24) % 8 != 0) {
      return arrow::Status::Invalid("Invalid ", type_name(), " serialization fields");
    }
    options.skip_nulls = skip != 0;
    options.scaler = static_cast<TDigestOptions::Scaler>(scaler);
    options.q.reserve(n);
    for (uint64_t i = 0; i < n; ++i) {
      const uint64_t bits = Get(&input, 8);
      double q;
      std::memcpy(&q, &bits, sizeof(q));
      options.q.push_back(q);
    }
    return FromOptions<T>(std::move(options));
  }
};
template <typename T>
const OptionsType<T>* Type() {
  static const OptionsType<T> type;
  return &type;
}
}  // namespace

TDigestOptions::TDigestOptions(double q, uint32_t delta, uint32_t buffer_size,
                               bool skip_nulls, uint32_t min_count, Scaler scaler)
    : TDigestOptions(std::vector<double>{q}, delta, buffer_size, skip_nulls, min_count,
                     scaler) {}
TDigestOptions::TDigestOptions(std::vector<double> q, uint32_t delta,
                               uint32_t buffer_size, bool skip_nulls, uint32_t min_count,
                               Scaler scaler)
    : FunctionOptions(Type<TDigestOptions>()),
      q(std::move(q)),
      delta(delta),
      buffer_size(buffer_size),
      skip_nulls(skip_nulls),
      min_count(min_count),
      scaler(scaler) {}
TDigestMapOptions::TDigestMapOptions(uint32_t delta, uint32_t buffer_size,
                                     bool skip_nulls, Scaler scaler)
    : FunctionOptions(Type<TDigestMapOptions>()),
      delta(delta),
      buffer_size(buffer_size),
      skip_nulls(skip_nulls),
      scaler(scaler) {}
TDigestReduceOptions::TDigestReduceOptions(uint32_t delta, Scaler scaler)
    : FunctionOptions(Type<TDigestReduceOptions>()), delta(delta), scaler(scaler) {}
TDigestQuantileOptions::TDigestQuantileOptions(double q, uint32_t delta,
                                               uint32_t min_count, Scaler scaler)
    : TDigestQuantileOptions(std::vector<double>{q}, delta, min_count, scaler) {}
TDigestQuantileOptions::TDigestQuantileOptions(std::vector<double> q, uint32_t delta,
                                               uint32_t min_count, Scaler scaler)
    : FunctionOptions(Type<TDigestQuantileOptions>()),
      q(std::move(q)),
      delta(delta),
      min_count(min_count),
      scaler(scaler) {}

namespace internal {
std::vector<const arrow::compute::FunctionOptionsType*> TDigestOptionTypes() {
  return {Type<TDigestOptions>(), Type<TDigestMapOptions>(), Type<TDigestReduceOptions>(),
          Type<TDigestQuantileOptions>()};
}
}  // namespace internal

#define ACIER_CALL(NAME, OPTIONS, FUNCTION)                                           \
  arrow::Result<arrow::Datum> NAME(const arrow::Datum& value, const OPTIONS& options, \
                                   arrow::compute::ExecContext* ctx) {                \
    return arrow::compute::CallFunction(FUNCTION, {value}, &options, ctx);            \
  }
ACIER_CALL(TDigest, TDigestOptions, "acier_tdigest")
ACIER_CALL(TDigestMap, TDigestMapOptions, "acier_tdigest_map")
ACIER_CALL(TDigestReduce, TDigestReduceOptions, "acier_tdigest_reduce")
ACIER_CALL(TDigestQuantile, TDigestQuantileOptions, "acier_tdigest_quantile")
ACIER_CALL(TDigestQuantileElementWise, TDigestQuantileOptions,
           "acier_tdigest_quantile_element_wise")
ACIER_CALL(ApproximateMedian, arrow::compute::ScalarAggregateOptions,
           "acier_approximate_median")
#undef ACIER_CALL
}  // namespace acier::compute

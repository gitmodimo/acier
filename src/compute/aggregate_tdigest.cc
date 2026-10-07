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

// Modified for Acier: owned namespaces and adapters using installed Arrow APIs.

#include <acier/compute/api_aggregate.h>
#include <acier/compute/initialize.h>
#include "tdigest_internal.h"

#include <arrow/api.h>
#include <arrow/compute/exec.h>
#include <arrow/compute/function.h>
#include <arrow/compute/initialize.h>
#include <arrow/compute/kernel.h>
#include <arrow/compute/registry.h>
#include <arrow/visit_type_inline.h>
#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <mutex>
#include <type_traits>

namespace acier::compute {
std::shared_ptr<arrow::DataType> TDigestCentroidType() {
  static auto type = arrow::struct_(
      {arrow::field("centroids",
                    arrow::list(arrow::field(
                        "item",
                        arrow::struct_({arrow::field("mean", arrow::float64(), false),
                                        arrow::field("weight", arrow::float64(), false)}),
                        false)),
                    false),
       arrow::field("min", arrow::float64(), true),
       arrow::field("max", arrow::float64(), true)});
  return type;
}
namespace internal {
std::vector<const arrow::compute::FunctionOptionsType*> TDigestOptionTypes();
namespace {
using namespace arrow;
using namespace arrow::compute;
using acier::compute::TDigestMapOptions;
using acier::compute::TDigestOptions;
using acier::compute::TDigestQuantileOptions;
using acier::compute::TDigestReduceOptions;

struct ScalarAggregator : KernelState {
  virtual Status Consume(KernelContext*, const ExecSpan&) = 0;
  virtual Status MergeFrom(KernelContext*, KernelState&&) = 0;
  virtual Status Finalize(KernelContext*, Datum*) = 0;
};

template <typename Options>
Result<const Options*> CheckedOptions(const KernelInitArgs& args) {
  const auto* options = dynamic_cast<const Options*>(args.options);
  if (!options) return Status::TypeError("Expected ", Options::kTypeName);
  if (options->delta == 0) return Status::Invalid("TDigest delta must be positive");
  if (options->scaler != TDigestOptions::K0 && options->scaler != TDigestOptions::K1) {
    return Status::Invalid("Unknown TDigest scaler");
  }
  if constexpr (std::is_same_v<Options, TDigestOptions> ||
                std::is_same_v<Options, TDigestMapOptions>) {
    if (options->buffer_size == 0) {
      return Status::Invalid("TDigest buffer_size must be positive");
    }
  }
  if constexpr (std::is_same_v<Options, TDigestOptions> ||
                std::is_same_v<Options, TDigestQuantileOptions>) {
    if (options->q.size() > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
      return Status::Invalid("Too many quantile probabilities");
    }
    for (double q : options->q) {
      if (!std::isfinite(q) || q < 0 || q > 1) {
        return Status::Invalid("Quantile probability must be finite and in [0, 1]");
      }
    }
  }
  return options;
}

struct TDigestBaseImpl : public ScalarAggregator {
  explicit TDigestBaseImpl(std::shared_ptr<TDigest::Scaler> scaler, uint32_t buffer_size)
      : tdigest{std::move(scaler), buffer_size}, all_valid{true} {}

  Status MergeFrom(KernelContext*, KernelState&& src) override {
    const auto& other = static_cast<const TDigestBaseImpl&>(src);
    if (!this->all_valid || !other.all_valid) {
      this->all_valid = false;
      return Status::OK();
    }
    this->tdigest.Merge(other.tdigest);
    return Status::OK();
  }

  static Result<std::shared_ptr<TDigest::Scaler>> MakeScaler(
      TDigestOptions::Scaler scaler, uint32_t delta) {
    if (delta == 0) return Status::Invalid("TDigest delta must be positive");
    switch (scaler) {
      case TDigestOptions::K0:
        return std::make_unique<TDigestScalerK0>(delta);
      case TDigestOptions::K1:
        return std::make_unique<TDigestScalerK1>(delta);
    }
    return Status::NotImplemented("Invalid TDigest scaler");
  }

  TDigest tdigest;
  bool all_valid;
  static const std::shared_ptr<DataType>& out_type() {
    static auto out_type = TDigestCentroidType();
    return out_type;
  }
};

struct TDigestQuantileFinalizer : public TDigestBaseImpl {
  template <typename... Args>
  explicit TDigestQuantileFinalizer(std::vector<double> q, uint32_t min_count,
                                    Args&&... args)
      : TDigestBaseImpl(std::forward<Args>(args)...),
        q(std::move(q)),
        min_count(min_count) {}

  bool isNull() {
    return this->tdigest.is_empty() || !this->all_valid ||
           this->tdigest.TotalWeight() < (double)min_count;
  }

  double Quantile(size_t i) { return this->tdigest.Quantile(this->q[i]); }
  void Reset() { this->tdigest.Reset(); }

  Status Finalize(KernelContext* ctx, Datum* out) override {
    const size_t out_length = q.size();
    auto out_data = ArrayData::Make(float64(), out_length, 0);
    out_data->buffers.resize(2, nullptr);
    ARROW_ASSIGN_OR_RAISE(out_data->buffers[1],
                          ctx->Allocate(out_length * sizeof(double)));
    double* out_buffer = out_data->template GetMutableValues<double>(1);

    if (isNull()) {
      ARROW_ASSIGN_OR_RAISE(out_data->buffers[0], ctx->AllocateBitmap(out_length));
      std::memset(out_data->buffers[0]->mutable_data(), 0x00,
                  out_data->buffers[0]->size());
      std::fill(out_buffer, out_buffer + out_length, 0.0);
      out_data->null_count = out_length;
    } else {
      for (size_t i = 0; i < out_length; ++i) {
        out_buffer[i] = Quantile(i);
      }
    }
    *out = Datum(std::move(out_data));
    return Status::OK();
  }

  std::vector<double> q;
  uint32_t min_count;
};

struct TDigestCentroidFinalizer : public TDigestBaseImpl {
  template <typename... Args>
  explicit TDigestCentroidFinalizer(Args&&... args)
      : TDigestBaseImpl(std::forward<Args>(args)...) {}

  Status Finalize(KernelContext* ctx, Datum* out) override {
    if (!this->all_valid) {
      *out = MakeNullScalar(out_type());
    } else {
      // Float64Array
      const int64_t out_length = this->tdigest.GetCentroidCount();
      auto mean_data = ArrayData::Make(float64(), out_length, 0);
      mean_data->buffers.resize(2, nullptr);
      ARROW_ASSIGN_OR_RAISE(mean_data->buffers[1],
                            ctx->Allocate(out_length * sizeof(double)));
      double* mean_buffer = mean_data->template GetMutableValues<double>(1);

      auto weight_data = ArrayData::Make(float64(), out_length, 0);
      weight_data->buffers.resize(2, nullptr);
      ARROW_ASSIGN_OR_RAISE(weight_data->buffers[1],
                            ctx->Allocate(out_length * sizeof(double)));
      double* weight_buffer = weight_data->template GetMutableValues<double>(1);
      for (int64_t i = 0; i < out_length; ++i) {
        std::tie(mean_buffer[i], weight_buffer[i]) = this->tdigest.GetCentroid(i);
      }

      ARROW_ASSIGN_OR_RAISE(
          auto centroids,
          StructArray::Make(
              {MakeArray(mean_data), MakeArray(weight_data)},
              {field("mean", float64(), false), field("weight", float64(), false)}));
      auto centroids_scalar = std::make_shared<ListScalar>(
          centroids, list(field("item",
                                struct_({field("mean", float64(), false),
                                         field("weight", float64(), false)}),
                                false)));
      std::shared_ptr<Scalar> min, max;
      if (!this->tdigest.is_empty()) {
        min = std::make_shared<DoubleScalar>(this->tdigest.Min());
        max = std::make_shared<DoubleScalar>(this->tdigest.Max());
      } else {
        min = max = MakeNullScalar(float64());
      }
      *out = std::make_shared<StructScalar>(
          std::vector<std::shared_ptr<Scalar>>{centroids_scalar, min, max}, out_type());
    }

    return Status::OK();
  }
};

template <typename ArrowType, typename TDigestFinalizer_T>
struct TDigestInputConsumerImpl : public TDigestFinalizer_T {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using CType = typename TypeTraits<ArrowType>::CType;

  template <typename... Args>
  explicit TDigestInputConsumerImpl(bool skip_nulls, const DataType& in_type,
                                    Args&&... args)
      : TDigestFinalizer_T(std::forward<Args>(args)...),
        skip_nulls{skip_nulls},
        decimal_scale{0} {
    if (is_decimal_type<ArrowType>::value) {
      decimal_scale = static_cast<const DecimalType&>(in_type).scale();
    }
  }

  template <typename T>
  double ToDouble(T value) const {
    return static_cast<double>(value);
  }
  double ToDouble(const Decimal32& value) const { return value.ToDouble(decimal_scale); }
  double ToDouble(const Decimal64& value) const { return value.ToDouble(decimal_scale); }
  double ToDouble(const Decimal128& value) const { return value.ToDouble(decimal_scale); }
  double ToDouble(const Decimal256& value) const { return value.ToDouble(decimal_scale); }

  Status Consume(KernelContext*, const ExecSpan& batch) override {
    if (!this->all_valid) return Status::OK();
    if (!skip_nulls && batch[0].null_count() > 0) {
      this->all_valid = false;
      return Status::OK();
    }
    if (batch[0].is_array()) {
      const ArraySpan& data = batch[0].array;
      const CType* values = data.GetValues<CType>(1);

      if (data.length > data.GetNullCount()) {
        for (int64_t i = 0; i < data.length; ++i) {
          if (data.IsValid(i)) this->tdigest.NanAdd(ToDouble(values[i]));
        }
      }
    } else {
      const CType value =
          static_cast<const typename TypeTraits<ArrowType>::ScalarType&>(*batch[0].scalar)
              .value;
      if (batch[0].scalar->is_valid) {
        for (int64_t i = 0; i < batch.length; i++) {
          this->tdigest.NanAdd(ToDouble(value));
        }
      }
    }
    return Status::OK();
  }

  bool skip_nulls;
  int32_t decimal_scale;
};

template <typename TDigestFinalizer_T>
struct TDigestCentroidConsumerImpl : public TDigestFinalizer_T {
  template <typename... Args>
  explicit TDigestCentroidConsumerImpl(Args&&... args)
      : TDigestFinalizer_T(std::forward<Args>(args)...) {}

  Status Consume(const Scalar* scalar) {
    const auto* input_struct_scalar = static_cast<const StructScalar*>(scalar);
    if (!input_struct_scalar->is_valid) {
      this->all_valid = false;
      return Status::OK();
    }
    if (!input_struct_scalar->value[0]->is_valid) {
      return Status::Invalid("Digest centroids must not be null");
    }
    auto centroids_array =
        static_cast<const ListScalar*>(input_struct_scalar->value[0].get())->value;
    auto centroids_struct_array = static_cast<const StructArray*>(centroids_array.get());
    auto mean_array = centroids_struct_array->field(0);
    auto weight_array = centroids_struct_array->field(1);
    auto min = static_cast<const DoubleScalar*>(input_struct_scalar->value[1].get());
    auto max = static_cast<const DoubleScalar*>(input_struct_scalar->value[2].get());
    auto mean_double_array = static_cast<const DoubleArray*>(mean_array.get());
    auto weight_double_array = static_cast<const DoubleArray*>(weight_array.get());
    if (centroids_struct_array->null_count() != 0 || mean_array->null_count() != 0 ||
        weight_array->null_count() != 0) {
      return Status::Invalid("Centroid entries, means, and weights must not be null");
    }
    ARROW_DCHECK_EQ(mean_double_array->length(), weight_double_array->length());
    if (mean_double_array->length() > 0) {
      if (!min->is_valid || !max->is_valid || std::isnan(min->value) ||
          std::isnan(max->value) || min->value > max->value) {
        return Status::Invalid("Nonempty digest requires valid ordered min/max");
      }
      this->tdigest.SetMinMax(min->value, max->value);
      for (int64_t i = 0; i < mean_double_array->length(); i++) {
        const double weight = weight_double_array->Value(i);
        if (!std::isfinite(weight) || weight <= 0) {
          return Status::Invalid("Centroid weight must be finite and positive");
        }
        this->tdigest.NanAdd(mean_double_array->Value(i), weight);
      }
    } else {
      if (min->is_valid || max->is_valid) {
        return Status::Invalid("Empty digest requires null min/max");
      }
    }

    return Status::OK();
  }
  Status Consume(KernelContext*, const ExecSpan& batch) override {
    if (!this->all_valid) return Status::OK();
    if (batch[0].null_count() > 0) {
      this->all_valid = false;
      return Status::OK();
    }
    if (batch[0].is_array()) {
      std::shared_ptr<Array> array = MakeArray(batch[0].array.ToArrayData());
      for (int64_t i = 0; i < array->length(); ++i) {
        ARROW_ASSIGN_OR_RAISE(auto scalar, array->GetScalar(i));
        ARROW_RETURN_NOT_OK(Consume(scalar.get()));
      }
    } else {
      const Scalar* scalar = batch[0].scalar;
      ARROW_RETURN_NOT_OK(Consume(scalar));
    }
    return Status::OK();
  }
};

template <typename ArrowType>
struct TDigestImpl
    : public TDigestInputConsumerImpl<ArrowType, TDigestQuantileFinalizer> {
  explicit TDigestImpl(const TDigestOptions& options, const DataType& in_type,
                       std::shared_ptr<TDigest::Scaler> scaler)
      : TDigestInputConsumerImpl<ArrowType, TDigestQuantileFinalizer>(
            // TDigestInputConsumerImpl
            options.skip_nulls, in_type,
            // TDigestQuantileFinalizer
            options.q, options.min_count,
            // TDigestBaseImpl
            std::move(scaler), options.buffer_size) {}
};

template <typename ArrowType>
struct TDigestMapImpl
    : public TDigestInputConsumerImpl<ArrowType, TDigestCentroidFinalizer> {
  explicit TDigestMapImpl(const TDigestMapOptions& options, const DataType& in_type,
                          std::shared_ptr<TDigest::Scaler> scaler)
      : TDigestInputConsumerImpl<ArrowType, TDigestCentroidFinalizer>(

            // TDigestInputConsumerImpl
            options.skip_nulls, in_type,
            // TDigestCentroidFinalizer
            // TDigestBaseImpl
            std::move(scaler), options.buffer_size) {}
};

struct TDigestReduceImpl : public TDigestCentroidConsumerImpl<TDigestCentroidFinalizer> {
  explicit TDigestReduceImpl(const TDigestReduceOptions& options,
                             std::shared_ptr<TDigest::Scaler> scaler)
      : TDigestCentroidConsumerImpl<TDigestCentroidFinalizer>(
            // TDigestCentroidConsumerImpl
            // TDigestCentroidFinalizer
            // TDigestBaseImpl
            std::move(scaler), options.delta) {}
};

struct TDigestQuantileImpl
    : public TDigestCentroidConsumerImpl<TDigestQuantileFinalizer> {
  explicit TDigestQuantileImpl(const TDigestQuantileOptions& options,
                               std::shared_ptr<TDigest::Scaler> scaler)
      : TDigestCentroidConsumerImpl<TDigestQuantileFinalizer>(

            // TDigestCentroidConsumerImpl
            // TDigestQuantileFinalizer
            options.q, options.min_count,
            // TDigestBaseImpl
            std::move(scaler), options.delta) {}
};

struct TDigestQuantileScalarImpl : public KernelState {
  explicit TDigestQuantileScalarImpl(const TDigestQuantileOptions& options,
                                     std::shared_ptr<TDigest::Scaler> scaler)
      : options(options), scaler(std::move(scaler)) {}

  static Result<std::unique_ptr<KernelState>> Init(KernelContext* ctx,
                                                   const KernelInitArgs& args) {
    ARROW_ASSIGN_OR_RAISE(auto checked_options,
                          CheckedOptions<TDigestQuantileOptions>(args));
    const auto& options = *checked_options;
    ARROW_ASSIGN_OR_RAISE(auto scaler,
                          TDigestBaseImpl::MakeScaler(options.scaler, options.delta));
    return std::make_unique<TDigestQuantileScalarImpl>(options, std::move(scaler));
  }

  static Result<TypeHolder> ResolveOutput(KernelContext* ctx,
                                          const std::vector<TypeHolder>& types) {
    auto state = static_cast<TDigestQuantileScalarImpl*>(ctx->state());
    return state->OutputType();
  }

  size_t OutputSize() const { return options.q.size(); }

  TypeHolder OutputType() const {
    return fixed_size_list(field("item", float64()), OutputSize());
  }

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    auto state = static_cast<TDigestQuantileScalarImpl*>(ctx->state());
    TDigestQuantileImpl tdigest(state->options, state->scaler);
    auto value_builder = std::make_shared<DoubleBuilder>(ctx->memory_pool());
    const auto output_size = state->OutputSize();
    FixedSizeListBuilder fsl_builder(
        ctx->memory_pool(), std::static_pointer_cast<arrow::ArrayBuilder>(value_builder),
        output_size);

    std::shared_ptr<Array> array;
    if (batch[0].is_scalar()) {
      ARROW_ASSIGN_OR_RAISE(
          array, MakeArrayFromScalar(*batch[0].scalar, batch.length, ctx->memory_pool()));
    } else {
      array = MakeArray(batch[0].array.ToArrayData());
    }

    for (int64_t i = 0; i < array->length(); ++i) {
      if (array->IsValid(i)) {
        ARROW_RETURN_NOT_OK(fsl_builder.Append());
        ARROW_ASSIGN_OR_RAISE(auto scalar, array->GetScalar(i));
        tdigest.Reset();
        ARROW_RETURN_NOT_OK(tdigest.Consume(scalar.get()));

        if (tdigest.isNull()) {
          ARROW_RETURN_NOT_OK(value_builder->AppendNulls(output_size));
        } else {
          for (size_t i = 0; i < output_size; ++i) {
            ARROW_RETURN_NOT_OK(value_builder->Append(tdigest.Quantile(i)));
          }
        }
      } else {
        ARROW_RETURN_NOT_OK(fsl_builder.AppendNull());
      }
    }
    std::shared_ptr<arrow::Array> out_array;
    ARROW_RETURN_NOT_OK(fsl_builder.Finish(&out_array));
    out->value = std::move(out_array->data());
    return Status::OK();
  }

 private:
  TDigestQuantileOptions options;
  std::shared_ptr<TDigest::Scaler> scaler;
};

template <template <typename> typename TDigestImpl_T, typename TDigestOptions_T>
struct TDigestInitState {
  std::unique_ptr<KernelState> state;
  KernelContext* ctx;
  const DataType& in_type;
  const TDigestOptions_T& options;

  TDigestInitState(KernelContext* ctx, const DataType& in_type,
                   const TDigestOptions_T& options)
      : ctx(ctx), in_type(in_type), options(options) {}

  Status Visit(const DataType&) {
    return Status::NotImplemented("No tdigest implemented");
  }

  Status Visit(const HalfFloatType&) {
    return Status::NotImplemented("No tdigest implemented");
  }

  template <typename Type>
  enable_if_number<Type, Status> Visit(const Type&) {
    ARROW_ASSIGN_OR_RAISE(auto scaler,
                          TDigestBaseImpl::MakeScaler(options.scaler, options.delta));
    state.reset(new TDigestImpl_T<Type>(options, in_type, std::move(scaler)));
    return Status::OK();
  }

  template <typename Type>
  enable_if_decimal<Type, Status> Visit(const Type&) {
    ARROW_ASSIGN_OR_RAISE(auto scaler,
                          TDigestBaseImpl::MakeScaler(options.scaler, options.delta));
    state.reset(new TDigestImpl_T<Type>(options, in_type, std::move(scaler)));
    return Status::OK();
  }

  Result<std::unique_ptr<KernelState>> Create() {
    RETURN_NOT_OK(VisitTypeInline(in_type, this));
    return std::move(state);
  }
};

template <template <typename> class Impl, typename Options>
Result<std::unique_ptr<KernelState>> NumericInit(KernelContext* ctx,
                                                 const KernelInitArgs& args) {
  ARROW_ASSIGN_OR_RAISE(auto options, CheckedOptions<Options>(args));
  TDigestInitState<Impl, Options> visitor(ctx, *args.inputs[0].type, *options);
  return visitor.Create();
}
template <typename Impl, typename Options>
Result<std::unique_ptr<KernelState>> CentroidInit(KernelContext*,
                                                  const KernelInitArgs& args) {
  ARROW_ASSIGN_OR_RAISE(auto options, CheckedOptions<Options>(args));
  ARROW_ASSIGN_OR_RAISE(auto scaler,
                        TDigestBaseImpl::MakeScaler(options->scaler, options->delta));
  return std::make_unique<Impl>(*options, std::move(scaler));
}
Status Consume(KernelContext* ctx, const ExecSpan& batch) {
  return static_cast<ScalarAggregator*>(ctx->state())->Consume(ctx, batch);
}
Status Merge(KernelContext* ctx, KernelState&& src, KernelState* dst) {
  return static_cast<ScalarAggregator*>(dst)->MergeFrom(ctx, std::move(src));
}
Status Finalize(KernelContext* ctx, Datum* out) {
  return static_cast<ScalarAggregator*>(ctx->state())->Finalize(ctx, out);
}
Status AddAggregate(ScalarAggregateFunction* function, InputType input, OutputType output,
                    KernelInit init, ScalarAggregateFinalize finalize = Finalize) {
  return function->AddKernel(ScalarAggregateKernel({std::move(input)}, std::move(output),
                                                   std::move(init), Consume, Merge,
                                                   finalize, false));
}
Result<std::vector<std::shared_ptr<Function>>> MakeFunctions() {
  static const TDigestOptions tdigest_options;
  static const TDigestMapOptions map_options;
  static const TDigestReduceOptions reduce_options;
  static const TDigestQuantileOptions quantile_options;
  static const ScalarAggregateOptions median_options;
  const auto centroid_type = TDigestCentroidType();
  auto tdigest = std::make_shared<ScalarAggregateFunction>(
      "acier_tdigest", Arity::Unary(),
      FunctionDoc{"Approximate numeric quantiles",
                  "Return one double per probability.\n"
                  "NaNs are ignored and do not count toward min_count.",
                  {"values"},
                  TDigestOptions::kTypeName},
      &tdigest_options);
  auto map = std::make_shared<ScalarAggregateFunction>(
      "acier_tdigest_map", Arity::Unary(),
      FunctionDoc{"Compress numeric values into a digest",
                  "Return a centroid struct.\n"
                  "An empty valid digest has an empty list and null min/max.",
                  {"values"},
                  TDigestMapOptions::kTypeName},
      &map_options);
  auto types = NumericTypes();
  types.push_back(decimal128(1, 1));
  types.push_back(decimal256(1, 1));
  for (const auto& type : types) {
    ARROW_RETURN_NOT_OK(AddAggregate(tdigest.get(), InputType(type->id()), float64(),
                                     NumericInit<TDigestImpl, TDigestOptions>));
    ARROW_RETURN_NOT_OK(AddAggregate(map.get(), InputType(type->id()), centroid_type,
                                     NumericInit<TDigestMapImpl, TDigestMapOptions>));
  }
  auto reduce = std::make_shared<ScalarAggregateFunction>(
      "acier_tdigest_reduce", Arity::Unary(),
      FunctionDoc{"Merge centroid sets",
                  "Return one digest; any null input digest makes "
                  "the result null.",
                  {"digests"},
                  TDigestReduceOptions::kTypeName},
      &reduce_options);
  ARROW_RETURN_NOT_OK(
      AddAggregate(reduce.get(), centroid_type, centroid_type,
                   CentroidInit<TDigestReduceImpl, TDigestReduceOptions>));
  auto quantile = std::make_shared<ScalarAggregateFunction>(
      "acier_tdigest_quantile", Arity::Unary(),
      FunctionDoc{"Quantiles from combined centroid sets",
                  "Return a double array of q.size() values.\n"
                  "Null, empty, or insufficient-weight input yields null quantiles.",
                  {"digests"},
                  TDigestQuantileOptions::kTypeName},
      &quantile_options);
  ARROW_RETURN_NOT_OK(
      AddAggregate(quantile.get(), centroid_type, float64(),
                   CentroidInit<TDigestQuantileImpl, TDigestQuantileOptions>));
  auto elementwise = std::make_shared<ScalarFunction>(
      "acier_tdigest_quantile_element_wise", Arity::Unary(),
      FunctionDoc{"Quantiles independently per digest",
                  "Return a fixed-size list of q.size() nullable doubles per row.\n"
                  "Null digests yield null lists; empty or insufficient-weight digests\n"
                  "yield lists of nulls.",
                  {"digests"},
                  TDigestQuantileOptions::kTypeName},
      &quantile_options);
  ScalarKernel kernel({InputType(centroid_type)},
                      TDigestQuantileScalarImpl::ResolveOutput,
                      TDigestQuantileScalarImpl::Exec, TDigestQuantileScalarImpl::Init);
  kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
  kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;
  ARROW_RETURN_NOT_OK(elementwise->AddKernel(std::move(kernel)));
  auto median = std::make_shared<ScalarAggregateFunction>(
      "acier_approximate_median", Arity::Unary(),
      FunctionDoc{"Approximate numeric median",
                  "Return a scalar median. NaNs are ignored and do not count toward "
                  "min_count.\n"
                  "Empty input yields null.",
                  {"values"},
                  "ScalarAggregateOptions"},
      &median_options);
  auto init = [tdigest](
                  KernelContext* ctx,
                  const KernelInitArgs& args) -> Result<std::unique_ptr<KernelState>> {
    const auto* scalar_options =
        dynamic_cast<const ScalarAggregateOptions*>(args.options);
    if (!scalar_options) return Status::TypeError("Expected ScalarAggregateOptions");
    auto types = args.inputs;
    ARROW_ASSIGN_OR_RAISE(auto numeric_kernel, tdigest->DispatchBest(&types));
    TDigestOptions options;
    options.min_count = scalar_options->min_count;
    options.skip_nulls = scalar_options->skip_nulls;
    return numeric_kernel->init(ctx, KernelInitArgs{numeric_kernel, types, &options});
  };
  auto finalize = [](KernelContext* ctx, Datum* out) -> Status {
    Datum quantiles;
    ARROW_RETURN_NOT_OK(Finalize(ctx, &quantiles));
    ARROW_ASSIGN_OR_RAISE(auto scalar, quantiles.make_array()->GetScalar(0));
    *out = std::move(scalar);
    return Status::OK();
  };
  ARROW_RETURN_NOT_OK(
      AddAggregate(median.get(), InputType::Any(), float64(), init, finalize));
  return std::vector<std::shared_ptr<Function>>{tdigest,  map,         reduce,
                                                quantile, elementwise, median};
}
Status Register(FunctionRegistry* registry) {
  ARROW_ASSIGN_OR_RAISE(auto functions, MakeFunctions());
  const auto options = TDigestOptionTypes();
  for (const auto* type : options)
    ARROW_RETURN_NOT_OK(registry->CanAddFunctionOptionsType(type));
  for (const auto& function : functions)
    ARROW_RETURN_NOT_OK(registry->CanAddFunction(function));
  for (const auto* type : options)
    ARROW_RETURN_NOT_OK(registry->AddFunctionOptionsType(type));
  for (auto& function : functions)
    ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(function)));
  return Status::OK();
}
}  // namespace
}  // namespace internal

arrow::Status Initialize(arrow::compute::FunctionRegistry* registry) {
  ARROW_RETURN_NOT_OK(arrow::compute::Initialize());
  if (registry && registry != arrow::compute::GetFunctionRegistry()) {
    return internal::Register(registry);
  }
  static std::once_flag once;
  static arrow::Status status;
  std::call_once(
      once, [] { status = internal::Register(arrow::compute::GetFunctionRegistry()); });
  return status;
}
}  // namespace acier::compute

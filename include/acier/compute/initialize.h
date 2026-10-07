// SPDX-License-Identifier: Apache-2.0
#pragma once
#include <acier/visibility.h>
#include <arrow/compute/type_fwd.h>
#include <arrow/status.h>

namespace acier::compute {
/// Register the six acier_tdigest*/acier_approximate_median functions and owned
/// option types. nullptr selects Arrow's default registry; repeated default
/// initialization is safe. For a supplied custom registry, duplicate names are
/// errors. Existing Arrow functions/options are never replaced. The caller must
/// serialize registration into a custom registry and keep it alive during use.
ACIER_EXPORT arrow::Status Initialize(
    arrow::compute::FunctionRegistry* registry = nullptr);
}  // namespace acier::compute

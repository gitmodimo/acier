// SPDX-License-Identifier: Apache-2.0
#pragma once
#include <acier/visibility.h>
#include <arrow/acero/exec_plan.h>
#include <arrow/status.h>

namespace acier {
/// Initialize Arrow Compute/Dataset and register Acier's factories/functions in
/// the default registries. Thread-safe and idempotent; returns initialization
/// errors. Call before constructing a plan that uses an acier_
/// factory/function.
ACIER_EXPORT arrow::Status Initialize();

/// Add Acier's execution factories to a caller-owned registry. The registry
/// must be non-null and remain alive while constructing plans that use it.
/// Duplicate names fail; this call is not idempotent. It does not register
/// Compute functions. Registration may be partial on error. Arrow's existing
/// factories are unchanged.
ACIER_EXPORT arrow::Status RegisterNodeFactories(
    arrow::acero::ExecFactoryRegistry* registry);
}  // namespace acier

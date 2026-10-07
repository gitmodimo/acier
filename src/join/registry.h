// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <arrow/acero/exec_plan.h>

namespace acier::internal {
arrow::Status RegisterAsofJoinNode(arrow::acero::ExecFactoryRegistry* registry);
arrow::Status RegisterSortedMergeNode(arrow::acero::ExecFactoryRegistry* registry);
arrow::Status RegisterJoinNodes(arrow::acero::ExecFactoryRegistry* registry);
}  // namespace acier::internal

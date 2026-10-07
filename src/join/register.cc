// SPDX-License-Identifier: Apache-2.0

#include "registry.h"

namespace acier::internal {
arrow::Status RegisterJoinNodes(arrow::acero::ExecFactoryRegistry* registry) {
  if (registry == nullptr) {
    return arrow::Status::Invalid("Acier join registration requires a registry");
  }
  ARROW_RETURN_NOT_OK(RegisterAsofJoinNode(registry));
  return RegisterSortedMergeNode(registry);
}
}  // namespace acier::internal

// SPDX-License-Identifier: Apache-2.0
#include <arrow/acero/exec_plan.h>
#include <arrow/status.h>

namespace acier::pipeline::internal {
arrow::Status RegisterAggregateNode(arrow::acero::ExecFactoryRegistry*);
}
namespace acier::internal {
arrow::Status RegisterPipeNodes(arrow::acero::ExecFactoryRegistry*);

arrow::Status RegisterPipelineNodes(arrow::acero::ExecFactoryRegistry* registry) {
  ARROW_RETURN_NOT_OK(RegisterPipeNodes(registry));
  return pipeline::internal::RegisterAggregateNode(registry);
}
}  // namespace acier::internal

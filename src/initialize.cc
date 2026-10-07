// SPDX-License-Identifier: Apache-2.0
#include <acier/compute/initialize.h>
#include <acier/initialize.h>
#include <arrow/compute/initialize.h>
#include <arrow/dataset/plan.h>

#include <mutex>

namespace acier {
namespace internal {
arrow::Status RegisterJoinNodes(arrow::acero::ExecFactoryRegistry*);
arrow::Status RegisterPipelineNodes(arrow::acero::ExecFactoryRegistry*);
arrow::Status RegisterSourceNodes(arrow::acero::ExecFactoryRegistry*);
arrow::Status RegisterFilterNode(arrow::acero::ExecFactoryRegistry*);
arrow::Status RegisterPivotLongerNode(arrow::acero::ExecFactoryRegistry*);
arrow::Status RegisterDatasetNodes(arrow::acero::ExecFactoryRegistry*);
}  // namespace internal

arrow::Status RegisterNodeFactories(arrow::acero::ExecFactoryRegistry* registry) {
  if (!registry) return arrow::Status::Invalid("Acier factory registry must be non-null");
  ARROW_RETURN_NOT_OK(internal::RegisterSourceNodes(registry));
  ARROW_RETURN_NOT_OK(internal::RegisterFilterNode(registry));
  ARROW_RETURN_NOT_OK(internal::RegisterPivotLongerNode(registry));
  ARROW_RETURN_NOT_OK(internal::RegisterDatasetNodes(registry));
  ARROW_RETURN_NOT_OK(internal::RegisterJoinNodes(registry));
  return internal::RegisterPipelineNodes(registry);
}

arrow::Status Initialize() {
  static std::once_flag flag;
  static arrow::Status status;
  std::call_once(flag, [] {
    status = arrow::compute::Initialize();
    if (!status.ok()) return;
    status = compute::Initialize();
    if (!status.ok()) return;
    arrow::dataset::internal::Initialize();
    status = RegisterNodeFactories(arrow::acero::default_exec_factory_registry());
  });
  return status;
}
}  // namespace acier

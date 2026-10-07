// SPDX-License-Identifier: Apache-2.0
#pragma once
#include <acier/dataset/type_fwd.h>
#include <arrow/dataset/file_base.h>

namespace acier::dataset {
/// Options for acier_write and acier_tee. The custom schema may change
/// metadata, field names, or nullability but must retain field count and types.
/// Supplying both custom_schema and custom_metadata fails. Partitioning is
/// required.
class WriteNodeOptions : public arrow::acero::ExecNodeOptions {
 public:
  explicit WriteNodeOptions(
      arrow::dataset::FileSystemDatasetWriteOptions options,
      std::shared_ptr<const arrow::KeyValueMetadata> metadata = nullptr)
      : write_options(std::move(options)), custom_metadata(std::move(metadata)) {}
  arrow::dataset::FileSystemDatasetWriteOptions write_options;
  std::shared_ptr<arrow::Schema> custom_schema;
  std::shared_ptr<const arrow::KeyValueMetadata> custom_metadata;
  /// Row watermark for upstream backpressure, default 8,388,608. Must be
  /// positive. An individual batch may exceed the watermark and is allowed to
  /// complete.
  uint64_t max_rows_queued = 8 * 1024 * 1024;
};
}  // namespace acier::dataset

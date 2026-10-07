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

// SPDX-License-Identifier: Apache-2.0
// Derived from Apache Arrow pipe options.
#pragma once
#include <acier/type_fwd.h>
#include <arrow/acero/exec_plan.h>
#include <arrow/acero/options.h>
#include <string>
#include <utility>

namespace acier {
/// Select a named pipe and the schema/ordering expected at this source.
class PipeSourceNodeOptions : public arrow::acero::ExecNodeOptions {
 public:
  PipeSourceNodeOptions(
      std::string pipe_name, std::shared_ptr<arrow::Schema> output_schema,
      arrow::compute::Ordering ordering = arrow::compute::Ordering::Unordered())
      : pipe_name(std::move(pipe_name)),
        output_schema(std::move(output_schema)),
        ordering(std::move(ordering)) {}
  std::string pipe_name;
  std::shared_ptr<arrow::Schema> output_schema;
  arrow::compute::Ordering ordering;
};
/// Connect a sink or tee to sources with the same name.
/// pause_on_any defaults to true; false requires all consumers to pause.
/// stop_on_any defaults to false; true stops input when any consumer stops.
class PipeSinkNodeOptions : public arrow::acero::ExecNodeOptions {
 public:
  explicit PipeSinkNodeOptions(std::string pipe_name, bool pause_on_any = true,
                               bool stop_on_any = false)
      : pipe_name(std::move(pipe_name)),
        pause_on_any(pause_on_any),
        stop_on_any(stop_on_any) {}
  std::string pipe_name;
  bool pause_on_any;
  bool stop_on_any;
};
}  // namespace acier

// Copyright 2026 Rossi Sun
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

/// @file arrow.h
///
/// @brief Optional Arrow-backed Traits adapter for Broken Pipeline.
///
/// Hosts that include this header are responsible for providing the Arrow include paths
/// and link libraries in their build system.

#include <memory>

#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>

namespace bp::traits::arrow {

/// @brief Arrow-backed Traits binding for Broken Pipeline.
///
/// - `Batch` is an `arrow::RecordBatch`.
/// - `Status` / `Result<T>` are Arrow's transport types.
struct Traits {
  using Batch = std::shared_ptr<::arrow::RecordBatch>;
  using Status = ::arrow::Status;

  template <class T>
  using Result = ::arrow::Result<T>;
};

}  // namespace bp::traits::arrow

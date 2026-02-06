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

#include <cstddef>

namespace bp {

/// @brief Task instance id within a `TaskGroup`.
///
/// Broken Pipeline intentionally keeps ids simple and uniform: task instances are indexed
/// 0..N-1 within their group.
///
/// Type: `std::size_t`.
using TaskId = std::size_t;

/// @brief Execution lane id.
///
/// Many operator implementations keep per-lane state indexed by `ThreadId`. In the
/// reference runtime (`PipeExec`), a task instance typically uses
/// `TaskId` as its `ThreadId`.
///
/// Type: `std::size_t`.
using ThreadId = std::size_t;

}  // namespace bp

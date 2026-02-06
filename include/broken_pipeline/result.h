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

namespace bp {

/// @brief Alias helper for `Traits::Result<T>`.
///
/// Broken Pipeline never assumes a particular error type or transport. Instead, all APIs
/// return `Traits::Result<T>` and rely on an Arrow-like result surface:
/// - `result.ok()`
/// - `result.status()`
/// - `result.ValueOrDie()`
template <class Traits, class T>
using Result = typename Traits::template Result<T>;

/// @brief Status type (success-or-error) used by Broken Pipeline.
///
/// This is `Traits::Status` (Arrow-style, separate from `Result<T>`).
template <class Traits>
using Status = typename Traits::Status;

}  // namespace bp

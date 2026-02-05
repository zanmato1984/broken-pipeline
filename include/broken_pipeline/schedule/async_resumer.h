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

/// @file async_resumer.h
///
/// @brief Resumer implementation for asynchronous schedulers.
///
/// `AsyncResumer` is a thin alias of `CallbackResumer` that pairs with
/// `AsyncAwaiter` to drive Folly-based scheduling.

#include "sync_resumer.h"

namespace bp::schedule {

/// @brief Asynchronous resumer used by `AsyncAwaiter`.
class AsyncResumer final : public CallbackResumer {};

}  // namespace bp::schedule

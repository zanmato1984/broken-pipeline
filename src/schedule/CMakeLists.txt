# Copyright 2026 Rossi Sun
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if (PROJECT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
  if (TARGET Arrow::arrow)
    message(FATAL_ERROR "Arrow::arrow is already defined.")
  endif()

  find_package(Arrow CONFIG REQUIRED)
  if (TARGET Arrow::arrow_shared)
    add_library(Arrow::arrow ALIAS Arrow::arrow_shared)
  elseif (TARGET Arrow::arrow_static)
    add_library(Arrow::arrow ALIAS Arrow::arrow_static)
  endif()
endif()

if (NOT TARGET Arrow::arrow)
  message(FATAL_ERROR "Arrow::arrow target must be provided before enabling broken_pipeline schedule")
endif()

find_package(folly CONFIG REQUIRED)

find_package(glog CONFIG REQUIRED)

add_library(broken_pipeline_schedule
  async_dual_pool_scheduler.cpp
  conditional_awaiter.cpp
  coro_awaiter.cpp
  future_awaiter.cpp
  naive_parallel_scheduler.cpp
  parallel_coro_scheduler.cpp
  sequential_coro_scheduler.cpp
  single_thread_awaiter.cpp
  single_thread_resumer.cpp
)
add_library(broken_pipeline::schedule ALIAS broken_pipeline_schedule)

set_target_properties(broken_pipeline_schedule PROPERTIES
  EXPORT_NAME schedule
)

target_compile_features(broken_pipeline_schedule PUBLIC cxx_std_20)
target_include_directories(broken_pipeline_schedule
  PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_link_libraries(broken_pipeline_schedule
  PUBLIC
    broken_pipeline
    Arrow::arrow
    Folly::folly
    glog::glog
)

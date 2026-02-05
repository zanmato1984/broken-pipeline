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

find_package(Arrow CONFIG REQUIRED)

set(_broken_pipeline_arrow_target "")
if(BUILD_SHARED_LIBS AND TARGET Arrow::arrow_shared)
  set(_broken_pipeline_arrow_target Arrow::arrow_shared)
elseif(TARGET Arrow::arrow_static)
  set(_broken_pipeline_arrow_target Arrow::arrow_static)
elseif(TARGET Arrow::arrow_shared)
  set(_broken_pipeline_arrow_target Arrow::arrow_shared)
else()
  message(FATAL_ERROR "Expected Arrow::arrow_shared or Arrow::arrow_static")
endif()

if(NOT TARGET Folly::folly)
  find_package(folly CONFIG REQUIRED)
endif()

if(NOT TARGET glog::glog)
  find_package(glog CONFIG REQUIRED)
endif()

add_library(broken_pipeline_schedule
  async_awaiter.cpp
  async_dual_pool_scheduler.cpp
  naive_parallel_scheduler.cpp
  sync_awaiter.cpp
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
    ${_broken_pipeline_arrow_target}
    Folly::folly
    glog::glog
)

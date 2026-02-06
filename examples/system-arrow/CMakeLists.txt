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

cmake_minimum_required(VERSION 3.20)

project(system-arrow LANGUAGES CXX)

find_package(Arrow CONFIG REQUIRED)

if (TARGET Arrow::arrow)
  message(FATAL_ERROR "Arrow::arrow is already defined.")
endif()

find_package(Arrow CONFIG REQUIRED)
if (TARGET Arrow::arrow_shared)
  add_library(Arrow::arrow ALIAS Arrow::arrow_shared)
elseif (TARGET Arrow::arrow_static)
  add_library(Arrow::arrow ALIAS Arrow::arrow_static)
else()
  message(FATAL_ERROR "Arrow package did not create arrow_shared or arrow_static targets")
endif()

# Build broken_pipeline with the schedule runtime enabled.
set(BROKEN_PIPELINE_BUILD_SCHEDULE ON CACHE BOOL "" FORCE)
set(BROKEN_PIPELINE_BUILD_TESTS OFF CACHE BOOL "" FORCE)
add_subdirectory(${CMAKE_CURRENT_LIST_DIR}/../.. broken_pipeline_build)

set(EXAMPLE_MAIN "${CMAKE_CURRENT_LIST_DIR}/../arrow-common/main.cpp")

add_executable(system-arrow ${EXAMPLE_MAIN})
target_compile_features(system-arrow PRIVATE cxx_std_20)
target_compile_definitions(system-arrow PRIVATE EXAMPLE_PIPELINE_NAME="system-arrow")

target_link_libraries(system-arrow
  PRIVATE
    broken_pipeline::schedule Arrow::arrow
)

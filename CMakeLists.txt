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

project(broken-pipeline VERSION 0.1.1 LANGUAGES CXX)

add_library(broken_pipeline INTERFACE)
target_compile_features(broken_pipeline INTERFACE cxx_std_20)
target_include_directories(broken_pipeline INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>
)
add_library(broken_pipeline::broken_pipeline ALIAS broken_pipeline)

option(BROKEN_PIPELINE_BUILD_SCHEDULE "Build broken_pipeline schedule library" OFF)
option(BROKEN_PIPELINE_BUILD_TESTS "Build broken_pipeline tests" OFF)

if(BROKEN_PIPELINE_BUILD_TESTS AND NOT BROKEN_PIPELINE_BUILD_SCHEDULE)
  message(FATAL_ERROR "BROKEN_PIPELINE_BUILD_TESTS requires BROKEN_PIPELINE_BUILD_SCHEDULE=ON.")
endif()

if(BROKEN_PIPELINE_BUILD_SCHEDULE)
  add_subdirectory(src/schedule)
endif()

if(BROKEN_PIPELINE_BUILD_TESTS)
  add_subdirectory(tests)
endif()

include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/install.cmake")

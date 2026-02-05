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

project(broken-pipeline VERSION 0.1.0 LANGUAGES CXX)

add_library(broken_pipeline INTERFACE)
target_compile_features(broken_pipeline INTERFACE cxx_std_20)
target_include_directories(broken_pipeline INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>
)

set(BROKEN_PIPELINE_ARROW_CORE_TARGET "" CACHE STRING
  "Override the Arrow CMake target to link against (e.g. Arrow::arrow_shared, Arrow::arrow_static, arrow_shared)"
)
set(BROKEN_PIPELINE_ARROW_COMPUTE_TARGET "" CACHE STRING
  "Override the ArrowCompute CMake target to link against (tests only)"
)
mark_as_advanced(BROKEN_PIPELINE_ARROW_CORE_TARGET BROKEN_PIPELINE_ARROW_COMPUTE_TARGET)

option(BROKEN_PIPELINE_BUILD_SCHEDULE "Build broken_pipeline schedule library" OFF)
option(BROKEN_PIPELINE_BUILD_TESTS "Build broken_pipeline tests" OFF)

if(BROKEN_PIPELINE_BUILD_SCHEDULE)
  add_subdirectory(src/schedule)
endif()

if(BROKEN_PIPELINE_BUILD_TESTS)
  add_subdirectory(tests)
endif()

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

install(
  TARGETS broken_pipeline
  EXPORT broken_pipelineTargets
)

if(BROKEN_PIPELINE_BUILD_SCHEDULE)
  install(
    TARGETS broken_pipeline_schedule
    EXPORT broken_pipelineTargets
  )
endif()

install(
  DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/include/broken_pipeline/"
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/broken_pipeline"
  PATTERN "schedule" EXCLUDE
)

if(BROKEN_PIPELINE_BUILD_SCHEDULE)
  install(
    DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/include/broken_pipeline/schedule/"
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/broken_pipeline/schedule"
  )
endif()

install(
  EXPORT broken_pipelineTargets
  FILE broken_pipelineTargets.cmake
  NAMESPACE broken_pipeline::
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/broken_pipeline"
)

set(BROKEN_PIPELINE_SCHEDULE_ENABLED "${BROKEN_PIPELINE_BUILD_SCHEDULE}")
configure_package_config_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake/broken_pipelineConfig.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/broken_pipelineConfig.cmake"
  INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/broken_pipeline"
)

write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/broken_pipelineConfigVersion.cmake"
  VERSION "${PROJECT_VERSION}"
  COMPATIBILITY SameMajorVersion
)

install(
  FILES
    "${CMAKE_CURRENT_BINARY_DIR}/broken_pipelineConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/broken_pipelineConfigVersion.cmake"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/broken_pipeline"
)

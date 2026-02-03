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

include_guard(GLOBAL)

# broken_pipeline Arrow integration (tests-only for now).
#
# Goals:
# - Support both "system" and "bundled" Arrow (bundled is Arrow 23.x only).
# - Use ArrowTesting for gtest utilities when building broken_pipeline tests.
# - When Arrow is built as a subproject, compute include directories so our test targets can
#   include Arrow's generated headers reliably.

set(_broken_pipeline_allowed_arrow_providers bundled system)
if(NOT BROKEN_PIPELINE_ARROW_PROVIDER IN_LIST _broken_pipeline_allowed_arrow_providers)
  message(
    FATAL_ERROR
    "Invalid BROKEN_PIPELINE_ARROW_PROVIDER='${BROKEN_PIPELINE_ARROW_PROVIDER}'. Allowed: bundled|system")
endif()

set(BROKEN_PIPELINE_ARROW_CORE_TARGET "")
set(BROKEN_PIPELINE_ARROW_COMPUTE_TARGET "")
set(BROKEN_PIPELINE_ARROW_TESTING_TARGET "")
set(BROKEN_PIPELINE_ARROW_INCLUDE_DIRS "")

function(_broken_pipeline_select_target OUT_VAR)
  foreach(_broken_pipeline_candidate IN LISTS ARGN)
    if(TARGET "${_broken_pipeline_candidate}")
      set(${OUT_VAR} "${_broken_pipeline_candidate}" PARENT_SCOPE)
      return()
    endif()
  endforeach()
  message(FATAL_ERROR "Failed to select a required CMake target")
endfunction()

if(BROKEN_PIPELINE_ARROW_PROVIDER STREQUAL "system")
  message(STATUS "Using system Arrow")

  set(Arrow_FIND_QUIETLY 0)
  find_package(Arrow CONFIG REQUIRED)

  find_package(ArrowTesting CONFIG REQUIRED)
  find_package(ArrowCompute CONFIG QUIET)

  _broken_pipeline_select_target(
    BROKEN_PIPELINE_ARROW_CORE_TARGET Arrow::arrow_shared arrow_shared Arrow::arrow arrow)
  _broken_pipeline_select_target(
    BROKEN_PIPELINE_ARROW_COMPUTE_TARGET
    ArrowCompute::arrow_compute_shared
    arrow_compute_shared
    ArrowCompute::arrow_compute
    arrow_compute
    Arrow::arrow_compute_shared
    Arrow::arrow_compute)
  _broken_pipeline_select_target(
    BROKEN_PIPELINE_ARROW_TESTING_TARGET
    ArrowTesting::arrow_testing_shared
    arrow_testing_shared
    ArrowTesting::arrow_testing
    arrow_testing)

  set(BROKEN_PIPELINE_ARROW_INCLUDE_DIRS "")
  return()
endif()

message(STATUS "Using bundled Arrow (FetchContent, Arrow 23.0.0)")

include(FetchContent)

if(POLICY CMP0135)
  cmake_policy(SET CMP0135 NEW)
endif()

# Arrow 23.x assumes `CMAKE_BUILD_TYPE` is non-empty when using a single-config
# generator. With CMake 4.x, some `string(TOLOWER ...)` calls error when
# `CMAKE_BUILD_TYPE` is empty, so set a default.
if(NOT CMAKE_CONFIGURATION_TYPES AND (NOT DEFINED CMAKE_BUILD_TYPE OR CMAKE_BUILD_TYPE STREQUAL ""))
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Choose the type of build." FORCE)
endif()

FetchContent_Declare(
  arrow
  URL "https://github.com/apache/arrow/archive/refs/tags/apache-arrow-23.0.0.tar.gz"
  URL_HASH "SHA256=7510f4b578febb3af5b3e93ad4616ae3cb680b0f651217ebb29f4c7e5ea952f3"
  SOURCE_SUBDIR cpp)

set(ARROW_DEPENDENCY_SOURCE "AUTO" CACHE STRING "" FORCE)
set(ARROW_SIMD_LEVEL "NONE" CACHE STRING "" FORCE)
set(ARROW_RUNTIME_SIMD_LEVEL "NONE" CACHE STRING "" FORCE)

set(ARROW_BUILD_SHARED ON CACHE BOOL "" FORCE)
set(ARROW_BUILD_STATIC OFF CACHE BOOL "" FORCE)

set(ARROW_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(ARROW_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(ARROW_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(ARROW_BUILD_INTEGRATION OFF CACHE BOOL "" FORCE)
set(ARROW_BUILD_UTILITIES OFF CACHE BOOL "" FORCE)

set(ARROW_COMPUTE ON CACHE BOOL "" FORCE)
# ArrowTesting requires JSON support.
set(ARROW_JSON ON CACHE BOOL "" FORCE)
set(ARROW_TESTING ON CACHE BOOL "" FORCE)

set(ARROW_ACERO OFF CACHE BOOL "" FORCE)
set(ARROW_CSV OFF CACHE BOOL "" FORCE)
set(ARROW_DATASET OFF CACHE BOOL "" FORCE)
set(ARROW_FILESYSTEM OFF CACHE BOOL "" FORCE)
set(ARROW_FLIGHT OFF CACHE BOOL "" FORCE)
set(ARROW_GANDIVA OFF CACHE BOOL "" FORCE)
set(ARROW_IPC OFF CACHE BOOL "" FORCE)
set(ARROW_ORC OFF CACHE BOOL "" FORCE)
set(ARROW_PARQUET OFF CACHE BOOL "" FORCE)
set(ARROW_PLASMA OFF CACHE BOOL "" FORCE)
set(ARROW_SUBSTRAIT OFF CACHE BOOL "" FORCE)

set(ARROW_WITH_UTF8PROC OFF CACHE BOOL "" FORCE)
set(ARROW_WITH_RE2 OFF CACHE BOOL "" FORCE)

set(ARROW_JEMALLOC OFF CACHE BOOL "" FORCE)
set(ARROW_MIMALLOC OFF CACHE BOOL "" FORCE)

set(ARROW_WITH_BROTLI OFF CACHE BOOL "" FORCE)
set(ARROW_WITH_BZ2 OFF CACHE BOOL "" FORCE)
set(ARROW_WITH_LZ4 OFF CACHE BOOL "" FORCE)
set(ARROW_WITH_SNAPPY OFF CACHE BOOL "" FORCE)
set(ARROW_WITH_ZLIB OFF CACHE BOOL "" FORCE)
set(ARROW_WITH_ZSTD OFF CACHE BOOL "" FORCE)

include(CheckCXXCompilerFlag)

set(_broken_pipeline_arrow_extra_cxx_flags "")

function(_broken_pipeline_maybe_append_cxx_flag FLAG)
  string(REPLACE "-" "_" _broken_pipeline_flag_var "${FLAG}")
  string(REPLACE "+" "x" _broken_pipeline_flag_var "${_broken_pipeline_flag_var}")
  set(_broken_pipeline_check_var "BROKEN_PIPELINE_SUPPORTS_${_broken_pipeline_flag_var}")

  check_cxx_compiler_flag("${FLAG}" "${_broken_pipeline_check_var}")
  if(${_broken_pipeline_check_var})
    set(_broken_pipeline_arrow_extra_cxx_flags "${_broken_pipeline_arrow_extra_cxx_flags} ${FLAG}"
        PARENT_SCOPE)
  endif()
endfunction()

_broken_pipeline_maybe_append_cxx_flag("-Wno-thread-safety-analysis")
_broken_pipeline_maybe_append_cxx_flag("-Wno-non-virtual-dtor")
_broken_pipeline_maybe_append_cxx_flag("-Wno-deprecated-declarations")
_broken_pipeline_maybe_append_cxx_flag("-Wno-unused-but-set-variable")
_broken_pipeline_maybe_append_cxx_flag("-Wno-implicit-const-int-float-conversion")
_broken_pipeline_maybe_append_cxx_flag("-Wno-non-c-typedef-for-linkage")
_broken_pipeline_maybe_append_cxx_flag("-Wno-unused-function")
_broken_pipeline_maybe_append_cxx_flag("-Wno-unused-private-field")

set(ARROW_CXXFLAGS "${ARROW_CXXFLAGS} ${_broken_pipeline_arrow_extra_cxx_flags}" CACHE STRING "" FORCE)

FetchContent_MakeAvailable(arrow)

set(_broken_pipeline_arrow_cpp_source_dir "${arrow_SOURCE_DIR}")
if(EXISTS "${_broken_pipeline_arrow_cpp_source_dir}/cpp/src/arrow/api.h")
  set(_broken_pipeline_arrow_cpp_source_dir "${_broken_pipeline_arrow_cpp_source_dir}/cpp")
elseif(NOT EXISTS "${_broken_pipeline_arrow_cpp_source_dir}/src/arrow/api.h")
  message(
    FATAL_ERROR
    "Unexpected Arrow source layout at '${arrow_SOURCE_DIR}': can't locate cpp/src/arrow/api.h")
endif()

set(BROKEN_PIPELINE_ARROW_INCLUDE_DIRS
    "${_broken_pipeline_arrow_cpp_source_dir}/src"
    "${_broken_pipeline_arrow_cpp_source_dir}/src/generated"
    "${arrow_BINARY_DIR}/src")

_broken_pipeline_select_target(
  BROKEN_PIPELINE_ARROW_CORE_TARGET Arrow::arrow_shared arrow_shared Arrow::arrow arrow)
_broken_pipeline_select_target(
  BROKEN_PIPELINE_ARROW_COMPUTE_TARGET
  ArrowCompute::arrow_compute_shared
  arrow_compute_shared
  ArrowCompute::arrow_compute
  arrow_compute)
_broken_pipeline_select_target(
  BROKEN_PIPELINE_ARROW_TESTING_TARGET
  ArrowTesting::arrow_testing_shared
  arrow_testing_shared
  ArrowTesting::arrow_testing
  arrow_testing)

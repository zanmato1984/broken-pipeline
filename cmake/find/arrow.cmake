include_guard(GLOBAL)

# opl Arrow integration (tests-only for now).
#
# Goals:
# - Support both "system" and "bundled" Arrow (bundled is Arrow 22.x only).
# - Use ArrowTesting for gtest utilities when building opl tests.
# - When Arrow is built as a subproject, compute include directories so our test targets can
#   include Arrow's generated headers reliably.

set(_opl_allowed_arrow_providers bundled system)
if(NOT OPL_ARROW_PROVIDER IN_LIST _opl_allowed_arrow_providers)
  message(
    FATAL_ERROR
    "Invalid OPL_ARROW_PROVIDER='${OPL_ARROW_PROVIDER}'. Allowed: bundled|system")
endif()

set(OPL_ARROW_CORE_TARGET "")
set(OPL_ARROW_COMPUTE_TARGET "")
set(OPL_ARROW_TESTING_TARGET "")
set(OPL_ARROW_INCLUDE_DIRS "")

function(_opl_select_target OUT_VAR)
  foreach(_opl_candidate IN LISTS ARGN)
    if(TARGET "${_opl_candidate}")
      set(${OUT_VAR} "${_opl_candidate}" PARENT_SCOPE)
      return()
    endif()
  endforeach()
  message(FATAL_ERROR "Failed to select a required CMake target")
endfunction()

if(OPL_ARROW_PROVIDER STREQUAL "system")
  message(STATUS "Using system Arrow")

  set(Arrow_FIND_QUIETLY 0)
  find_package(Arrow CONFIG REQUIRED)

  find_package(ArrowTesting CONFIG REQUIRED)
  find_package(ArrowCompute CONFIG QUIET)

  _opl_select_target(OPL_ARROW_CORE_TARGET Arrow::arrow_shared arrow_shared Arrow::arrow arrow)
  _opl_select_target(
    OPL_ARROW_COMPUTE_TARGET
    ArrowCompute::arrow_compute_shared
    arrow_compute_shared
    ArrowCompute::arrow_compute
    arrow_compute
    Arrow::arrow_compute_shared
    Arrow::arrow_compute)
  _opl_select_target(
    OPL_ARROW_TESTING_TARGET
    ArrowTesting::arrow_testing_shared
    arrow_testing_shared
    ArrowTesting::arrow_testing
    arrow_testing)

  set(OPL_ARROW_INCLUDE_DIRS "")
  return()
endif()

message(STATUS "Using bundled Arrow (FetchContent, Arrow 22.0.0)")

include(FetchContent)

if(POLICY CMP0135)
  cmake_policy(SET CMP0135 NEW)
endif()

# Arrow 22.x assumes `CMAKE_BUILD_TYPE` is non-empty when using a single-config
# generator. With CMake 4.x, some `string(TOLOWER ...)` calls error when
# `CMAKE_BUILD_TYPE` is empty, so set a default.
if(NOT CMAKE_CONFIGURATION_TYPES AND (NOT DEFINED CMAKE_BUILD_TYPE OR CMAKE_BUILD_TYPE STREQUAL ""))
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Choose the type of build." FORCE)
endif()

FetchContent_Declare(
  arrow
  URL "https://github.com/apache/arrow/archive/refs/tags/apache-arrow-22.0.0.tar.gz"
  URL_HASH "SHA256=8a95e6c7b9bec2bc0058feb73efe38ad6cfd49a0c7094db29b37ecaa8ab16051"
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
set(ARROW_TESTING ON CACHE BOOL "" FORCE)

set(ARROW_ACERO OFF CACHE BOOL "" FORCE)
set(ARROW_CSV OFF CACHE BOOL "" FORCE)
set(ARROW_DATASET OFF CACHE BOOL "" FORCE)
set(ARROW_FILESYSTEM OFF CACHE BOOL "" FORCE)
set(ARROW_FLIGHT OFF CACHE BOOL "" FORCE)
set(ARROW_GANDIVA OFF CACHE BOOL "" FORCE)
set(ARROW_IPC ON CACHE BOOL "" FORCE)
set(ARROW_JSON ON CACHE BOOL "" FORCE)
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

set(_opl_arrow_extra_cxx_flags "")

function(_opl_maybe_append_cxx_flag FLAG)
  string(REPLACE "-" "_" _opl_flag_var "${FLAG}")
  string(REPLACE "+" "x" _opl_flag_var "${_opl_flag_var}")
  set(_opl_check_var "OPL_SUPPORTS_${_opl_flag_var}")

  check_cxx_compiler_flag("${FLAG}" "${_opl_check_var}")
  if(${_opl_check_var})
    set(_opl_arrow_extra_cxx_flags "${_opl_arrow_extra_cxx_flags} ${FLAG}" PARENT_SCOPE)
  endif()
endfunction()

_opl_maybe_append_cxx_flag("-Wno-thread-safety-analysis")
_opl_maybe_append_cxx_flag("-Wno-non-virtual-dtor")
_opl_maybe_append_cxx_flag("-Wno-deprecated-declarations")
_opl_maybe_append_cxx_flag("-Wno-unused-but-set-variable")
_opl_maybe_append_cxx_flag("-Wno-implicit-const-int-float-conversion")
_opl_maybe_append_cxx_flag("-Wno-non-c-typedef-for-linkage")
_opl_maybe_append_cxx_flag("-Wno-unused-function")
_opl_maybe_append_cxx_flag("-Wno-unused-private-field")

set(ARROW_CXXFLAGS "${ARROW_CXXFLAGS} ${_opl_arrow_extra_cxx_flags}" CACHE STRING "" FORCE)

FetchContent_MakeAvailable(arrow)

set(_opl_arrow_cpp_source_dir "${arrow_SOURCE_DIR}")
if(EXISTS "${_opl_arrow_cpp_source_dir}/cpp/src/arrow/api.h")
  set(_opl_arrow_cpp_source_dir "${_opl_arrow_cpp_source_dir}/cpp")
elseif(NOT EXISTS "${_opl_arrow_cpp_source_dir}/src/arrow/api.h")
  message(
    FATAL_ERROR
    "Unexpected Arrow source layout at '${arrow_SOURCE_DIR}': can't locate cpp/src/arrow/api.h")
endif()

set(OPL_ARROW_INCLUDE_DIRS
    "${_opl_arrow_cpp_source_dir}/src"
    "${_opl_arrow_cpp_source_dir}/src/generated"
    "${arrow_BINARY_DIR}/src")

_opl_select_target(OPL_ARROW_CORE_TARGET Arrow::arrow_shared arrow_shared Arrow::arrow arrow)
_opl_select_target(
  OPL_ARROW_COMPUTE_TARGET
  ArrowCompute::arrow_compute_shared
  arrow_compute_shared
  ArrowCompute::arrow_compute
  arrow_compute)
_opl_select_target(
  OPL_ARROW_TESTING_TARGET
  ArrowTesting::arrow_testing_shared
  arrow_testing_shared
  ArrowTesting::arrow_testing
  arrow_testing)

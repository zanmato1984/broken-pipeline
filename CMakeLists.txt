cmake_minimum_required(VERSION 3.20)

project(opl VERSION 0.1.0 LANGUAGES CXX)

set(OPL_IS_TOP_LEVEL OFF)
if(CMAKE_SOURCE_DIR STREQUAL PROJECT_SOURCE_DIR)
  set(OPL_IS_TOP_LEVEL ON)
endif()

add_library(opl INTERFACE)
target_compile_features(opl INTERFACE cxx_std_20)
target_include_directories(opl INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>
)

option(OPL_BUILD_TESTS "Build opl tests (top-level only)" OFF)
if(NOT OPL_IS_TOP_LEVEL AND OPL_BUILD_TESTS)
  message(FATAL_ERROR "OPL_BUILD_TESTS must be OFF when opl is included as a subproject")
endif()

set(OPL_ARROW_PROVIDER "bundled" CACHE STRING "Arrow provider (bundled|system)")
set_property(CACHE OPL_ARROW_PROVIDER PROPERTY STRINGS bundled system)
set(OPL_ARROW_LINKAGE "shared" CACHE STRING "Arrow linkage (shared|static)")
set_property(CACHE OPL_ARROW_LINKAGE PROPERTY STRINGS shared static)

if(OPL_BUILD_TESTS)
  include(cmake/find/arrow.cmake)
  include(cmake/testing/testing.cmake)
  add_subdirectory(tests)
endif()

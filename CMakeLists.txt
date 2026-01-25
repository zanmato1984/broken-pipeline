cmake_minimum_required(VERSION 3.20)

project(opl VERSION 0.1.0 LANGUAGES CXX)

add_library(opl INTERFACE)
target_compile_features(opl INTERFACE cxx_std_20)
target_include_directories(opl INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>
)

option(OPL_BUILD_TESTS "Build opl tests" OFF)

set(OPL_ARROW_PROVIDER "bundled" CACHE STRING "Arrow provider (bundled|system)")
set_property(CACHE OPL_ARROW_PROVIDER PROPERTY STRINGS bundled system)

if(OPL_BUILD_TESTS)
  include(cmake/find/arrow.cmake)
  include(cmake/testing/testing.cmake)
  add_subdirectory(tests)
endif()

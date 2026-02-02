cmake_minimum_required(VERSION 3.20)

project(broken_pipeline VERSION 0.1.0 LANGUAGES CXX)

add_library(broken_pipeline INTERFACE)
target_compile_features(broken_pipeline INTERFACE cxx_std_20)
target_include_directories(broken_pipeline INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>
)

option(BROKEN_PIPELINE_BUILD_TESTS "Build broken_pipeline tests" OFF)

if(BROKEN_PIPELINE_BUILD_TESTS)
  include(cmake/testing/testing.cmake)
  add_subdirectory(tests)
endif()

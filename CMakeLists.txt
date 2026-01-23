cmake_minimum_required(VERSION 3.20)

project(openpipeline VERSION 0.1.0 LANGUAGES CXX)

add_library(openpipeline INTERFACE)
target_compile_features(openpipeline INTERFACE cxx_std_20)
target_include_directories(openpipeline INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>
)


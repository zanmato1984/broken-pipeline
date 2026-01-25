cmake_minimum_required(VERSION 3.20)

project(opl VERSION 0.1.0 LANGUAGES CXX)

add_library(opl INTERFACE)
target_compile_features(opl INTERFACE cxx_std_20)
target_include_directories(opl INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>
)

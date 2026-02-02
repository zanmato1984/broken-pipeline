cmake_minimum_required(VERSION 3.20)

project(broken_pipeline-arrow VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Arrow CONFIG REQUIRED)

add_executable(broken_pipeline-arrow-demo
  src/arrow_traits.h
  src/arrow_op.h
  src/main.cpp
)

target_include_directories(broken_pipeline-arrow-demo PRIVATE
  ${CMAKE_CURRENT_LIST_DIR}/../../include
  ${CMAKE_CURRENT_LIST_DIR}/src
)

if(TARGET Arrow::arrow_shared)
  target_link_libraries(broken_pipeline-arrow-demo PRIVATE Arrow::arrow_shared)
elseif(TARGET Arrow::arrow_static)
  target_link_libraries(broken_pipeline-arrow-demo PRIVATE Arrow::arrow_static)
elseif(TARGET Arrow::arrow)
  target_link_libraries(broken_pipeline-arrow-demo PRIVATE Arrow::arrow)
else()
  message(FATAL_ERROR "Arrow CMake target (Arrow::arrow_*) not found")
endif()

target_compile_features(broken_pipeline-arrow-demo PRIVATE cxx_std_20)

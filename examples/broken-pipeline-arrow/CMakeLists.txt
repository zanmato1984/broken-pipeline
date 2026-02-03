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

project(broken-pipeline-arrow VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Arrow CONFIG REQUIRED)

add_executable(broken-pipeline-arrow
  src/arrow_traits.h
  src/arrow_op.h
  src/main.cpp
)

target_include_directories(broken-pipeline-arrow PRIVATE
  ${CMAKE_CURRENT_LIST_DIR}/../../include
  ${CMAKE_CURRENT_LIST_DIR}/src
)

if(TARGET Arrow::arrow_shared)
  target_link_libraries(broken-pipeline-arrow PRIVATE Arrow::arrow_shared)
elseif(TARGET Arrow::arrow_static)
  target_link_libraries(broken-pipeline-arrow PRIVATE Arrow::arrow_static)
elseif(TARGET Arrow::arrow)
  target_link_libraries(broken-pipeline-arrow PRIVATE Arrow::arrow)
else()
  message(FATAL_ERROR "Arrow CMake target (Arrow::arrow_*) not found")
endif()

target_compile_features(broken-pipeline-arrow PRIVATE cxx_std_20)

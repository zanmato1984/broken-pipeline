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

include("${CMAKE_CURRENT_LIST_DIR}/deps.cmake")

broken_pipeline_resolve_arrow_core_target(BROKEN_PIPELINE_ARROW_CORE_TARGET_RESOLVED)
broken_pipeline_resolve_arrow_compute_target(BROKEN_PIPELINE_ARROW_COMPUTE_TARGET_RESOLVED)

message(STATUS "Finding Folly")
set(Folly_FIND_QUIETLY 0)
find_package(folly CONFIG REQUIRED)

message(STATUS "Finding GTest")
set(GTest_FIND_QUIETLY 0)
find_package(GTest REQUIRED)

find_package(glog CONFIG REQUIRED)
find_package(Threads REQUIRED)

message(STATUS "Building the broken-pipeline googletest unit tests")
enable_testing()

function(add_broken_pipeline_test TEST_NAME TEST_SRC)
  add_executable(${TEST_NAME} ${TEST_SRC})
  target_link_libraries(
    ${TEST_NAME}
    PRIVATE broken_pipeline GTest::gtest GTest::gtest_main Threads::Threads
            ${BROKEN_PIPELINE_ARROW_CORE_TARGET_RESOLVED}
            ${BROKEN_PIPELINE_ARROW_COMPUTE_TARGET_RESOLVED})

  set_target_properties(${TEST_NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/gtests")
  add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
endfunction()

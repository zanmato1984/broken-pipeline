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

set(BROKEN_PIPELINE_ARROW_PROVIDER "bundled" CACHE STRING "Arrow provider (bundled|system)")
set_property(CACHE BROKEN_PIPELINE_ARROW_PROVIDER PROPERTY STRINGS bundled system)

include(cmake/find/arrow.cmake)

message(STATUS "Building the broken-pipeline googletest unit tests")
enable_testing()
include(cmake/find/gtest.cmake)
find_package(Threads REQUIRED)
if(NOT BROKEN_PIPELINE_ARROW_CORE_TARGET)
  message(FATAL_ERROR "broken-pipeline tests require an Arrow core library target")
endif()
if(NOT BROKEN_PIPELINE_ARROW_COMPUTE_TARGET)
  message(FATAL_ERROR "broken-pipeline tests require an Arrow compute library target")
endif()
if(NOT BROKEN_PIPELINE_ARROW_TESTING_TARGET)
  message(FATAL_ERROR "broken-pipeline tests require an Arrow testing library target")
endif()

function(add_broken_pipeline_test TEST_NAME TEST_SRC)
  add_executable(${TEST_NAME} ${TEST_SRC})
  target_link_libraries(
    ${TEST_NAME}
    PRIVATE broken_pipeline GTest::gtest GTest::gtest_main Threads::Threads
            ${BROKEN_PIPELINE_ARROW_CORE_TARGET} ${BROKEN_PIPELINE_ARROW_COMPUTE_TARGET}
            ${BROKEN_PIPELINE_ARROW_TESTING_TARGET})

  if(BROKEN_PIPELINE_ARROW_INCLUDE_DIRS)
    foreach(_broken_pipeline_arrow_inc_dir IN LISTS BROKEN_PIPELINE_ARROW_INCLUDE_DIRS)
      target_include_directories(
        ${TEST_NAME} SYSTEM PRIVATE "$<BUILD_INTERFACE:${_broken_pipeline_arrow_inc_dir}>")
    endforeach()
  endif()

  set_target_properties(${TEST_NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/gtests")
  add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
endfunction()

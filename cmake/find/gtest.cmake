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

message(STATUS "Finding GTest")

# When using bundled Arrow, Arrow provides googletest targets (`gtest`/`gtest_main`)
# via ArrowTesting. Reuse those targets instead of fetching another copy.
if(BROKEN_PIPELINE_ARROW_PROVIDER STREQUAL "bundled")
  if(NOT TARGET gtest OR NOT TARGET gtest_main)
    message(
      FATAL_ERROR
      "Bundled Arrow test builds must provide googletest targets ('gtest' and 'gtest_main')")
  endif()
  if(NOT TARGET GTest::gtest)
    add_library(GTest::gtest ALIAS gtest)
  endif()
  if(NOT TARGET GTest::gtest_main)
    add_library(GTest::gtest_main ALIAS gtest_main)
  endif()
  return()
endif()

set(GTest_FIND_QUIETLY 0)
find_package(GTest REQUIRED)

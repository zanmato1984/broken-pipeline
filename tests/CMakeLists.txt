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

include("${PROJECT_SOURCE_DIR}/cmake/testing.cmake")

add_broken_pipeline_test(
  broken-pipeline-pipe-exec-test "${CMAKE_CURRENT_SOURCE_DIR}/pipe_exec_test.cpp")

add_broken_pipeline_test(
  broken-pipeline-pipeline-compile-test "${CMAKE_CURRENT_SOURCE_DIR}/pipeline_compile_test.cpp")

add_broken_pipeline_test(
  broken-pipeline-task-test "${CMAKE_CURRENT_SOURCE_DIR}/task_test.cpp")

add_subdirectory(schedule)

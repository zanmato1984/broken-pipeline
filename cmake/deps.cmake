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

function(broken_pipeline_resolve_arrow_core_target OUT_VAR)
  if(DEFINED BROKEN_PIPELINE_ARROW_CORE_TARGET AND NOT "${BROKEN_PIPELINE_ARROW_CORE_TARGET}" STREQUAL "")
    if(NOT TARGET "${BROKEN_PIPELINE_ARROW_CORE_TARGET}")
      message(FATAL_ERROR
        "BROKEN_PIPELINE_ARROW_CORE_TARGET was set to '${BROKEN_PIPELINE_ARROW_CORE_TARGET}', "
        "but that target does not exist."
      )
    endif()
    set("${OUT_VAR}" "${BROKEN_PIPELINE_ARROW_CORE_TARGET}" PARENT_SCOPE)
    return()
  endif()

  foreach(tgt IN ITEMS
    Arrow::arrow_shared
    Arrow::arrow_static
    Arrow::arrow
    arrow_shared
    arrow_static
    arrow
  )
    if(TARGET "${tgt}")
      set("${OUT_VAR}" "${tgt}" PARENT_SCOPE)
      return()
    endif()
  endforeach()

  message(STATUS "Finding Arrow")
  set(Arrow_FIND_QUIETLY 0)
  find_package(Arrow CONFIG REQUIRED)

  foreach(tgt IN ITEMS Arrow::arrow_shared Arrow::arrow_static Arrow::arrow arrow_shared arrow_static arrow)
    if(TARGET "${tgt}")
      set("${OUT_VAR}" "${tgt}" PARENT_SCOPE)
      return()
    endif()
  endforeach()

  message(FATAL_ERROR
    "Arrow CMake target not found. Expected one of: "
    "Arrow::arrow_shared, Arrow::arrow_static, Arrow::arrow (or non-namespaced equivalents). "
    "If Arrow is provided by the parent project, set BROKEN_PIPELINE_ARROW_CORE_TARGET to that target name."
  )
endfunction()

function(broken_pipeline_resolve_arrow_compute_target OUT_VAR)
  if(DEFINED BROKEN_PIPELINE_ARROW_COMPUTE_TARGET AND NOT "${BROKEN_PIPELINE_ARROW_COMPUTE_TARGET}" STREQUAL "")
    if(NOT TARGET "${BROKEN_PIPELINE_ARROW_COMPUTE_TARGET}")
      message(FATAL_ERROR
        "BROKEN_PIPELINE_ARROW_COMPUTE_TARGET was set to '${BROKEN_PIPELINE_ARROW_COMPUTE_TARGET}', "
        "but that target does not exist."
      )
    endif()
    set("${OUT_VAR}" "${BROKEN_PIPELINE_ARROW_COMPUTE_TARGET}" PARENT_SCOPE)
    return()
  endif()

  foreach(tgt IN ITEMS
    ArrowCompute::arrow_compute_shared
    ArrowCompute::arrow_compute_static
    ArrowCompute::arrow_compute
    arrow_compute_shared
    arrow_compute_static
    arrow_compute
  )
    if(TARGET "${tgt}")
      set("${OUT_VAR}" "${tgt}" PARENT_SCOPE)
      return()
    endif()
  endforeach()

  message(STATUS "Finding ArrowCompute")
  set(ArrowCompute_FIND_QUIETLY 0)
  find_package(ArrowCompute CONFIG REQUIRED)

  foreach(tgt IN ITEMS
    ArrowCompute::arrow_compute_shared
    ArrowCompute::arrow_compute_static
    ArrowCompute::arrow_compute
    arrow_compute_shared
    arrow_compute_static
    arrow_compute
  )
    if(TARGET "${tgt}")
      set("${OUT_VAR}" "${tgt}" PARENT_SCOPE)
      return()
    endif()
  endforeach()

  message(FATAL_ERROR
    "ArrowCompute CMake target not found. Expected one of: "
    "ArrowCompute::arrow_compute_shared, ArrowCompute::arrow_compute_static, ArrowCompute::arrow_compute "
    "(or non-namespaced equivalents). If ArrowCompute is provided by the parent project, "
    "set BROKEN_PIPELINE_ARROW_COMPUTE_TARGET to that target name."
  )
endfunction()


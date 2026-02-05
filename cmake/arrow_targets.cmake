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

include_guard(GLOBAL)

find_package(Arrow CONFIG REQUIRED)

if(NOT TARGET Arrow::arrow)
  foreach(tgt IN ITEMS Arrow::arrow_shared Arrow::arrow_static arrow_shared arrow_static arrow)
    if(TARGET "${tgt}")
      add_library(Arrow::arrow ALIAS "${tgt}")
      break()
    endif()
  endforeach()

  if(NOT TARGET Arrow::arrow)
    message(FATAL_ERROR
      "Arrow CMake target not found. Expected 'Arrow::arrow' (preferred) or one of: "
      "Arrow::arrow_shared, Arrow::arrow_static (or non-namespaced equivalents)."
    )
  endif()
endif()

if(NOT TARGET Arrow::compute)
  foreach(tgt IN ITEMS
      Arrow::arrow_compute_shared Arrow::arrow_compute_static Arrow::arrow_compute
      arrow_compute_shared arrow_compute_static arrow_compute
  )
    if(TARGET "${tgt}")
      add_library(Arrow::compute ALIAS "${tgt}")
      break()
    endif()
  endforeach()

  if(NOT TARGET Arrow::compute)
    find_package(ArrowCompute CONFIG QUIET)
    foreach(tgt IN ITEMS
        ArrowCompute::arrow_compute_shared ArrowCompute::arrow_compute_static ArrowCompute::arrow_compute
        arrow_compute_shared arrow_compute_static arrow_compute
    )
      if(TARGET "${tgt}")
        add_library(Arrow::compute ALIAS "${tgt}")
        break()
      endif()
    endforeach()
  endif()

  # Not all Arrow builds ship a distinct compute library target. Provide a target
  # name we can always link against (it will just be "arrow" in monolithic builds).
  if(NOT TARGET Arrow::compute)
    add_library(Arrow::compute INTERFACE IMPORTED)
    set_target_properties(Arrow::compute PROPERTIES INTERFACE_LINK_LIBRARIES Arrow::arrow)
  endif()
endif()

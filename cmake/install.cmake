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

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

install(
  TARGETS broken_pipeline
  EXPORT broken_pipelineTargets
)

if(BROKEN_PIPELINE_BUILD_SCHEDULE)
  install(
    TARGETS broken_pipeline_schedule
    EXPORT broken_pipelineTargets
  )
endif()

install(
  DIRECTORY "${PROJECT_SOURCE_DIR}/include/broken_pipeline/"
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/broken_pipeline"
  PATTERN "schedule" EXCLUDE
  PATTERN "traits" EXCLUDE
)

install(
  DIRECTORY "${PROJECT_SOURCE_DIR}/include/broken_pipeline/traits/"
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/broken_pipeline/traits"
)

if(BROKEN_PIPELINE_BUILD_SCHEDULE)
  install(
    DIRECTORY "${PROJECT_SOURCE_DIR}/include/broken_pipeline/schedule/"
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/broken_pipeline/schedule"
  )
endif()

install(
  EXPORT broken_pipelineTargets
  FILE broken_pipelineTargets.cmake
  NAMESPACE broken_pipeline::
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/broken_pipeline"
)

set(BROKEN_PIPELINE_SCHEDULE_ENABLED "${BROKEN_PIPELINE_BUILD_SCHEDULE}")
configure_package_config_file(
  "${CMAKE_CURRENT_LIST_DIR}/package_config.cmake.in"
  "${PROJECT_BINARY_DIR}/broken_pipelineConfig.cmake"
  INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/broken_pipeline"
)

write_basic_package_version_file(
  "${PROJECT_BINARY_DIR}/broken_pipelineConfigVersion.cmake"
  VERSION "${PROJECT_VERSION}"
  COMPATIBILITY SameMajorVersion
)

install(
  FILES
    "${PROJECT_BINARY_DIR}/broken_pipelineConfig.cmake"
    "${PROJECT_BINARY_DIR}/broken_pipelineConfigVersion.cmake"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/broken_pipeline"
)

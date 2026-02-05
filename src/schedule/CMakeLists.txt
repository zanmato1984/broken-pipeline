include("${PROJECT_SOURCE_DIR}/cmake/deps.cmake")

broken_pipeline_resolve_arrow_core_target(BROKEN_PIPELINE_ARROW_CORE_TARGET_RESOLVED)

if(NOT TARGET Folly::folly)
  message(STATUS "Finding Folly")
  set(Folly_FIND_QUIETLY 0)
  find_package(folly CONFIG REQUIRED)
endif()

if(NOT TARGET glog::glog)
  find_package(glog CONFIG REQUIRED)
endif()

add_library(broken_pipeline_schedule STATIC
  async_awaiter.cpp
  async_dual_pool_scheduler.cpp
  naive_parallel_scheduler.cpp
  scheduler.cpp
  sync_awaiter.cpp
)
add_library(broken_pipeline::schedule ALIAS broken_pipeline_schedule)

set_target_properties(broken_pipeline_schedule PROPERTIES
  OUTPUT_NAME "broken-pipeline-schedule"
  EXPORT_NAME schedule
)

target_compile_features(broken_pipeline_schedule PUBLIC cxx_std_20)
target_include_directories(broken_pipeline_schedule
  PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_link_libraries(broken_pipeline_schedule
  PUBLIC
    broken_pipeline
    "${BROKEN_PIPELINE_ARROW_CORE_TARGET_RESOLVED}"
    Folly::folly
    glog::glog
)


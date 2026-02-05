include("${PROJECT_SOURCE_DIR}/cmake/arrow_targets.cmake")

if(NOT TARGET Folly::folly)
  find_package(folly CONFIG REQUIRED)
endif()

if(NOT TARGET glog::glog)
  find_package(glog CONFIG REQUIRED)
endif()

add_library(broken_pipeline_schedule
  async_awaiter.cpp
  async_dual_pool_scheduler.cpp
  naive_parallel_scheduler.cpp
  scheduler.cpp
  sync_awaiter.cpp
)
add_library(broken_pipeline::schedule ALIAS broken_pipeline_schedule)

set_target_properties(broken_pipeline_schedule PROPERTIES
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
    Arrow::arrow
    Folly::folly
    glog::glog
)

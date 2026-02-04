add_library(schedule STATIC
  "${CMAKE_CURRENT_SOURCE_DIR}/async_awaiter.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/async_dual_pool_scheduler.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/naive_parallel_scheduler.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/scheduler.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/sync_awaiter.cpp"
)

target_include_directories(
  schedule
  PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}"
    "${BROKEN_PIPELINE_TEST_INCLUDE_DIR}"
)

target_link_libraries(
  schedule
  PUBLIC
    broken_pipeline
    ${BROKEN_PIPELINE_ARROW_CORE_TARGET}
)

target_link_libraries(schedule PUBLIC Folly::folly glog::glog)

# Schedule unit tests (isolated from broken-pipeline test_support, but reuse the same
# GTest wiring via add_broken_pipeline_test()).
add_broken_pipeline_test(
  broken-pipeline-schedule-sync-resumer-awaiter-test
  "${CMAKE_CURRENT_SOURCE_DIR}/sync_resumer_awaiter_test.cpp"
)
target_link_libraries(broken-pipeline-schedule-sync-resumer-awaiter-test PRIVATE schedule)

add_broken_pipeline_test(
  broken-pipeline-schedule-async-resumer-awaiter-test
  "${CMAKE_CURRENT_SOURCE_DIR}/async_resumer_awaiter_test.cpp"
)
target_link_libraries(broken-pipeline-schedule-async-resumer-awaiter-test PRIVATE schedule)

add_broken_pipeline_test(
  broken-pipeline-schedule-scheduler-test
  "${CMAKE_CURRENT_SOURCE_DIR}/scheduler_test.cpp"
)
target_link_libraries(broken-pipeline-schedule-scheduler-test PRIVATE schedule)

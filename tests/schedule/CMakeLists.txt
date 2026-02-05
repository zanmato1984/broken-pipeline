add_broken_pipeline_test(
  broken-pipeline-schedule-sync-resumer-awaiter-test
  "${CMAKE_CURRENT_SOURCE_DIR}/sync_resumer_awaiter_test.cpp"
)

add_broken_pipeline_test(
  broken-pipeline-schedule-async-resumer-awaiter-test
  "${CMAKE_CURRENT_SOURCE_DIR}/async_resumer_awaiter_test.cpp"
)

add_broken_pipeline_test(
  broken-pipeline-schedule-scheduler-test
  "${CMAKE_CURRENT_SOURCE_DIR}/scheduler_test.cpp"
)

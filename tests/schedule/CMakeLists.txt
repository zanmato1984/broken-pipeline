add_broken_pipeline_unit_test(
  broken-pipeline-schedule-sync-resumer-awaiter-test
  "${CMAKE_CURRENT_SOURCE_DIR}/sync_resumer_awaiter_test.cpp"
)
target_link_libraries(
  broken-pipeline-schedule-sync-resumer-awaiter-test
  PRIVATE
    broken_pipeline_schedule
)

add_broken_pipeline_unit_test(
  broken-pipeline-schedule-async-resumer-awaiter-test
  "${CMAKE_CURRENT_SOURCE_DIR}/async_resumer_awaiter_test.cpp"
)
target_link_libraries(
  broken-pipeline-schedule-async-resumer-awaiter-test
  PRIVATE
    broken_pipeline_schedule
)

add_broken_pipeline_unit_test(
  broken-pipeline-schedule-scheduler-test
  "${CMAKE_CURRENT_SOURCE_DIR}/scheduler_test.cpp"
)
target_link_libraries(
  broken-pipeline-schedule-scheduler-test
  PRIVATE
    broken_pipeline_schedule
)


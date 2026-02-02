set(BROKEN_PIPELINE_TEST_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/include")

function(add_broken_pipeline_unit_test TEST_NAME TEST_SRC)
  add_broken_pipeline_test(${TEST_NAME} ${TEST_SRC})
  target_include_directories(${TEST_NAME} PRIVATE "${BROKEN_PIPELINE_TEST_INCLUDE_DIR}")
endfunction()

add_broken_pipeline_unit_test(
  broken-pipeline-task-test "${CMAKE_CURRENT_SOURCE_DIR}/task_test.cpp")
add_broken_pipeline_unit_test(
  broken-pipeline-pipeline-compile-test "${CMAKE_CURRENT_SOURCE_DIR}/pipeline_compile_test.cpp")
add_broken_pipeline_unit_test(
  broken-pipeline-pipeline-task-test "${CMAKE_CURRENT_SOURCE_DIR}/pipeline_task_test.cpp")
set(BROKEN_PIPELINE_TEST_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/include")

function(add_broken_pipeline_unit_test TEST_NAME TEST_SRC)
  add_broken_pipeline_test(${TEST_NAME} ${TEST_SRC})
  target_include_directories(${TEST_NAME} PRIVATE "${BROKEN_PIPELINE_TEST_INCLUDE_DIR}")
endfunction()

add_broken_pipeline_unit_test(
  broken_pipeline_task_test "${CMAKE_CURRENT_SOURCE_DIR}/broken_pipeline_task_test.cpp")
add_broken_pipeline_unit_test(
  broken_pipeline_pipeline_compile_test
  "${CMAKE_CURRENT_SOURCE_DIR}/broken_pipeline_pipeline_compile_test.cpp")
add_broken_pipeline_unit_test(
  broken_pipeline_pipeline_task_test
  "${CMAKE_CURRENT_SOURCE_DIR}/broken_pipeline_pipeline_task_test.cpp")

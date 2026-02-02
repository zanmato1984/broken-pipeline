set(OPL_TEST_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/include")

function(add_opl_unit_test TEST_NAME TEST_SRC)
  add_opl_test(${TEST_NAME} ${TEST_SRC})
  target_include_directories(${TEST_NAME} PRIVATE "${OPL_TEST_INCLUDE_DIR}")
endfunction()

add_opl_unit_test(opl_arrow_smoke_test "${CMAKE_CURRENT_SOURCE_DIR}/opl_arrow_smoke_test.cpp")
add_opl_unit_test(opl_pipeline_exec_smoke_test "${CMAKE_CURRENT_SOURCE_DIR}/opl_pipeline_exec_smoke_test.cpp")
add_opl_unit_test(opl_task_test "${CMAKE_CURRENT_SOURCE_DIR}/opl_task_test.cpp")
add_opl_unit_test(opl_pipeline_compile_test "${CMAKE_CURRENT_SOURCE_DIR}/opl_pipeline_compile_test.cpp")
add_opl_unit_test(opl_pipeline_task_test "${CMAKE_CURRENT_SOURCE_DIR}/opl_pipeline_task_test.cpp")

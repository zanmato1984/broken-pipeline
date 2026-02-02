set(BROKEN_PIPELINE_ARROW_PROVIDER "bundled" CACHE STRING "Arrow provider (bundled|system)")
set_property(CACHE BROKEN_PIPELINE_ARROW_PROVIDER PROPERTY STRINGS bundled system)

include(cmake/find/arrow.cmake)

message(STATUS "Building the broken_pipeline googletest unit tests")
enable_testing()
include(cmake/find/gtest.cmake)
find_package(Threads REQUIRED)
if(NOT BROKEN_PIPELINE_ARROW_CORE_TARGET)
  message(FATAL_ERROR "broken_pipeline tests require an Arrow core library target")
endif()
if(NOT BROKEN_PIPELINE_ARROW_COMPUTE_TARGET)
  message(FATAL_ERROR "broken_pipeline tests require an Arrow compute library target")
endif()
if(NOT BROKEN_PIPELINE_ARROW_TESTING_TARGET)
  message(FATAL_ERROR "broken_pipeline tests require an Arrow testing library target")
endif()

function(add_broken_pipeline_test TEST_NAME TEST_SRC)
  add_executable(${TEST_NAME} ${TEST_SRC})
  target_link_libraries(
    ${TEST_NAME}
    PRIVATE broken_pipeline GTest::gtest GTest::gtest_main Threads::Threads
            ${BROKEN_PIPELINE_ARROW_CORE_TARGET} ${BROKEN_PIPELINE_ARROW_COMPUTE_TARGET}
            ${BROKEN_PIPELINE_ARROW_TESTING_TARGET})

  if(BROKEN_PIPELINE_ARROW_INCLUDE_DIRS)
    foreach(_broken_pipeline_arrow_inc_dir IN LISTS BROKEN_PIPELINE_ARROW_INCLUDE_DIRS)
      target_include_directories(
        ${TEST_NAME} SYSTEM PRIVATE "$<BUILD_INTERFACE:${_broken_pipeline_arrow_inc_dir}>")
    endforeach()
  endif()

  set_target_properties(${TEST_NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/gtests")
  add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
endfunction()

if(OPL_BUILD_TESTS)
  message(STATUS "Building the opl googletest unit tests")
  enable_testing()
  include(cmake/find/gtest.cmake)
  find_package(Threads REQUIRED)
  if(NOT OPL_ARROW_CORE_TARGET)
    message(FATAL_ERROR "OPL_BUILD_TESTS=ON requires an Arrow core library target")
  endif()
  if(NOT OPL_ARROW_TESTING_TARGET)
    message(FATAL_ERROR "OPL_BUILD_TESTS=ON requires an Arrow testing library target")
  endif()
endif()

function(add_opl_test TEST_NAME TEST_SRC)
  add_executable(${TEST_NAME} ${TEST_SRC})
  target_link_libraries(
    ${TEST_NAME}
    PRIVATE opl GTest::gtest GTest::gtest_main Threads::Threads ${OPL_ARROW_CORE_TARGET}
            ${OPL_ARROW_TESTING_TARGET})

  if(OPL_ARROW_INCLUDE_DIRS)
    foreach(_opl_arrow_inc_dir IN LISTS OPL_ARROW_INCLUDE_DIRS)
      target_include_directories(${TEST_NAME} SYSTEM PRIVATE "$<BUILD_INTERFACE:${_opl_arrow_inc_dir}>")
    endforeach()
  endif()

  set_target_properties(${TEST_NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/gtests")
  add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
endfunction()

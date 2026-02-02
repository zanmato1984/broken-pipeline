message(STATUS "Finding GTest")

# When using bundled Arrow, Arrow provides googletest targets (`gtest`/`gtest_main`)
# via ArrowTesting. Reuse those targets instead of fetching another copy.
if(BROKEN_PIPELINE_ARROW_PROVIDER STREQUAL "bundled")
  if(NOT TARGET gtest OR NOT TARGET gtest_main)
    message(
      FATAL_ERROR
      "Bundled Arrow test builds must provide googletest targets ('gtest' and 'gtest_main')")
  endif()
  if(NOT TARGET GTest::gtest)
    add_library(GTest::gtest ALIAS gtest)
  endif()
  if(NOT TARGET GTest::gtest_main)
    add_library(GTest::gtest_main ALIAS gtest_main)
  endif()
  return()
endif()

set(GTest_FIND_QUIETLY 0)
find_package(GTest REQUIRED)

rp_test(
  UNIT_TEST
  BINARY_NAME v8_script
  SOURCES
    script_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main Boost::unit_test_framework v::v8_engine v::utils
  INPUT_FILES ${CMAKE_CURRENT_SOURCE_DIR}/scripts/sum.js
              ${CMAKE_CURRENT_SOURCE_DIR}/scripts/to_upper.js
  ARGS "-- -c 1"
  LABELS v8_engine
)

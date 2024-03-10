rp_test(
  UNIT_TEST
  BINARY_NAME meta_test
  SOURCES
    meta_json_test.cc
  LIBRARIES
    v::json
    Boost::unit_test_framework
  LABELS meta
)

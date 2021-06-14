rp_test(
  UNIT_TEST
  BINARY_NAME pandaproxy_json_types
  SOURCES
    iobuf.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v::pandaproxy_json v::utils
  LABELS pandaproxy
)

rp_test(
  BENCHMARK_TEST
  BINARY_NAME pandaproxy_json_bench
  SOURCES parse.cc
  LIBRARIES Seastar::seastar_perf_testing v::pandaproxy_common
  LABELS pandaproxy
)

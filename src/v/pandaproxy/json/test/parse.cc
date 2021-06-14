// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf_parser.h"
#include "json/json.h"
#include "pandaproxy/json/iobuf.h"
#include "pandaproxy/json/requests/produce.h"
#include "pandaproxy/json/rjson_util.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/std-coroutine.hh>
#include <seastar/testing/perf_tests.hh>

#include <rapidjson/reader.h>

namespace pp = pandaproxy;
namespace ppj = pp::json;

auto make_binary_v2_handler() {
    return ppj::produce_request_handler<>(ppj::serialization_format::binary_v2);
}

auto gen(size_t data_size) {
    const ss::sstring beg{R"({"records": [)"};
    const ss::sstring end{R"(]})"};
    const ss::sstring rec{R"({"value": "dmVjdG9yaXplZA==","partition": 0})"};
    ss::sstring buf;
    buf += beg;
    for (size_t i = 0; i < data_size - 1; ++i) {
        buf += rec;
        buf += ",";
    }
    buf += rec;
    buf += end;
    return buf;
}

inline void parse_test(size_t data_size) {
    auto input = gen(data_size);
    perf_tests::start_measuring_time();
    perf_tests::do_not_optimize(
      ppj::rjson_parse(input.c_str(), make_binary_v2_handler()));
    perf_tests::stop_measuring_time();
}

inline ss::future<> async_parse_test(size_t data_size) {
    auto input = gen(data_size);
    perf_tests::start_measuring_time();
    perf_tests::do_not_optimize(
      co_await ppj::rjson_parse_async(input.c_str(), make_binary_v2_handler()));
    perf_tests::stop_measuring_time();
}

PERF_TEST(json_parse_test, sync) { parse_test(1024); }
PERF_TEST(json_parse_test, async) { return async_parse_test(1024); }

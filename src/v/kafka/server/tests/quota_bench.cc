/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "base/vassert.h"
#include "config/configuration.h"
#include "kafka/server/snc_quota_manager.h"

#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/testing/perf_tests.hh>
#include <seastar/util/later.hh>

static ss::logger fpt_logger("fpt_test");

ss::future<> test_snc_quota(size_t count) {
    kafka::snc_quota_manager::buckets_t b;
    ss::sharded<kafka::snc_quota_manager> snc;
    co_await snc.start(std::ref(b));
    co_await snc.invoke_on_all(&kafka::snc_quota_manager::start);

    perf_tests::start_measuring_time();
    co_await snc.invoke_on_all(
      [count](kafka::snc_quota_manager& snc) -> ss::future<> {
          kafka::snc_quota_context c{""};
          for (size_t i = 0; i < count; ++i) {
              snc.record_request_intake(c, 1);
              snc.record_request_receive(c, 1);
              snc.record_response(c, 1);
              co_await ss::coroutine::maybe_yield();
          }
          co_return;
      });
    perf_tests::stop_measuring_time();
    co_await snc.invoke_on_all([count](kafka::snc_quota_manager& snc) {
        auto in = snc.get_snc_quotas_probe().get_traffic_in();
        auto eg = snc.get_snc_quotas_probe().get_traffic_eg();
        vassert(in == count, "");
        vassert(eg == count, "");
    });
    co_await snc.stop();
}

struct quota_group {};

const size_t total_requests{10 << 20};

PERF_TEST_C(quota_group, test_snc_quota_on_threshold_1000) {
    int64_t q{total_requests};
    int64_t t = 1000;
    config::shard_local_cfg().kafka_throughput_limit_node_in_bps.set_value(q);
    config::shard_local_cfg().kafka_throughput_limit_node_out_bps.set_value(q);
    config::shard_local_cfg().kafka_throughput_replenish_threshold.set_value(t);
    return test_snc_quota(total_requests / ss::smp::count);
}

PERF_TEST_C(quota_group, test_snc_quota_on_def) {
    int64_t q{total_requests};
    int64_t t = 0;
    config::shard_local_cfg().kafka_throughput_limit_node_in_bps.set_value(q);
    config::shard_local_cfg().kafka_throughput_limit_node_out_bps.set_value(q);
    config::shard_local_cfg().kafka_throughput_replenish_threshold.set_value(t);
    co_await test_snc_quota(total_requests / ss::smp::count);
}

PERF_TEST_C(quota_group, test_snc_quota_off) {
    return test_snc_quota(total_requests / ss::smp::count);
}

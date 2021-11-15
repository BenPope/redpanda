// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/client/client.h"
#include "kafka/client/test/fixture.h"
#include "kafka/client/test/utils.h"

#include <seastar/core/future.hh>
#include <seastar/testing/thread_test_case.hh>

namespace kc = kafka::client;

inline const model::topic_partition unknown_tp{
  model::topic{"unknown"}, model::partition_id{0}};

FIXTURE_TEST(test_retry_list_offsets, kafka_client_fixture) {
    auto client = make_connected_client();
    auto stop_client = ss::defer([&client]() { client.stop().get(); });

    client.config().retry_base_backoff.set_value(10ms);
    client.config().retries.set_value(size_t(5));

    auto res = client.list_offsets(unknown_tp).get();
    info("res: {}", res);
    BOOST_REQUIRE_EQUAL(
      res.data.topics[0].partitions[0].error_code,
      kafka::error_code::unknown_topic_or_partition);
    BOOST_REQUIRE_EQUAL(res.data.topics[0].name, unknown_tp.topic);
}

FIXTURE_TEST(test_retry_produce, kafka_client_fixture) {
    auto client = make_connected_client();
    auto stop_client = ss::defer([&client]() { client.stop().get(); });

    client.config().retry_base_backoff.set_value(10ms);
    client.config().retries.set_value(size_t(5));

    auto res = client
                 .produce_record_batch(
                   unknown_tp, make_batch(model::offset{0}, 12))
                 .get();
    info("res: {}", res);
    BOOST_REQUIRE_EQUAL(
      res.error_code, kafka::error_code::unknown_topic_or_partition);
}

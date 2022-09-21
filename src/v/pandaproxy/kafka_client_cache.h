/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "config/broker_authn_endpoint.h"
#include "config/rest_authn_endpoint.h"
#include "pandaproxy/types.h"

#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index_container.hpp>

#include <utility>
#include <vector>

namespace bmi = boost::multi_index;

namespace pandaproxy {

class sharded_client_cache;

// A LRU cache implemented with a doubly-linked list and a
// hash using boost multi container. The list tracks
// frequency where the most recently used client is at the front.
// When the cache is full remove the client from the end of the
// list. The hash is for constant time look-ups of the kafka clients.
class kafka_client_cache {
public:
    // Tags used for indexing
    struct underlying_list {};
    struct underlying_hash {};

    using underlying_t = bmi::multi_index_container<
      timestamped_user,
      bmi::indexed_by<
        bmi::sequenced<bmi::tag<underlying_list>>,
        bmi::hashed_unique<
          bmi::tag<underlying_hash>,
          bmi::
            member<timestamped_user, ss::sstring, &timestamped_user::username>,
          std::hash<ss::sstring>,
          std::equal_to<>>>>;

    kafka_client_cache(
      YAML::Node const& cfg,
      config::binding<int64_t> max_size,
      config::binding<model::timestamp::type> keep_alive);

    ~kafka_client_cache() = default;

    kafka_client_cache(kafka_client_cache const&) = delete;
    kafka_client_cache& operator=(kafka_client_cache const&) = delete;

    client_ptr
    make_client(credential_t user, config::rest_authn_method authn_method);

    client_ptr
    fetch_or_insert(credential_t user, config::rest_authn_method authn_method);

    void clean_stale_clients();

    size_t size() const;
    size_t max_size() const;

private:
    kafka::client::configuration _config;
    config::binding<int64_t> _cache_max_size;
    config::binding<model::timestamp::type> _keep_alive;
    underlying_t _cache;
};
} // namespace pandaproxy

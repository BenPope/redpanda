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
#include "hashing/jump_consistent_hash.h"
#include "hashing/xx.h"
#include "pandaproxy/kafka_client_cache.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/types.h"

#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/timer.hh>

#include <chrono>
#include <utility>

namespace pandaproxy {

inline ss::shard_id user_shard(const ss::sstring& name) {
    auto hash = xxhash_64(name.data(), name.length());
    return jump_consistent_hash(hash, ss::smp::count);
}

class sharded_client_cache {
public:
    ss::future<> start(
      ss::smp_service_group sg,
      YAML::Node const& cfg,
      config::binding<int64_t> max_size,
      config::binding<model::timestamp::type> keep_alive);

    ss::future<> stop();

    static std::chrono::milliseconds clean_timer();

    template<
      typename Func,
      typename... Args,
      typename Ret = ss::futurize_t<
        std::invoke_result_t<Func, kafka_client_cache&, Args...>>>
    Ret invoke_on_cache(credential_t user, Func&& func, Args&&... args) {
        return ss::with_gate(_gate, [this, user, &func, &args...] {
            // Access the cache on the appropriate
            // shard please.
            ss::shard_id u_shard{user_shard(user.name)};
            return _cache.invoke_on(
              u_shard,
              _smp_opts,
              std::forward<Func>(func),
              std::forward<Args>(args)...);
        });
    }

private:
    ss::smp_submit_to_options _smp_opts;
    ss::gate _gate;
    ss::sharded<kafka_client_cache> _cache;
    ss::timer<ss::lowres_clock> _clean_timer;
};
} // namespace pandaproxy

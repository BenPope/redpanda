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

#include "pandaproxy/sharded_client_cache.h"

#include "ssx/future-util.h"

using namespace std::chrono_literals;

namespace pandaproxy {

static constexpr auto clean_timer_period = 10s;

ss::future<> sharded_client_cache::start(
  ss::smp_service_group sg,
  YAML::Node const& cfg,
  config::binding<int64_t> max_size,
  config::binding<model::timestamp::type> keep_alive) {
    _smp_opts = ss::smp_submit_to_options{sg};

    _clean_timer.set_callback([this] {
        ssx::spawn_with_gate(_gate, [this] {
            return _cache.invoke_on_all(
              _smp_opts,
              [](kafka_client_cache& cache) { cache.clean_stale_clients(); });
        });
    });

    _clean_timer.arm_periodic(clean_timer_period);

    return _cache.start(cfg, max_size, keep_alive);
}

ss::future<> sharded_client_cache::stop() {
    // Reset callback first because the original
    // captured "this" and cancelling on that could
    // be unsafe.
    _clean_timer.set_callback([] {});
    _clean_timer.cancel();
    co_await _gate.close();
    co_await _cache.stop();
}

std::chrono::milliseconds sharded_client_cache::clean_timer() {
    return clean_timer_period;
}

} // namespace pandaproxy

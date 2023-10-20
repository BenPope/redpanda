/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "config/configuration.h"
#include "prometheus/prometheus_sanitize.h"
#include "ssx/metrics.h"

#include <seastar/core/metrics.hh>

namespace kafka {
class sasl_probe {
public:
    sasl_probe() = default;
    sasl_probe(const sasl_probe&) = delete;
    sasl_probe& operator=(const sasl_probe&) = delete;
    sasl_probe(sasl_probe&&) = delete;
    sasl_probe& operator=(sasl_probe&&) = delete;
    ~sasl_probe() = default;

    void setup_metrics(std::string_view name) {
        namespace sm = ss::metrics;
        if (config::shard_local_cfg().disable_metrics()) {
            return;
        }

        auto aggregate_labels = config::shard_local_cfg().aggregate_metrics()
                                  ? std::vector<sm::label>{sm::shard_label}
                                  : std::vector<sm::label>{};

        _metrics.add_group(
          prometheus_sanitize::metrics_name(ssx::sformat("{}:sasl", name)),
          {
            sm::make_counter(
              "session_expiration_count",
              [this] { return _session_expiration_count; },
              sm::description("Total number of SASL session expirations"))
              .aggregate(aggregate_labels),
            sm::make_counter(
              "session_reauth_attempts",
              [this] { return _session_reauth_attempts; },
              sm::description("Total number of SASL reauthentication attempts"))
              .aggregate(aggregate_labels),
          });
    }

    void session_expired() { ++_session_expiration_count; }
    void session_reauth() { ++_session_reauth_attempts; }

private:
    ssx::metrics::all_metrics_groups _metrics;
    uint32_t _session_expiration_count{0};
    uint32_t _session_reauth_attempts{0};
};

}; // namespace kafka

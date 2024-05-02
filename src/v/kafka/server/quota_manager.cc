// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/quota_manager.h"

#include "base/vassert.h"
#include "base/vlog.h"
#include "config/configuration.h"
#include "container/fragmented_vector.h"
#include "kafka/server/atomic_token_bucket.h"
#include "kafka/server/logger.h"
#include "resource_mgmt/rate.h"

#include <seastar/core/future.hh>
#include <seastar/core/map_reduce.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/smp.hh>

#include <fmt/chrono.h>

#include <chrono>
#include <optional>
#include <stdexcept>
#include <string_view>

using namespace std::chrono_literals;

namespace kafka {

using clock = quota_manager::clock;
using throttle_delay = quota_manager::throttle_delay;

quota_manager::quota_manager(client_quotas_t& client_quotas)
  : _default_num_windows(config::shard_local_cfg().default_num_windows.bind())
  , _default_window_width(config::shard_local_cfg().default_window_sec.bind())
  , _replenish_threshold(
      config::shard_local_cfg().kafka_throughput_replenish_threshold.bind())
  , _default_target_produce_tp_rate(
      config::shard_local_cfg().target_quota_byte_rate.bind())
  , _default_target_fetch_tp_rate(
      config::shard_local_cfg().target_fetch_quota_byte_rate.bind())
  , _target_partition_mutation_quota(
      config::shard_local_cfg().kafka_admin_topic_api_rate.bind())
  , _target_produce_tp_rate_per_client_group(
      config::shard_local_cfg().kafka_client_group_byte_rate_quota.bind())
  , _target_fetch_tp_rate_per_client_group(
      config::shard_local_cfg().kafka_client_group_fetch_byte_rate_quota.bind())
  , _client_quotas{client_quotas}
  , _gc_freq(config::shard_local_cfg().quota_manager_gc_sec())
  , _max_delay(config::shard_local_cfg().max_kafka_throttle_delay_ms.bind()) {
    if (seastar::this_shard_id() == _client_quotas.shard_id()) {
        _gc_timer.set_callback([this]() { gc(); });
    }
}

quota_manager::~quota_manager() { _gc_timer.cancel(); }

ss::future<> quota_manager::stop() {
    _gc_timer.cancel();
    co_await _gate.close();
}

ss::future<> quota_manager::start() {
    if (ss::this_shard_id() == _client_quotas.shard_id()) {
        co_await _client_quotas.reset(client_quotas_map_t{});
        _gc_timer.arm_periodic(_gc_freq);
    }
}

/// Returns the client_quota corresponding to the given quota_id.
/// It uses a backing map to keep track of the recently seen client_quota's.
/// It bumps the last_seen_ms field of client_quota for gc() tracking.
/// It may return a std::nullopt in a rare case when there are concurrent
/// updated to the map here and in gc().
ss::future<
  std::optional<quota_manager::value_node<quota_manager::client_quota>>>
quota_manager::maybe_add_and_retrieve_quota(
  std::optional<std::string_view> quota_id, clock::time_point now) {
    // requests without a client id are grouped into an anonymous group that
    // shares a default quota. the anonymous group is keyed on empty string.
    auto qid = quota_id.value_or("");

    vassert(_client_quotas, "_client_quotas should have been initialized");

    auto it = _client_quotas->find(qid);
    auto inserted = false;
    if (it == _client_quotas->end()) {
        co_await container().invoke_on(
          _client_quotas.shard_id(),
          [qid, now](quota_manager& me) { return me.add_quota_id(qid, now); });

        inserted = true;
        it = _client_quotas->find(qid);
        if (it == _client_quotas->end()) {
            // The newly inserted quota map entry should always be available
            // here because the update to the map is guarded by a mutex, so
            // there is no chance of losing updates. There is a low chance that
            // we insert into the map, then there is an unusually long pause on
            // the handler core, gc() gets scheduled and cleans up the inserted
            // quota, and by the time the handler gets here the inserted entry
            // is gone. In that case, we give up and don't throttle.
            vlog(klog.debug, "Failed to find quota id after insert...");
            co_return std::nullopt;
        }
    }

    // bump to prevent gc
    if (!inserted) {
        it->second->last_seen_ms.local() = now;
    }

    co_return it->second;
}

ss::future<>
quota_manager::add_quota_id(std::string_view qid, clock::time_point now) {
    vassert(
      ss::this_shard_id() == _client_quotas.shard_id(),
      "add_quota_id should only be called on the owner shard");

    auto update_func = [this, qid = ss::sstring{qid}, now](
                         client_quotas_map_t new_map) -> client_quotas_map_t {
        auto produce_rate = get_client_target_produce_tp_rate(qid);
        auto fetch_rate = get_client_target_fetch_tp_rate(qid);
        auto partition_mutation_rate = *_target_partition_mutation_quota();
        auto default_num_windows = static_cast<uint32_t>(
          _default_num_windows());
        auto default_window_width = _default_window_width();
        auto replenish_threshold = static_cast<uint64_t>(
          _replenish_threshold().value_or(1));

        auto new_value = make_value_node<client_quota>(
          ssx::sharded_value<clock::time_point>(now),
          std::nullopt,
          std::nullopt,
          /// pm_rate is only accessed on the qm home shard
          token_bucket_rate_tracker{
            partition_mutation_rate,
            default_num_windows,
            default_window_width});

        new_value->tp_produce_rate.emplace(
          produce_rate, produce_rate, replenish_threshold, true);
        if (fetch_rate.has_value()) {
            new_value->tp_fetch_rate.emplace(
              *fetch_rate, *fetch_rate, replenish_threshold, true);
        }

        new_map.try_emplace(std::move(qid), std::move(new_value));

        return new_map;
    };

    co_await _client_quotas.update(std::move(update_func));
}

// If client is part of some group then client quota ID is a group
// else client quota ID is client_id
static std::optional<std::string_view> get_client_quota_id(
  const std::optional<std::string_view>& client_id,
  const std::unordered_map<ss::sstring, config::client_group_quota>&
    group_quota) {
    if (!client_id) {
        return std::nullopt;
    }
    for (const auto& group_and_limit : group_quota) {
        if (client_id->starts_with(
              std::string_view(group_and_limit.second.clients_prefix))) {
            return group_and_limit.first;
        }
    }
    return client_id;
}

int64_t quota_manager::get_client_target_produce_tp_rate(
  const std::optional<std::string_view>& quota_id) {
    if (!quota_id) {
        return _default_target_produce_tp_rate();
    }
    auto group_tp_rate = _target_produce_tp_rate_per_client_group().find(
      ss::sstring(quota_id.value()));
    if (group_tp_rate != _target_produce_tp_rate_per_client_group().end()) {
        return group_tp_rate->second.quota;
    }
    return _default_target_produce_tp_rate();
}

std::optional<int64_t> quota_manager::get_client_target_fetch_tp_rate(
  const std::optional<std::string_view>& quota_id) {
    if (!quota_id) {
        return _default_target_fetch_tp_rate();
    }
    auto group_tp_rate = _target_fetch_tp_rate_per_client_group().find(
      ss::sstring(quota_id.value()));
    if (group_tp_rate != _target_fetch_tp_rate_per_client_group().end()) {
        return group_tp_rate->second.quota;
    }
    return _default_target_fetch_tp_rate();
}

ss::future<std::chrono::milliseconds> quota_manager::record_partition_mutations(
  std::optional<std::string_view> client_id,
  uint32_t mutations,
  clock::time_point now) {
    /// KIP-599 throttles create_topics / delete_topics / create_partitions
    /// request. This delay should only be applied to these requests if the
    /// quota has been exceeded
    if (!_target_partition_mutation_quota()) {
        co_return 0ms;
    }
    co_return co_await container().invoke_on(
      quota_manager_shard, [client_id, mutations, now](quota_manager& qm) {
          return qm.do_record_partition_mutations(client_id, mutations, now);
      });
}

ss::future<std::chrono::milliseconds>
quota_manager::do_record_partition_mutations(
  std::optional<std::string_view> client_id,
  uint32_t mutations,
  clock::time_point now) {
    vassert(
      ss::this_shard_id() == quota_manager_shard,
      "This method can only be executed from quota manager home shard");

    auto quota_id = get_client_quota_id(client_id, {});
    auto cq = co_await maybe_add_and_retrieve_quota(quota_id, now);
    if (!cq) {
        co_return std::chrono::milliseconds::zero();
    }
    const auto units = cq.value()->pm_rate->record_and_measure(mutations, now);
    auto delay_ms = 0ms;
    if (units < 0) {
        /// Throttle time is defined as -K * R, where K is the number of
        /// tokens in the bucket and R is the avg rate. This only works when
        /// the number of tokens are negative which is the case when the
        /// rate limiter recommends throttling
        const auto rate = (units * -1)
                          * std::chrono::seconds(
                            *_target_partition_mutation_quota());
        delay_ms = std::chrono::duration_cast<std::chrono::milliseconds>(rate);
        std::chrono::milliseconds max_delay_ms(_max_delay());
        if (delay_ms > max_delay_ms) {
            vlog(
              klog.info,
              "Found partition mutation rate for window of: {}. Client:{}, "
              "Estimated backpressure delay of {}. Limiting to {} backpressure "
              "delay",
              rate,
              quota_id,
              delay_ms,
              max_delay_ms);
            delay_ms = max_delay_ms;
        }
    }
    co_return delay_ms;
}

std::chrono::milliseconds quota_manager::cap_to_max_delay(
  std::optional<std::string_view> quota_id,
  std::chrono::milliseconds delay_ms) {
    std::chrono::milliseconds max_delay_ms(_max_delay());
    if (delay_ms > max_delay_ms) {
        vlog(
          klog.info,
          "Client:{}, "
          "Estimated "
          "backpressure delay of {}. Limiting to {} backpressure delay",
          quota_id,
          delay_ms,
          max_delay_ms);
        delay_ms = max_delay_ms;
    }
    return delay_ms;
}

// record a new observation and return <previous delay, new delay>
ss::future<throttle_delay> quota_manager::record_produce_tp_and_throttle(
  std::optional<std::string_view> client_id,
  uint64_t bytes,
  clock::time_point now) {
    auto quota_id = get_client_quota_id(
      client_id, _target_produce_tp_rate_per_client_group());
    auto cq = co_await maybe_add_and_retrieve_quota(quota_id, now);

    if (!cq.has_value() || !cq.value()->tp_produce_rate.has_value()) {
        co_return throttle_delay{};
    }

    auto& produce_tracker = cq.value()->tp_produce_rate.value();
    auto delay_ms
      = produce_tracker.update_and_calculate_delay<std::chrono::milliseconds>(
        now, bytes);
    auto capped_delay_ms = cap_to_max_delay(quota_id, delay_ms);
    co_return throttle_delay{.duration = capped_delay_ms};
}

ss::future<> quota_manager::record_fetch_tp(
  std::optional<std::string_view> client_id,
  uint64_t bytes,
  clock::time_point now) {
    auto quota_id = get_client_quota_id(
      client_id, _target_fetch_tp_rate_per_client_group());
    auto cq = co_await maybe_add_and_retrieve_quota(quota_id, now);
    if (!cq.has_value() || !cq.value()->tp_fetch_rate.has_value()) {
        co_return;
    }
    auto& fetch_tracker = cq.value()->tp_fetch_rate.value();
    fetch_tracker.record(bytes);
}

ss::future<throttle_delay> quota_manager::throttle_fetch_tp(
  std::optional<std::string_view> client_id, clock::time_point now) {
    auto quota_id = get_client_quota_id(
      client_id, _target_fetch_tp_rate_per_client_group());
    auto cq = co_await maybe_add_and_retrieve_quota(quota_id, now);

    if (!cq.has_value() || !cq.value()->tp_fetch_rate.has_value()) {
        co_return throttle_delay{};
    }

    auto& fetch_tracker = cq.value()->tp_fetch_rate.value();
    auto delay_ms
      = fetch_tracker.update_and_calculate_delay<std::chrono::milliseconds>(
        now);
    auto capped_delay_ms = cap_to_max_delay(quota_id, delay_ms);
    co_return throttle_delay{.duration = capped_delay_ms};
}

// erase inactive tracked quotas. windows are considered inactive if
// they have not received any updates in ten window's worth of time.
void quota_manager::gc() {
    vassert(
      ss::this_shard_id() == _client_quotas.shard_id(),
      "gc should only be performed on the owner shard");
    auto full_window = _default_num_windows() * _default_window_width();
    auto expire_threshold = clock::now() - 10 * full_window;
    ssx::background
      = ssx::spawn_with_gate_then(_gate, [this, expire_threshold]() {
            return do_gc(expire_threshold);
        }).handle_exception([](const std::exception_ptr& e) {
            vlog(klog.warn, "Error garbage collecting quotas - {}", e);
        });
}

ss::future<> quota_manager::do_gc(clock::time_point expire_threshold) {
    vassert(
      ss::this_shard_id() == _client_quotas.shard_id(),
      "do_gc() should only be called on the owner shard");

    using key_set = chunked_vector<ss::sstring>;

    auto mapper = [expire_threshold](const quota_manager& qm) -> key_set {
        auto res = key_set{};
        auto map_shared_ptr = qm._client_quotas.local().get();
        for (const auto& kv : *map_shared_ptr) {
            auto last_seen_tp = kv.second->last_seen_ms.local();
            if (last_seen_tp < expire_threshold) {
                res.push_back(kv.first);
            }
        }
        // Note: need to pre-sort the vector for the std::set_intersection
        // in the reduce step
        std::sort(res.begin(), res.end());
        return res;
    };

    auto reducer = [](const key_set& acc, const key_set& next) -> key_set {
        key_set result;
        // Note: std::set_intersection assumes the inputs are sorted and
        // guarantees that the output is sorted
        std::set_intersection(
          acc.begin(),
          acc.end(),
          next.begin(),
          next.end(),
          std::back_inserter(result));
        return result;
    };

    auto expired_keys = co_await container().map_reduce0(
      mapper, key_set{}, reducer);

    if (expired_keys.empty()) {
        // Nothing to gc, so we're done
        co_return;
    }

    // Note: it is possible that we remove client ids here that we have not seen
    // for a long time before the map_reduce step, but that we see again between
    // the map_reduce step and this map update. This race between the two steps
    // can cause recently seen client ids to be deleted. That's acceptable
    // because this should be rare and the client will be tracked correctly
    // again from when we next see it.
    co_await _client_quotas.update(
      [expired_keys{std::move(expired_keys)}](
        client_quotas_map_t new_map) -> ss::future<client_quotas_map_t> {
          for (auto& k : expired_keys) {
              new_map.erase(k);
          }
          co_return new_map;
      });
}

} // namespace kafka

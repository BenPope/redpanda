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

#pragma once
#include "base/seastarx.h"
#include "config/client_group_byte_rate_quota.h"
#include "config/property.h"
#include "kafka/server/atomic_token_bucket.h"
#include "kafka/server/token_bucket_rate_tracker.h"
#include "ssx/sharded_ptr.h"
#include "ssx/sharded_value.h"
#include "utils/absl_sstring_hash.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/timer.hh>
#include <seastar/util/shared_token_bucket.hh>

#include <absl/container/node_hash_map.h>

#include <chrono>
#include <memory>
#include <optional>
#include <string_view>

namespace kafka {

// Shard on which partition mutation rate metrics are aggregated on
static constexpr ss::shard_id quota_manager_shard = 0;

// quota_manager tracks quota usage
//
// TODO:
//   - we will want to eventually add support for configuring the quotas and
//   quota settings as runtime through the kafka api and other mechanisms.
//
//   - currently only total throughput per client_id is tracked. in the future
//   we will want to support additional quotas and accouting granularities to be
//   at parity with kafka. for example:
//
//      - splitting out rates separately for produce and fetch
//      - accounting per user vs per client (these are separate in kafka)
//
//   - it may eventually be beneficial to periodically reduce stats across
//   shards or track stats globally to produce a more accurate per-node
//   representation of a statistic (e.g. bandwidth).
//
class quota_manager : public ss::peering_sharded_service<quota_manager> {
public:
    using clock = ss::lowres_clock;

    struct throttle_delay {
        clock::duration duration{0};
    };

    using bucket_t = atomic_token_bucket<ss::internal::capped_release::yes>;

    /// A thread-safe reference wrapper to help safely share a thread-safe value
    /// across cores. It is safe to hold on to this reference across scheduling
    /// points. This wrapped ensures that deallocation happens on the core that
    /// created this wrapper.
    template<typename T>
    class value_node {
    public:
        value_node(std::unique_ptr<T> unq)
          : _value{std::make_shared<ss::foreign_ptr<std::unique_ptr<T>>>(
            std::move(unq))} {}

        /// Takes a non-owning reference to the wrapped value
        /// The lifetime of the reference is at least the lifetime of the node.
        T& operator*() const { return &*_value.get()->get(); }

        /// Takes a non-owning pointer to the wrapped value
        /// The lifetime of the pointer is at least the lifetime of the node.
        T* operator->() const { return _value.get()->get(); }

    private:
        // TODO: replace std::shared_ptr with ssx::sharded_ptr to reduce
        // cross-core contention on the reference count
        // Note: the unique_ptr is only needed because ss::foreign_ptr expects
        // to wrap a pointer type
        std::shared_ptr<ss::foreign_ptr<std::unique_ptr<T>>> _value;
    };

    template<typename T, typename... Args>
    value_node<T> make_value_node(Args&&... args) {
        return value_node<T>(std::make_unique<T>(std::forward<Args>(args)...));
    }

    // Accounting for quota on per-client and per-client-group basis
    // last_seen: used for gc keepalive
    // tp_rate: throughput tracking
    // pm_rate: partition mutation quota tracking - only on home shard
    struct client_quota {
        ssx::sharded_value<clock::time_point> last_seen_ms;
        // TODO: do we need to pad these?
        std::optional<bucket_t> tp_produce_rate;
        std::optional<bucket_t> tp_fetch_rate;
        std::optional<token_bucket_rate_tracker> pm_rate;
    };

    using client_quotas_map_t = absl::node_hash_map<
      ss::sstring,
      value_node<client_quota>,
      sstring_hash,
      sstring_eq>;
    using client_quotas_t = ssx::sharded_ptr<client_quotas_map_t>;

    explicit quota_manager(client_quotas_t& client_quotas);
    quota_manager(const quota_manager&) = delete;
    quota_manager& operator=(const quota_manager&) = delete;
    quota_manager(quota_manager&&) = delete;
    quota_manager& operator=(quota_manager&&) = delete;
    ~quota_manager();

    ss::future<> stop();

    ss::future<> start();

    // record a new observation
    ss::future<throttle_delay> record_produce_tp_and_throttle(
      std::optional<std::string_view> client_id,
      uint64_t bytes,
      clock::time_point now = clock::now());

    // record a new observation
    ss::future<> record_fetch_tp(
      std::optional<std::string_view> client_id,
      uint64_t bytes,
      clock::time_point now = clock::now());

    ss::future<throttle_delay> throttle_fetch_tp(
      std::optional<std::string_view> client_id,
      clock::time_point now = clock::now());

    // Used to record new number of partitions mutations
    // Only for use with the quotas introduced by KIP-599, namely to track
    // partition creation and deletion events (create topics, delete topics &
    // create partitions)
    //
    // NOTE: This method will be invoked on shard 0, therefore ensure that it is
    // not called within a tight loop from another shard
    ss::future<std::chrono::milliseconds> record_partition_mutations(
      std::optional<std::string_view> client_id,
      uint32_t mutations,
      clock::time_point now = clock::now());

private:
    ss::future<std::chrono::milliseconds> do_record_partition_mutations(
      std::optional<std::string_view> client_id,
      uint32_t mutations,
      clock::time_point now);

    std::chrono::milliseconds cap_to_max_delay(
      std::optional<std::string_view> client_id,
      std::chrono::milliseconds delay_ms);

private:
    // erase inactive tracked quotas. windows are considered inactive if they
    // have not received any updates in ten window's worth of time.
    void gc();
    ss::future<> do_gc(clock::time_point expire_threshold);

    ss::future<std::optional<value_node<quota_manager::client_quota>>>
    maybe_add_and_retrieve_quota(
      std::optional<std::string_view>, const clock::time_point);
    ss::future<> add_quota_id(std::string_view quota_id, clock::time_point now);
    int64_t get_client_target_produce_tp_rate(
      const std::optional<std::string_view>& quota_id);
    std::optional<int64_t> get_client_target_fetch_tp_rate(
      const std::optional<std::string_view>& quota_id);

private:
    config::binding<int16_t> _default_num_windows;
    config::binding<std::chrono::milliseconds> _default_window_width;
    config::binding<std::optional<int64_t>> _replenish_threshold;

    config::binding<uint32_t> _default_target_produce_tp_rate;
    config::binding<std::optional<uint32_t>> _default_target_fetch_tp_rate;
    config::binding<std::optional<uint32_t>> _target_partition_mutation_quota;
    config::binding<std::unordered_map<ss::sstring, config::client_group_quota>>
      _target_produce_tp_rate_per_client_group;
    config::binding<std::unordered_map<ss::sstring, config::client_group_quota>>
      _target_fetch_tp_rate_per_client_group;

    client_quotas_t& _client_quotas;

    ss::timer<> _gc_timer;
    clock::duration _gc_freq;
    config::binding<std::chrono::milliseconds> _max_delay;
    ss::gate _gate;
};

} // namespace kafka

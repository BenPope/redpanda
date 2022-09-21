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

#include "pandaproxy/kafka_client_cache.h"

#include "pandaproxy/logger.h"
#include "random/generators.h"
#include "ssx/future-util.h"

#include <seastar/core/loop.hh>

namespace pandaproxy {

kafka_client_cache::kafka_client_cache(
  YAML::Node const& cfg, size_t max_size, std::chrono::milliseconds keep_alive)
  : _config{cfg}
  , _cache_max_size{max_size}
  , _keep_alive{keep_alive} {}

client_ptr kafka_client_cache::make_client(
  credential_t user, config::rest_authn_method authn_method) {
    kafka::client::configuration cfg{
      to_yaml(_config, config::redact_secrets::no)};

    // Set the principal when the request is using HTTP Basic AuthN
    if (authn_method == config::rest_authn_method::http_basic) {
        // Need to specify type or else bad any_cast runtime error
        cfg.sasl_mechanism.set_value(ss::sstring{"SCRAM-SHA-256"});
        cfg.scram_username.set_value(std::move(user.name));
        cfg.scram_password.set_value(std::move(user.pass));
    }

    return ss::make_lw_shared<kafka::client::client>(
      to_yaml(cfg, config::redact_secrets::no));
}

ss::future<client_ptr> kafka_client_cache::fetch_or_insert(
  credential_t user, config::rest_authn_method authn_method) {
    auto& inner_list = _cache.get<underlying_list>();
    auto& inner_hash = _cache.get<underlying_hash>();
    ss::sstring k{user.name};
    auto it_hash = inner_hash.find(k);

    client_ptr client;

    // When no client is found ...
    if (it_hash == inner_hash.end()) {
        if (_cache.size() >= _cache_max_size) {
            if (!_cache.empty()) {
                auto item = inner_list.back();
                vlog(plog.debug, "Cache size reached, evicting {}", item.key);
                inner_list.pop_back();
                _evicted_items.push_back(item);
            }
        }

        vlog(plog.debug, "Make client for user {}", k);

        client = make_client(user, authn_method);
        inner_list.emplace_front(k, client);
    } else {
        // Make a new client if the passwords don't match
        if (it_hash->client->config().scram_password.value() != user.pass) {
            vlog(plog.debug, "Password changed. Remake client for user {}", k);
            client_ptr old = it_hash->client;
            client_ptr next = make_client(user, authn_method);
            co_await _lock.with([k, next] {
                // Try connect as a safety check
                return next->connect().handle_exception(
                  [k](std::exception_ptr ex) {
                      // The failed connection is dropped
                      vlog(
                        plog.debug,
                        "Connection for {} already formed. Dropping dup: {}",
                        k,
                        ex);
                  });
            });

            inner_hash.modify(
              it_hash, [next](timestamped_user& item) { item.client = next; });

            // Add a prefix to the evicted key so debugging logs
            // is easier
            _evicted_items.emplace_back(
              k + random_generators::gen_alphanum_string(8), old);
        } else {
            vlog(plog.debug, "Reuse client for user {}", k);
        }

        client = it_hash->client;

        // Convert hash iterator to list iterator
        auto it_list = _cache.project<underlying_list>(it_hash);

        // Update the timestamp and relocate the element
        // to the front
        inner_list.modify(it_list, [](timestamped_user& item) {
            item.last_used = timestamped_user::clock::now();
        });
        inner_list.relocate(inner_list.begin(), it_list);
    }

    co_return client;
}

namespace {
template<typename List_Type>
ss::future<> clean_clients_from_list(
  List_Type& list, mutex& lock, std::chrono::milliseconds keep_alive) {
    auto has_expired = timestamped_user::clock::now();
    auto condition = [keep_alive, has_expired](const timestamped_user& item) {
        return has_expired >= (item.last_used + keep_alive);
    };

    auto first = list.begin();
    auto last = list.end();
    while (first != last) {
        if (condition(*first)) {
            vlog(plog.debug, "Remove {} from cache", first->key);
            // Stop client before erase
            co_await lock.with([&first] {
                return first->client->stop().handle_exception(
                  [](std::exception_ptr ex) {
                      // The stop failed
                      vlog(
                        plog.debug,
                        "Stale client stop already happened {}",
                        ex);
                  });
            });
            list.erase(first++);
        } else {
            ++first;
        }
    }
}

template<typename List_Type>
ss::future<> stop_clients_until_empty(List_Type& list, mutex& lock) {
    while (!list.empty()) {
        co_await lock.with([&list] {
            return list.back().client->stop().handle_exception(
              [](std::exception_ptr ex) {
                  // The stop failed
                  vlog(plog.debug, "Stop already happened {}", ex);
              });
        });
        list.pop_back();
    }
}
} // namespace

// Modified boost remove_if so we can call kafka::client::stop
// See sequenced_index_remove in
// https://github.com/boostorg/multi_index/blob/develop/include/boost/multi_index/detail/seq_index_ops.hpp
void kafka_client_cache::clean_stale_clients() {
    auto& inner_list = _cache.get<underlying_list>();
    ssx::background = clean_clients_from_list(inner_list, _lock, _keep_alive);
    ssx::background = clean_clients_from_list(
      _evicted_items, _lock, _keep_alive);
}

ss::future<> kafka_client_cache::stop() {
    auto& inner_list = _cache.get<underlying_list>();
    ssx::background = stop_clients_until_empty(inner_list, _lock);
    ssx::background = stop_clients_until_empty(_evicted_items, _lock);

    // Must return future<void> because sharded::stop expects a return.
    // See sharded_call_stop<true>::call in seastar/core/sharded.hh
    return ss::now();
}

size_t kafka_client_cache::size() const { return _cache.size(); }
size_t kafka_client_cache::max_size() const { return _cache_max_size; }

} // namespace pandaproxy

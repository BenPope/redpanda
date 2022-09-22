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

namespace pandaproxy {

kafka_client_cache::kafka_client_cache(
  YAML::Node const& cfg,
  config::binding<size_t> max_size,
  config::binding<std::chrono::milliseconds> keep_alive)
  : _config{cfg}
  , _cache_max_size{std::move(max_size)}
  , _keep_alive{std::move(keep_alive)} {}

client_ptr kafka_client_cache::make_client(
  credential_t user, config::rest_authn_method authn_method) {
    kafka::client::configuration cfg{
      to_yaml(_config, config::redact_secrets::no)};

    // Set the principal when the request is using HTTP Basic AuthN
    if (authn_method == config::rest_authn_method::http_basic) {
        // Need to specify type or else bad any_cast runtime error
        cfg.sasl_mechanism.set_value(ss::sstring{"SCRAM-SHA-256"});
        cfg.scram_username.set_value(user.name);
        cfg.scram_password.set_value(user.pass);
    }

    return ss::make_lw_shared<kafka::client::client>(
      to_yaml(cfg, config::redact_secrets::no));
}

client_ptr kafka_client_cache::fetch_or_insert(
  credential_t user, config::rest_authn_method authn_method) {
    auto& inner_list = _cache.get<underlying_list>();
    auto& inner_hash = _cache.get<underlying_hash>();
    auto it_hash = inner_hash.find(user.name);

    client_ptr client;

    // When no client is found ...
    if (it_hash == inner_hash.end()) {
        if (_cache.size() >= _cache_max_size()) {
            if (!_cache.empty()) {
                inner_list.pop_back();
            }
        }

        vlog(plog.debug, "Make client for user {}", user.name);

        client = make_client(user, authn_method);
        inner_list.emplace_front(user.name, client);
    } else {
        // Otherwise the user has a client
        vlog(plog.debug, "Reuse client for user {}", user.name);

        // Grab the node located at that position
        auto node = inner_hash.extract(it_hash);

        // Update the timestamp and put it to the front
        node.value().last_used = timestamped_user::clock::now();
        client = node.value().client;
        inner_list.insert(inner_list.begin(), std::move(node));
    }

    return client;
}

void kafka_client_cache::clean_stale_clients() {
    auto& inner_list = _cache.get<underlying_list>();
    auto now = timestamped_user::clock::now();
    inner_list.remove_if([this, now](const timestamped_user& item) {
        bool remove = (item.last_used + _keep_alive()) >= now;
        if (remove) {
            vlog(plog.debug, "Removed {} from cache", item.username);
        }
        return remove;
    });
}

size_t kafka_client_cache::size() const { return _cache.size(); }
size_t kafka_client_cache::max_size() const { return _cache_max_size(); }

} // namespace pandaproxy

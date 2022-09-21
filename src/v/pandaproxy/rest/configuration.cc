// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "configuration.h"

#include "config/base_property.h"
#include "config/configuration.h"
#include "config/endpoint_tls_config.h"
#include "model/metadata.h"
#include "units.h"

#include <chrono>

namespace pandaproxy::rest {
using namespace std::chrono_literals;

configuration::configuration(const YAML::Node& cfg)
  : configuration() {
    read_yaml(cfg);
}

configuration::configuration()
  : pandaproxy_api(
    *this,
    "pandaproxy_api",
    "Rest API listen address and port",
    {},
    {config::rest_authn_endpoint{
      .address = net::unresolved_address("0.0.0.0", 8082),
      .authn_method = std::nullopt}})
  , pandaproxy_api_tls(
      *this,
      "pandaproxy_api_tls",
      "TLS configuration for Pandaproxy api",
      {},
      {},
      config::endpoint_tls_config::validate_many)
  , advertised_pandaproxy_api(
      *this,
      "advertised_pandaproxy_api",
      "Rest API address and port to publish to client")
  , api_doc_dir(
      *this,
      "api_doc_dir",
      "API doc directory",
      {},
      "/usr/share/redpanda/proxy-api-doc")
  , consumer_instance_timeout(
      *this,
      "consumer_instance_timeout_ms",
      "How long to wait for an idle consumer before removing it",
      {},
      std::chrono::minutes{5})
  , client_cache_max_size(
      *this,
      "client_cache_max_size",
      "The maximum number of kafka clients in the LRU cache",
      {.needs_restart = config::needs_restart::no},
      10,
      [](const int64_t& max_size) {
          std::optional<ss::sstring> msg{std::nullopt};
          if (max_size <= 0) {
              msg = ss::sstring{"Client cache max size must be greater than 0"};
          }
          return msg;
      })
  , client_keep_alive(
      *this,
      "client_keep_alive",
      "Time in milliseconds that an idle connection may remain open",
      {.needs_restart = config::needs_restart::no, .example = "30000"},
      (30000ms).count(),
      [](const int64_t& keep_alive) {
          std::optional<ss::sstring> msg{std::nullopt};
          if (keep_alive <= 0) {
              msg = ss::sstring{"Client keep alive must be greater than 0"};
          }
          return msg;
      }) {}

config::rest_authn_method configuration::authn_method(size_t listener_idx) {
    auto pp_api = pandaproxy_api.value();

    if (listener_idx < 0 || listener_idx >= pp_api.size()) {
        return config::rest_authn_method::none;
    }

    return pp_api[listener_idx].authn_method.value_or(
      config::rest_authn_method::none);
}

} // namespace pandaproxy::rest

// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/service.h"

#include "pandaproxy/api/api-doc/schema_registry.json.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/schema_registry/configuration.h"
#include "pandaproxy/schema_registry/handlers.h"
#include "utils/gate_guard.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/std-coroutine.hh>
#include <seastar/http/api_docs.hh>

namespace pandaproxy::schema_registry {

using server = ctx_server<service>;

template<typename Handler>
auto wrap(ss::gate& g, one_shot& es, Handler h) {
    return [&g, &es, h{std::move(h)}](
             server::request_t rq,
             server::reply_t rp) -> ss::future<server::reply_t> {
        co_await es();
        auto guard = gate_guard(g);
        co_return co_await h(std::move(rq), std::move(rp));
    };
}

server::routes_t get_schema_registry_routes(ss::gate& gate, one_shot& es) {
    server::routes_t routes;
    routes.api = ss::httpd::schema_registry_json::name;

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::get_schemas_types,
      wrap(gate, es, get_schemas_types)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::get_schemas_ids_id,
      wrap(gate, es, get_schemas_ids_id)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::get_subjects,
      wrap(gate, es, get_subjects)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::get_subject_versions,
      wrap(gate, es, get_subject_versions)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::post_subject_versions,
      wrap(gate, es, post_subject_versions)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::get_subject_versions_version,
      wrap(gate, es, get_subject_versions_version)});

    return routes;
}

ss::future<> service::do_start() {
    auto guard = gate_guard(_gate);
    try {
        return ss::now();
        vlog(plog.error, "service::ensure_started() - success");
    } catch (...) {
        vlog(
          plog.error,
          "Schema registry failed to initialize internal topic: {}",
          std::current_exception());
        throw;
    }
}

service::service(
  const YAML::Node& config,
  ss::smp_service_group smp_sg,
  size_t max_memory,
  ss::sharded<kafka::client::client>& client)
  : _config(config)
  , _mem_sem(max_memory)
  , _client(client)
  , _ctx{{{}, _mem_sem, {}, smp_sg}, *this}
  , _server(
      "schema_registry",
      ss::api_registry_builder20(_config.api_doc_dir(), "/v1"),
      "schema_registry_header",
      "/definitions",
      _ctx)
  , _ensure_started{[this]() { return do_start(); }} {}

ss::future<> service::start() {
    static std::vector<model::broker_endpoint> not_advertised{};
    _server.routes(get_schema_registry_routes(_gate, _ensure_started));
    return _server.start(
      _config.schema_registry_api(),
      _config.schema_registry_api_tls(),
      not_advertised);
}

ss::future<> service::stop() {
    co_await _gate.close();
    co_await _server.stop();
}

configuration& service::config() { return _config; }

kafka::client::configuration& service::client_config() {
    return _client.local().config();
}

} // namespace pandaproxy::schema_registry

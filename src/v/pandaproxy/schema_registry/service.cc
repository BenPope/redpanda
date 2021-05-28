// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/service.h"

#include "kafka/protocol/create_topics.h"
#include "kafka/protocol/list_offsets.h"
#include "kafka/server/handlers/topics/types.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "pandaproxy/api/api-doc/schema_registry.json.h"
#include "pandaproxy/error.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/schema_registry/configuration.h"
#include "pandaproxy/schema_registry/handlers.h"
#include "pandaproxy/schema_registry/storage.h"
#include "pandaproxy/schema_registry/util.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/std-coroutine.hh>
#include <seastar/http/api_docs.hh>

namespace pandaproxy::schema_registry {

using server = ctx_server<service>;

static const model::topic_partition schemas_tp{
  model::topic{"_schemas"}, model::partition_id{0}};

template<typename Handler>
auto wrap_with_gate(ss::gate& g, Handler h) {
    return
      [&g, h{std::move(h)}](server::request_t rq, server::reply_t rp) mutable {
          return ss::with_gate(
            g, [&h, rq{std::move(rq)}, rp{std::move(rp)}]() mutable {
                return h(std::move(rq), std::move(rp));
            });
      };
}

server::routes_t get_schema_registry_routes(ss::gate& gate) {
    server::routes_t routes;
    routes.api = ss::httpd::schema_registry_json::name;

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::get_schemas_types,
      wrap_with_gate(gate, get_schemas_types)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::get_schemas_ids_id,
      wrap_with_gate(gate, get_schemas_ids_id)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::get_subjects,
      wrap_with_gate(gate, get_subjects)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::get_subject_versions,
      wrap_with_gate(gate, get_subject_versions)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::post_subject_versions,
      wrap_with_gate(gate, post_subject_versions)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::get_subject_versions_version,
      wrap_with_gate(gate, get_subject_versions_version)});

    return routes;
}

ss::future<> service::create_internal_topic() {
    vlog(plog.debug, "Schema registry: attempting to create internal topic");
    static constexpr auto make_schema_topic_req = []() {
        return kafka::create_topics_request{.data{.topics{
          {.name{schemas_tp.topic},
           .num_partitions = 1,     // TODO(Ben): Make configurable
           .replication_factor = 1, // TODO(Ben): Make configurable
           .assignments{},
           .configs{
             {.name{ss::sstring{kafka::topic_property_cleanup_policy}},
              .value{"compact"}}}}}}};
    };
    auto res = co_await _client.local().dispatch(make_schema_topic_req);
    if (res.data.topics.size() != 1) {
        throw std::runtime_error("Unexpected topic count");
    }

    const auto& topic = res.data.topics[0];
    if (topic.error_code == kafka::error_code::none) {
        vlog(plog.debug, "Schema registry: created internal topic");
    } else if (topic.error_code == kafka::error_code::topic_already_exists) {
        vlog(plog.debug, "Schema registry: found internal topic");
    } else if (topic.error_code == kafka::error_code::not_controller) {
        vlog(plog.debug, "Schema registry: not controller");
    } else {
        throw kafka::exception(
          topic.error_code,
          topic.error_message.value_or(
            kafka::make_error_code(topic.error_code).message()));
    }

    // TODO(Ben): Validate the _schemas topic
}

ss::future<> service::fetch_internal_topic() {
    constexpr static auto offset_req = []() {
        return kafka::list_offsets_request{
          .data = {.topics{
            {{.name{schemas_tp.topic},
              .partitions{
                {{.partition_index{schemas_tp.partition},
                  .max_num_offsets = 128}}}}}}}};
    };
    auto offset_res = co_await _client.local().dispatch(offset_req);
    auto max_offset = offset_res.data.topics[0].partitions[0].offset;
    vlog(plog.debug, "Schema registry: _schemas max_offset: {}", max_offset);

    client_fetcher reader{
      _client.local(), schemas_tp, model::offset{0}, max_offset};
    consume_to_store consumer{_store};

    co_await ss::do_until(
      [this, &reader]() {
          return _gate.is_closed() || reader.is_end_of_stream();
      },
      [&consumer, &reader]() {
          return std::move(reader).consume(consumer, model::no_timeout);
      });
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
      _ctx) {}

ss::future<> service::start() {
    static std::vector<model::broker_endpoint> not_advertised{};
    _server.routes(get_schema_registry_routes(_gate));
    co_await _server.start(
      _config.schema_registry_api(),
      _config.schema_registry_api_tls(),
      not_advertised);
    (void)ss::with_gate(_gate, [this]() -> ss::future<> {
        try {
            co_await create_internal_topic();
            co_await fetch_internal_topic();
        } catch (...) {
            vlog(
              plog.error,
              "Schema registry failed to initialize internal topic: {}",
              std::current_exception());
        }
    });
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

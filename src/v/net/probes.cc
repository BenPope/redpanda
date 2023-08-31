// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/configuration.h"
#include "net/client_probe.h"
#include "net/server_probe.h"
#include "net/tls_credentials_probe.h"
#include "net/types.h"
#include "prometheus/prometheus_sanitize.h"
#include "ssx/metrics.h"
#include "ssx/sformat.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/smp.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/tls.hh>
#include <seastar/util/defer.hh>

#include <fmt/chrono.h>
#include <fmt/ranges.h>
#include <gnutls/gnutls.h>
#include <gnutls/gnutlsxx.h>
#include <gnutls/x509-ext.h>
#include <gnutls/x509.h>

#include <chrono>
#include <ostream>
#include <span>

namespace net {
void server_probe::setup_metrics(
  ssx::metrics::metric_groups& mgs, std::string_view proto) {
    namespace sm = ss::metrics;
    auto aggregate_labels = config::shard_local_cfg().aggregate_metrics()
                              ? std::vector<sm::label>{sm::shard_label}
                              : std::vector<sm::label>{};
    mgs.add_group(
      prometheus_sanitize::metrics_name(ss::sstring{proto}),
      {
        sm::make_gauge(
          "active_connections",
          [this] { return _connections; },
          sm::description(
            ssx::sformat("{}: Currently active connections", proto)))
          .aggregate(aggregate_labels),
        sm::make_counter(
          "connects",
          [this] { return _connects; },
          sm::description(
            ssx::sformat("{}: Number of accepted connections", proto)))
          .aggregate(aggregate_labels),
        sm::make_counter(
          "connection_close_errors",
          [this] { return _connection_close_error; },
          sm::description(ssx::sformat(
            "{}: Number of errors when shutting down the connection", proto)))
          .aggregate(aggregate_labels),
        sm::make_counter(
          "connections_rejected",
          [this] { return _connections_rejected; },
          sm::description(ssx::sformat(
            "{}: Number of connections rejected for hitting connection limits",
            proto)))
          .aggregate(aggregate_labels),
        sm::make_counter(
          "requests_completed",
          [this] { return _requests_completed; },
          sm::description(
            ssx::sformat("{}: Number of successful requests", proto)))
          .aggregate(aggregate_labels),
        sm::make_total_bytes(
          "received_bytes",
          [this] { return _in_bytes; },
          sm::description(ssx::sformat(
            "{}: Number of bytes received from the clients in valid requests",
            proto)))
          .aggregate(aggregate_labels),
        sm::make_total_bytes(
          "sent_bytes",
          [this] { return _out_bytes; },
          sm::description(
            ssx::sformat("{}: Number of bytes sent to clients", proto)))
          .aggregate(aggregate_labels),
        sm::make_counter(
          "method_not_found_errors",
          [this] { return _method_not_found_errors; },
          sm::description(ssx::sformat(
            "{}: Number of requests with not available RPC method", proto)))
          .aggregate(aggregate_labels),
        sm::make_counter(
          "corrupted_headers",
          [this] { return _corrupted_headers; },
          sm::description(ssx::sformat(
            "{}: Number of requests with corrupted headers", proto)))
          .aggregate(aggregate_labels),
        sm::make_counter(
          "service_errors",
          [this] { return _service_errors; },
          sm::description(ssx::sformat("{}: Number of service errors", proto)))
          .aggregate(aggregate_labels),
        sm::make_counter(
          "requests_blocked_memory",
          [this] { return _requests_blocked_memory; },
          sm::description(ssx::sformat(
            "{}: Number of requests blocked in memory backpressure", proto)))
          .aggregate(aggregate_labels),
        sm::make_gauge(
          "requests_pending",
          [this] { return _requests_received - _requests_completed; },
          sm::description(
            ssx::sformat("{}: Number of requests pending in the queue", proto)))
          .aggregate(aggregate_labels),
        sm::make_counter(
          "connections_wait_rate",
          [this] { return _connections_wait_rate; },
          sm::description(ssx::sformat(
            "{}: Number of connections are blocked by connection rate", proto)))
          .aggregate(aggregate_labels),
        sm::make_counter(
          "produce_bad_create_time",
          [this] { return _produce_bad_create_time; },
          sm::description("number of produce requests with timestamps too far "
                          "in the future or in the past"))
          .aggregate(aggregate_labels),
      });
}

void server_probe::setup_public_metrics(
  ssx::metrics::metric_groups& mgs, std::string_view proto) {
    namespace sm = ss::metrics;

    if (proto.ends_with("_rpc")) {
        proto.remove_suffix(4);
    }

    auto server_label = ssx::metrics::make_namespaced_label("server");

    mgs.add_group(
      "rpc",
      {
        sm::make_counter(
          "request_errors_total",
          [this] { return _service_errors; },
          sm::description("Number of rpc errors"),
          {server_label(proto)})
          .aggregate({sm::shard_label}),
        sm::make_gauge(
          "active_connections",
          [this] { return _connections; },
          sm::description("Count of currently active connections"),
          {server_label(proto)})
          .aggregate({sm::shard_label}),
      });
}

std::ostream& operator<<(std::ostream& o, const server_probe& p) {
    o << "{"
      << "connects: " << p._connects << ", "
      << "current connections: " << p._connections << ", "
      << "connection close errors: " << p._connection_close_error << ", "
      << "connections rejected: " << p._connections_rejected << ", "
      << "requests received: " << p._requests_received << ", "
      << "requests completed: " << p._requests_completed << ", "
      << "service errors: " << p._service_errors << ", "
      << "received bytes: " << p._in_bytes << ", "
      << "sent bytes: " << p._out_bytes << ", "
      << "corrupted headers: " << p._corrupted_headers << ", "
      << "method not found errors: " << p._method_not_found_errors << ", "
      << "requests blocked by memory: " << p._requests_blocked_memory << ", "
      << "declined new connections: " << p._declined_new_connections << ", "
      << "connections wait rate: " << p._connections_wait_rate << ", "
      << "produce bad create time: " << p._produce_bad_create_time << ", "
      << "}";
    return o;
}

void client_probe::setup_metrics(
  ssx::metrics::metric_groups& mgs,
  const std::optional<rpc::connection_cache_label>& label,
  const std::optional<model::node_id>& node_id,
  const net::unresolved_address& target_addr) {
    namespace sm = ss::metrics;
    auto target = sm::label("target");
    std::vector<sm::label_instance> labels = {
      target(ssx::sformat("{}:{}", target_addr.host(), target_addr.port()))};
    if (label) {
        labels.push_back(sm::label("connection_cache_label")((*label)()));
    }
    std::vector<sm::label> aggregate_labels;
    // Label the metrics for a given server with the node ID so Seastar can
    // differentiate between them, in case multiple node IDs start at the same
    // address (e.g. in an ungraceful decommission). Aggregate on node ID so
    // the user is presented metrics for each server regardless of node ID.
    if (node_id) {
        auto node_id_label = sm::label("node_id");
        labels.push_back(node_id_label(*node_id));
        aggregate_labels.push_back(node_id_label);
    }
    mgs.add_group(
      prometheus_sanitize::metrics_name("rpc_client"),
      {
        sm::make_gauge(
          "active_connections",
          [this] { return _connections; },
          sm::description("Currently active connections"),
          labels)
          .aggregate(aggregate_labels),
        sm::make_counter(
          "connects",
          [this] { return _connects; },
          sm::description("Connection attempts"),
          labels)
          .aggregate(aggregate_labels),
        sm::make_counter(
          "requests",
          [this] { return _requests; },
          sm::description("Number of requests"),
          labels)
          .aggregate(aggregate_labels),
        sm::make_gauge(
          "requests_pending",
          [this] { return _requests_pending; },
          sm::description("Number of requests pending"),
          labels)
          .aggregate(aggregate_labels),
        sm::make_counter(
          "request_errors",
          [this] { return _request_errors; },
          sm::description("Number or requests errors"),
          labels)
          .aggregate(aggregate_labels),
        sm::make_counter(
          "request_timeouts",
          [this] { return _request_timeouts; },
          sm::description("Number or requests timeouts"),
          labels)
          .aggregate(aggregate_labels),
        sm::make_total_bytes(
          "in_bytes",
          [this] { return _out_bytes; },
          sm::description("Total number of bytes sent (including headers)"),
          labels)
          .aggregate(aggregate_labels),
        sm::make_total_bytes(
          "out_bytes",
          [this] { return _in_bytes; },
          sm::description("Total number of bytes received"),
          labels)
          .aggregate(aggregate_labels),
        sm::make_counter(
          "connection_errors",
          [this] { return _connection_errors; },
          sm::description("Number of connection errors"),
          labels)
          .aggregate(aggregate_labels),
        sm::make_counter(
          "read_dispatch_errors",
          [this] { return _read_dispatch_errors; },
          sm::description("Number of errors while dispatching responses"),
          labels)
          .aggregate(aggregate_labels),
        sm::make_counter(
          "corrupted_headers",
          [this] { return _corrupted_headers; },
          sm::description("Number of responses with corrupted headers"),
          labels)
          .aggregate(aggregate_labels),
        sm::make_counter(
          "server_correlation_errors",
          [this] { return _server_correlation_errors; },
          sm::description("Number of responses with wrong correlation id"),
          labels)
          .aggregate(aggregate_labels),
        sm::make_counter(
          "client_correlation_errors",
          [this] { return _client_correlation_errors; },
          sm::description("Number of errors in client correlation id"),
          labels)
          .aggregate(aggregate_labels),
        sm::make_counter(
          "requests_blocked_memory",
          [this] { return _requests_blocked_memory; },
          sm::description("Number of requests that are blocked because"
                          " of insufficient memory"),
          labels)
          .aggregate(aggregate_labels),
      });
}

std::ostream& operator<<(std::ostream& o, const client_probe& p) {
    o << "{"
      << " requests_sent: " << p._requests
      << ", requests_pending: " << p._requests_pending
      << ", requests_completed: " << p._requests_completed
      << ", request_errors: " << p._request_errors
      << ", request_timeouts: " << p._request_timeouts
      << ", in_bytes: " << p._in_bytes << ", out_bytes: " << p._out_bytes
      << ", connects: " << p._connects << ", connections: " << p._connections
      << ", connection_errors: " << p._connection_errors
      << ", read_dispatch_errors: " << p._read_dispatch_errors
      << ", corrupted_headers: " << p._corrupted_headers
      << ", server_correlation_errors: " << p._server_correlation_errors
      << ", client_correlation_errors: " << p._client_correlation_errors
      << ", requests_blocked_memory: " << p._requests_blocked_memory << " }";
    return o;
}

void tls_certificate_probe::loaded(
  const ss::tls::certificate_credentials& creds, std::exception_ptr ex) {
    static ss::logger log("v::net");

    _load_time = clock_type::now();

    if (ex) {
        _cert_valid = false;
        _cert_expiry_time = {};
        _cert_serial = {};
        _ca_expiry_time = {};
        _ca_serial = {};
        return;
    }

    auto g_creds = reinterpret_cast<gnutls_certificate_credentials_t>(
      get_impl(creds));

    auto certs = [](gnutls_certificate_credentials_t const& creds) {
        gnutls_x509_crt_t* crt_list{};
        unsigned int crt_list_size{};
        gnutls_certificate_get_x509_crt(creds, 0, &crt_list, &crt_list_size);
        return std::span<gnutls_x509_crt_t>(crt_list, crt_list_size);
    }(g_creds);

    auto cleanup = ss::defer([certs]() {
        for (auto cert : certs) {
            gnutls_x509_crt_deinit(cert);
        }
        gnutls_free(&certs.front());
    });

    auto get_expiry = [](gnutls_x509_crt_t cert) {
        return tls_certificate_probe::clock_type::from_time_t(
          gnutls_x509_crt_get_expiration_time(cert));
    };

    auto get_serial = [](gnutls_x509_crt_t cert) {
        constexpr size_t serial_max = 128;
        size_t serial_size{serial_max};
        bytes serial(bytes::initialized_later{}, serial_size);
        gnutls_x509_crt_get_serial(cert, serial.data(), &serial_size);
        serial.resize(serial_size);
        return serial;
    };

    _cert_valid = true;

    for (auto cert : certs) {
        _cert_expiry_time = get_expiry(cert);
        _cert_serial = get_serial(cert);
        vlog(
          log.warn,
          "_cert_expiry: {}, _cert_serial: {}",
          _cert_expiry_time,
          to_hex(_cert_serial));
    }

    {
        gnutls_x509_trust_list_t tlist{};
        gnutls_certificate_get_trust_list(g_creds, &tlist);
        gnutls_x509_trust_list_iter_t iter{};
        gnutls_x509_crt_t cert{};
        while (GNUTLS_E_REQUESTED_DATA_NOT_AVAILABLE
               != gnutls_x509_trust_list_iter_get_ca(tlist, &iter, &cert)) {
            _ca_expiry_time = get_expiry(cert);
            _ca_serial = get_serial(cert);
            vlog(
              log.warn,
              "_ca_expiry: {}, _ca_serial: {}",
              _ca_expiry_time,
              to_hex(_ca_serial));
            gnutls_x509_crt_deinit(cert);
        }
    }

    // auto cert_exp = ss::tls::get_cert_expiry(creds);
    // if (!cert_exp.empty()) {
    //     _cert_expiry_time = cert_exp.front();
    // }
    // auto ca_exp = ss::tls::get_private_key_expiry(creds);
    // if (!ca_exp.empty()) {
    //     _cert_expiry_time = ca_exp.front();
    // }
}

void tls_certificate_probe::setup_metrics(
  std::string_view service, std::string_view listener_name) {
    if (ss::this_shard_id() != 0 || config::shard_local_cfg().disable_metrics) {
        return;
    }

    namespace sm = ss::metrics;
    const auto service_label = sm::label("service");
    const auto listener_label = sm::label("listener_name");
    const auto serial_label = sm::label("serial");
    const auto loaded = sm::label("loaded");

    const std::vector<sm::label_instance> labels = {
      service_label(service), listener_label(listener_name)};

    const auto aggregate_labels = config::shard_local_cfg().aggregate_metrics()
                                    ? std::vector<sm::label>{sm::shard_label}
                                    : std::vector<sm::label>{};
    _metrics.add_group(
      prometheus_sanitize::metrics_name("tls"),
      {
        sm::make_gauge(
          "truststore_expires_in_seconds",
          [this] {
              return std::chrono::duration_cast<std::chrono::seconds>(
                       _ca_expiry_time - clock_type::now())
                .count();
          },
          sm::description(
            "The number of seconds until expiration of the shortest-lived "
            "certificate in truststore certificate chain"),
          labels)
          .aggregate(aggregate_labels),
        sm::make_gauge(
          "certificate_expires_in_seconds",
          [this] {
              return std::chrono::duration_cast<std::chrono::seconds>(
                       _cert_expiry_time - clock_type::now())
                .count();
          },
          sm::description("The number of seconds until expiration of the "
                          "server certificate."),
          labels)
          .aggregate(aggregate_labels),
        sm::make_gauge(
          "certificate_loaded_seconds",
          [this] {
              return std::chrono::duration_cast<std::chrono::seconds>(
                       clock_type::now() - _load_time)
                .count();
          },
          sm::description(
            "The number of seconds ago the certifacate was loaded."),
          labels)
          .aggregate(aggregate_labels),
        sm::make_gauge(
          "certificate_valid",
          [this] { return _cert_valid ? 1 : 0; },
          sm::description("The value is one if the certificate is valid with "
                          "the given truststore, otherwise zero."),
          labels)
          .aggregate(aggregate_labels),
      });
}

void tls_certificate_probe::setup_public_metrics(
  std::string_view service, std::string_view listener_name) {
    if (
      ss::this_shard_id() != 0
      || config::shard_local_cfg().disable_public_metrics) {
        return;
    }

    namespace sm = ss::metrics;
    const auto service_label = sm::label("service");
    const auto listener_label = sm::label("listener_name");

    const std::vector<sm::label_instance> labels = {
      service_label(service), listener_label(listener_name)};

    const auto aggregate_labels = config::shard_local_cfg().aggregate_metrics()
                                    ? std::vector<sm::label>{sm::shard_label}
                                    : std::vector<sm::label>{};
    _public_metrics.add_group(
      prometheus_sanitize::metrics_name("tls"),
      {
        sm::make_gauge(
          "truststore_expires_in_seconds",
          [this] {
              return std::chrono::duration_cast<std::chrono::seconds>(
                       _ca_expiry_time - clock_type::now())
                .count();
          },
          sm::description(
            "The number of seconds until expiration of the shortest-lived "
            "certificate in truststore certificate chain"),
          labels)
          .aggregate(aggregate_labels),
        sm::make_gauge(
          "certificate_expires_in_seconds",
          [this] {
              return std::chrono::duration_cast<std::chrono::seconds>(
                       _cert_expiry_time - clock_type::now())
                .count();
          },
          sm::description("The number of seconds until expiration of the "
                          "server certificate."),
          labels)
          .aggregate(aggregate_labels),
        sm::make_gauge(
          "certificate_loaded_seconds",
          [this] {
              return std::chrono::duration_cast<std::chrono::seconds>(
                       clock_type::now() - _load_time)
                .count();
          },
          sm::description(
            "The number of seconds ago the certifacate was loaded."),
          labels)
          .aggregate(aggregate_labels),
        sm::make_gauge(
          "certificate_valid",
          [this] { return _cert_valid ? 1 : 0; },
          sm::description("The value is one if the certificate is valid with "
                          "the given truststore, otherwise zero."),
          labels)
          .aggregate(aggregate_labels),
      });
}

} // namespace net

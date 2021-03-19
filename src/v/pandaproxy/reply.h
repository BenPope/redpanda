/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "kafka/client/exceptions.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/exceptions.h"
#include "pandaproxy/json/error.h"
#include "pandaproxy/json/exceptions.h"
#include "pandaproxy/json/requests/error_reply.h"
#include "pandaproxy/json/rjson_util.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/parsing/exceptions.h"
#include "seastarx.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sstring.hh>
#include <seastar/http/exception.hh>
#include <seastar/http/reply.hh>

namespace pandaproxy {

constexpr ss::httpd::reply::status_type status_type_unprocessable_entity{422};

inline ss::httpd::reply::status_type error_code_to_status(json::error_code ec) {
    switch (ec) {
    case json::error_code::unprocessable_entity:
        return status_type_unprocessable_entity;
    case json::error_code::kafka_authentication_error:
        return ss::httpd::reply::status_type::unauthorized;
    case json::error_code::kafka_authorization_error:
        return ss::httpd::reply::status_type::forbidden;
    case json::error_code::topic_not_found:
    case json::error_code::partition_not_found:
    case json::error_code::consumer_instance_not_found:
        return ss::httpd::reply::status_type::not_found;
    case json::error_code::zookeeper_error:
    case json::error_code::kafka_error:
    case json::error_code::retriable_kafka_error:
    case json::error_code::ssl_unavailable:
        return ss::httpd::reply::status_type::internal_server_error;
    }
    return ss::httpd::reply::status_type::ok;
}

inline ss::httpd::reply& set_reply_unavailable(ss::httpd::reply& rep) {
    return rep.set_status(ss::httpd::reply::status_type::service_unavailable)
      .add_header("Retry-After", "0");
}

inline std::unique_ptr<ss::httpd::reply> reply_unavailable() {
    auto rep = std::make_unique<ss::httpd::reply>(ss::httpd::reply{});
    set_reply_unavailable(*rep);
    return rep;
}

class exceptional_reply final : public ss::httpd::base_exception {
public:
    exceptional_reply(
      ss::httpd::reply::status_type status,
      pandaproxy::json::error_code ec,
      ss::sstring msg)
      : base_exception{
        json::rjson_serialize(
          json::error_body{.ec = ec, .message = std::move(msg)}),
        status} {}
    exceptional_reply(pandaproxy::json::error_code ec, ss::sstring msg)
      : exceptional_reply{error_code_to_status(ec), ec, std::move(msg)} {}
    exceptional_reply(kafka::error_code ec, ss::sstring msg)
      : exceptional_reply{json::map_error_code(ec), std::move(msg)} {}
};

inline std::unique_ptr<ss::httpd::reply> errored_body(
  ss::httpd::reply::status_type status,
  pandaproxy::json::error_code ec,
  ss::sstring msg) {
    auto rep = std::make_unique<ss::httpd::reply>();
    rep->set_status(status);
    pandaproxy::json::error_body body{.ec = ec, .message = std::move(msg)};
    auto b = json::rjson_serialize(body);
    rep->write_body("json", b);
    return rep;
}

inline std::unique_ptr<ss::httpd::reply>
errored_body(json::error_code ec, ss::sstring msg) {
    return errored_body(error_code_to_status(ec), ec, std::move(msg));
}

inline std::unique_ptr<ss::httpd::reply>
errored_body(parse::error_code, ss::sstring msg) {
    return errored_body(
      ss::httpd::reply::status_type::bad_request,
      json::error_code{400}, // TODO BP: Yarr!
      std::move(msg));
}

inline std::unique_ptr<ss::httpd::reply>
errored_body(kafka::error_code kec, ss::sstring msg) {
    return errored_body(json::map_error_code(kec), std::move(msg));
}

inline std::unique_ptr<ss::httpd::reply> unprocessable_entity(ss::sstring msg) {
    return errored_body(
      status_type_unprocessable_entity,
      json::error_code::unprocessable_entity,
      std::move(msg));
}

inline std::unique_ptr<ss::httpd::reply> exception_reply(std::exception_ptr e) {
    try {
        std::rethrow_exception(e);
    } catch (const ss::gate_closed_exception& e) {
        auto eb = errored_body(
          ss::httpd::reply::status_type::service_unavailable,
          json::error_code::retriable_kafka_error,
          e.what());
        set_reply_unavailable(*eb);
        return eb;
    } catch (const json::exception_base& e) {
        return errored_body(e.error, e.what());
    } catch (const parse::exception_base& e) {
        return errored_body(e.error, e.what());
    } catch (const kafka::exception_base& e) {
        return errored_body(e.error, e.what());
    } catch (const seastar::httpd::base_exception& e) {
        return errored_body(
          e.status(), json::error_code{400}, e.what()); // TODO BP: Yarr!!
    } catch (...) {
        vlog(plog.error, "{}", std::current_exception());
        throw;
    }
}

} // namespace pandaproxy

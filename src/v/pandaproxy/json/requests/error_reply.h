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

#include "json/json.h"
#include "kafka/protocol/errors.h"
#include "pandaproxy/json/error.h"
#include "seastarx.h"

#include <seastar/core/sstring.hh>
#include <seastar/http/reply.hh>

namespace pandaproxy::json {

struct error_body {
    ss::httpd::reply::status_type error_code;
    ss::sstring message;
};

inline error_code map_error_code(kafka::error_code ec) {
    switch (ec) {
    case kafka::error_code::unknown_topic_or_partition:
        return error_code::partition_not_found;
    case kafka::error_code::unknown_member_id:
        return error_code::consumer_instance_not_found;
    case kafka::error_code::offset_out_of_range:
        return error_code::unprocessable_entity;
    default:
        return error_code::kafka_error;
    }
}

inline void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w, const error_body& v) {
    w.StartObject();
    w.Key("error_code");
    ::json::rjson_serialize(w, v.error_code);
    w.Key("message");
    ::json::rjson_serialize(w, v.message);
    w.EndObject();
}

} // namespace pandaproxy::json

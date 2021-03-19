/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include <cstdint>
#include <system_error>

namespace pandaproxy::json {

enum class error_code : uint16_t {
    unprocessable_entity = 422,
    kafka_authentication_error = 40101,
    kafka_authorization_error = 40301,
    topic_not_found = 40401,
    partition_not_found = 40402,
    consumer_instance_not_found = 40403,
    zookeeper_error = 50001,
    kafka_error = 50002,
    retriable_kafka_error = 50003,
    ssl_unavailable = 50101,
};

std::error_code make_error_code(error_code);

} // namespace pandaproxy::json

namespace std {

template<>
struct is_error_code_enum<pandaproxy::json::error_code> : true_type {};

} // namespace std

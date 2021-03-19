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

#include "error.h"

#include <system_error>

namespace pandaproxy::json {

namespace {

struct error_category final : std::error_category {
    const char* name() const noexcept override { return "pandaproxy::json"; }
    std::string message(int ev) const override {
        switch (static_cast<error_code>(ev)) {
        case error_code::unprocessable_entity:
            return "unprocessable_entity";
        case error_code::kafka_authentication_error:
            return "kafka_authentication_error";
        case error_code::kafka_authorization_error:
            return "kafka_authorization_error";
        case error_code::topic_not_found:
            return "topic_not_found";
        case error_code::partition_not_found:
            return "partition_not_found";
        case error_code::consumer_instance_not_found:
            return "consumer_instance_not_found";
        case error_code::zookeeper_error:
            return "zookeeper_error";
        case error_code::kafka_error:
            return "kafka_error";
        case error_code::retriable_kafka_error:
            return "retriable_kafka_error";
        case error_code::ssl_unavailable:
            return "ssl_unavailable";
        default:
            return "(unrecognized error)";
        }
    }
};

const error_category the_error_category{};

}; // namespace

std::error_code make_error_code(error_code e) {
    return {static_cast<int>(e), the_error_category};
}

} // namespace pandaproxy::json

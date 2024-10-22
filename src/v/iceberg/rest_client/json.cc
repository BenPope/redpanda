/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/rest_client/json.h"

#include "iceberg/json_utils.h"

namespace iceberg::rest_client {
oauth_token parse_oauth_token(const json::Document& doc) {
    oauth_token ret;
    ret.access_token = parse_required_str(doc, "access_token");
    auto expires_in = parse_optional_i32(doc, "expires_in");
    if (expires_in) {
        ret.expires_at = ss::lowres_clock::now()
                         + std::chrono::seconds(expires_in.value());
    } else {
        ret.expires_at = ss::lowres_clock::time_point::max();
    }
    ret.token_type = parse_required_str(doc, "token_type");
    ret.scope = parse_optional_str(doc, "scope");
    ret.refresh_token = parse_optional_str(doc, "refresh_token");
    return ret;
}

} // namespace iceberg::rest_client

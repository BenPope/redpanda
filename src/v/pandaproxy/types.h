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

#pragma once
#include "kafka/client/client.h"
#include "kafka/protocol/errors.h"
#include "model/timestamp.h"

#include <seastar/core/shared_ptr.hh>

#include <chrono>

namespace pandaproxy {

using client_ptr = ss::lw_shared_ptr<kafka::client::client>;

struct timestamped_user {
    ss::sstring username;
    client_ptr client;
    model::timestamp last_used;

    explicit timestamped_user(ss::sstring n, client_ptr c, model::timestamp t)
      : username{n}
      , client{c}
      , last_used{t} {}
};

struct credential_t {
    ss::sstring name;
    ss::sstring pass;

    credential_t() = default;
    credential_t(ss::sstring n, ss::sstring p)
      : name{std::move(n)}
      , pass{std::move(p)} {}
};

} // namespace pandaproxy

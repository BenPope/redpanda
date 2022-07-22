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

#include <seastar/core/semaphore.hh>
#include <seastar/core/sstring.hh>

#include <utility>

namespace ssx {

// We use named semaphores because the provided name will be included in
// exception messages, making diagnoising broken or timed-out semaphores much
// easier.

// Use `make_samaphore(name)` to create these.
using semaphore = seastar::named_semaphore;
using semaphore_units
  = seastar::semaphore_units<seastar::named_semaphore_exception_factory>;

template<typename T = seastar::sstring>
semaphore make_semaphore(size_t count, T&& name) {
    return {
      count, seastar::named_semaphore_exception_factory{std::forward<T>(name)}};
}

} // namespace ssx
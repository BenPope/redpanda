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

#include "likely.h"
#include "outcome.h"
#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/types.h"
#include "seastarx.h"

#include <seastar/core/shared_future.hh>
#include <seastar/util/later.hh>

#include <fmt/core.h>
#include <fmt/format.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <exception>
#include <utility>

namespace pandaproxy::schema_registry {

///\brief Create a schema definition from raw input.
///
/// Validates the JSON and minify it.
/// TODO(Ben): Validate that it's a valid schema
///
/// Returns error_code::schema_invalid on failure
template<typename Encoding>
result<schema_definition> make_schema_definition(std::string_view sv) {
    // Validate and minify
    // TODO (Ben): Minify. e.g.:
    // "schema": "{\"type\": \"string\"}" -> "schema": "\"string\""
    rapidjson::GenericDocument<Encoding> doc;
    doc.Parse(sv.data(), sv.size());
    if (doc.HasParseError()) {
        return error_code::schema_invalid;
    }
    rapidjson::GenericStringBuffer<Encoding> str_buf;
    str_buf.Reserve(sv.size());
    rapidjson::Writer<rapidjson::StringBuffer> w{str_buf};
    doc.Accept(w);
    return schema_definition{
      ss::sstring{str_buf.GetString(), str_buf.GetSize()}};
}

///\brief The first invocation of one_shot::operator()() will invoke func and
/// wait for it to finish. Concurrent invocatons will also wait.
///
/// On success, all waiters will be allowed to continue. Successive invocations
/// of one_shot::operator()() will return ss::now().
///
/// If func fails, waiters will receive the error, and one_shot will be reset.
/// Successive calls to operator()() will restart the process.
class one_shot {
    enum class state { empty, started, available };

public:
    explicit one_shot(ss::noncopyable_function<ss::future<>()> func)
      : _func{std::move(func)} {}
    ss::future<> operator()() {
        if (likely(_state == state::available)) {
            return ss::now();
        }
        if (std::exchange(_state, state::started) == state::empty) {
            _prom_started = {};
            (void)_func().then_wrapped([this](ss::future<> f) {
                _state = f.failed() ? state::empty : state::available;
                f.failed() ? _prom_started.set_exception(f.get_exception())
                           : _prom_started.set_value();
            });
        }
        return _prom_started.get_shared_future();
    }

private:
    ss::noncopyable_function<ss::future<>()> _func;
    ss::shared_promise<> _prom_started{};
    state _state{state::empty};
};

} // namespace pandaproxy::schema_registry

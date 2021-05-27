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

#include "kafka/client/client.h"
#include "kafka/protocol/kafka_batch_adapter.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "outcome.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/std-coroutine.hh>

#include <fmt/core.h>
#include <fmt/format.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

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

class client_fetcher final : public model::record_batch_reader::impl {
    using storage_t = model::record_batch_reader::storage_t;

public:
    client_fetcher(
      kafka::client::client& client,
      model::topic_partition tp,
      model::offset first,
      model::offset last)
      : _client{client}
      , _tp{std::move(tp)}
      , _next_offset{first}
      , _last_offset{last}
      , _batch_reader{} {}

    // Implements model::record_batch_reader::impl
    bool is_end_of_stream() const final { return _next_offset >= _last_offset; }

    // Implements model::record_batch_reader::impl
    ss::future<storage_t>
    do_load_slice(model::timeout_clock::time_point t) final {
        if (!_batch_reader || _batch_reader->is_end_of_stream()) {
            vlog(plog.debug, "Schema registry: fetch offset: {}", _next_offset);
            auto res = co_await _client.fetch_partition(
              _tp, _next_offset, 1_MiB, t - model::timeout_clock::now());
            vlog(plog.debug, "Schema registry: fetch result: {}", res);
            vassert(res.begin() != res.end(), "Can this fail?");
            _batch_reader = std::move(res.begin()->partition_response->records);
            vassert(++res.begin() == res.end(), "Can this fail?");
        }
        auto ret = co_await _batch_reader->do_load_slice(t);
        using data_t = model::record_batch_reader::data_t;
        vassert(std::holds_alternative<data_t>(ret), "Can this fail?");
        auto& data = std::get<data_t>(ret);
        vassert(!data.empty(), "Can this fail?");
        _next_offset = ++data.back().last_offset();
        vlog(plog.debug, "Schema registry: next_offset: {}", _next_offset);
        co_return ret;
    }

    // Implements model::record_batch_reader::impl
    void print(std::ostream& os) final {
        os << "{pandaproxy::schema_registry::client_fetcher}";
    }

private:
    kafka::client::client& _client;
    model::topic_partition _tp;
    model::offset _next_offset;
    model::offset _last_offset;
    std::optional<kafka::batch_reader> _batch_reader;
};

} // namespace pandaproxy::schema_registry

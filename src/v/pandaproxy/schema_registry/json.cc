/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "pandaproxy/schema_registry/json.h"

#include "json/allocator.h"
#include "json/document.h"
#include "json/encodings.h"
#include "json/schema.h"
#include "json/stringbuffer.h"
#include "json/types.h"
#include "json/writer.h"
#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/errors.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/types.h"
#include "strings/string_switch.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/util/defer.hh>

#include <absl/container/flat_hash_set.h>
#include <boost/outcome/std_result.hpp>
#include <boost/outcome/success_failure.hpp>
#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <rapidjson/error/en.h>

#include <exception>
#include <memory>
#include <stack>
#include <string_view>

namespace pandaproxy::schema_registry {

namespace {

bool check_compatible(
  [[maybe_unused]] const json::SchemaDocument& reader,
  [[maybe_unused]] const json::SchemaDocument& writer) {
    return false;
}

class remote_schema_document_provider final
  : public rapidjson::IGenericRemoteSchemaDocumentProvider<
      json::SchemaDocument> {
public:
    const json::SchemaDocument* GetRemoteDocument(
      [[maybe_unused]] const char* uri,
      [[maybe_unused]] rapidjson::SizeType length) override {
        // Resolve the uri and returns a pointer to that schema.
        return nullptr;
    }
};

} // namespace

struct json_schema_definition::impl {
    ss::sstring to_json() const {
        json::StringBuffer buf;
        json::Writer<json::StringBuffer> wrt(buf);
        doc.Accept(wrt);
        return {buf.GetString(), buf.GetLength()};
    }

    explicit impl(
      json::Document doc,
      std::string_view name,
      std::unique_ptr<remote_schema_document_provider> rsdp)
      : doc{std::move(doc)}
      , rsdp{std::move(rsdp)}
      , schema(doc, name.data(), name.size(), rsdp.get()) {}

    json::Document doc;
    std::unique_ptr<remote_schema_document_provider> rsdp;
    json::SchemaDocument schema;
};

bool operator==(
  const json_schema_definition& lhs, const json_schema_definition& rhs) {
    return lhs.raw() == rhs.raw();
}

std::ostream& operator<<(std::ostream& os, const json_schema_definition& def) {
    fmt::print(
      os,
      "type: {}, definition: {}",
      to_string_view(def.type()),
      def().to_json());
    return os;
}

canonical_schema_definition::raw_string json_schema_definition::raw() const {
    return canonical_schema_definition::raw_string{_impl->to_json()};
}

ss::sstring json_schema_definition::name() const {
    return {
      _impl->schema.GetURI().GetString(),
      _impl->schema.GetURI().GetStringLength()};
};

ss::future<json_schema_definition>
make_json_schema_definition(sharded_store&, canonical_schema schema) {
    std::optional<std::exception> ex;
    try {
        json::Document doc;

        if (!doc.Parse(schema.def().raw()()).HasParseError()) {
            // fail;
        }
        // Populate impl->rsdp from doc and schema.def().refs()
        auto rsdp = std::make_unique<remote_schema_document_provider>();

        auto impl = ss::make_shared<json_schema_definition::impl>(
          std::move(doc), schema.sub()(), std::move(rsdp));

        co_return json_schema_definition{std::move(impl), schema.def().refs()};
    } catch (const std::exception& e) {
        ex = e;
    }
    co_return ss::coroutine::exception(
      std::make_exception_ptr(as_exception(error_info{
        error_code::schema_invalid,
        fmt::format("Invalid schema {}", ex->what())})));
}

ss::future<canonical_schema>
make_canonical_json_schema(sharded_store&, unparsed_schema def) {
    // TODO, validate, pretty print
    co_return canonical_schema{
      def.sub(), canonical_schema_definition{def.def().raw(), def.type()}};
}

bool check_compatible(
  const json_schema_definition& reader, const json_schema_definition& writer) {
    return check_compatible(reader().schema, writer().schema);
}

} // namespace pandaproxy::schema_registry

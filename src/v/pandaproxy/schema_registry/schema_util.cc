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

#include "pandaproxy/schema_registry/schema_util.h"

#include "pandaproxy/schema_registry/avro.h"
#include "pandaproxy/schema_registry/error.h"

#include <boost/outcome/try.hpp>
#include <google/protobuf/descriptor.h>
#include <rapidjson/encodings.h>

namespace pandaproxy::schema_registry {

ss::sstring to_string(const schema_definition& def) {
    struct stringer {
        ss::sstring operator()(const raw_schema_definition& def) const {
            return def.definition();
        }
        ss::sstring operator()(const avro_schema_definition& def) const {
            return def().toJson(false);
        }
        ss::sstring operator()(const protobuf_schema_definition& def) const {
            return def()->DebugString();
        }
    };
    return std::visit(stringer{}, def);
}

std::ostream& operator<<(std::ostream& os, const schema_definition& def) {
    return os << to_string(def);
}

bool operator==(const schema_definition& lhs, const schema_definition& rhs) {
    struct equals {
        bool operator()(const raw_schema_definition& lhs) const {
            auto const& other = std::get<raw_schema_definition>(rhs);
            return lhs.type == other.type && lhs.definition == other.definition;
        }
        bool operator()(const avro_schema_definition& lhs) const {
            return to_string(lhs)
                   == to_string(std::get<avro_schema_definition>(rhs));
        }
        bool operator()(const protobuf_schema_definition& lhs) const {
            return lhs == std::get<protobuf_schema_definition>(rhs);
        }
        const schema_definition& rhs;
    };
    return lhs.index() == rhs.index() && std::visit(equals{rhs}, lhs);
}

result<schema_definition>
make_schema_definition(const raw_schema_definition& def) {
    switch (def.type) {
    case schema_type::avro:
        return BOOST_OUTCOME_TRYX(
          make_avro_schema_definition(def.definition()));
    case schema_type::protobuf:
        return BOOST_OUTCOME_TRYX(
          make_protobuf_schema_definition(def.definition()));
    case schema_type::json:
        return invalid_schema_type(def.type);
    }
    __builtin_unreachable();
}

schema_type get_schema_type(const schema_definition& def) {
    struct get_schema_type_impl {
        schema_type operator()(const raw_schema_definition& def) const {
            return def.type;
        }
        schema_type operator()(const avro_schema_definition&) const {
            return schema_type::avro;
        }
        schema_type operator()(const protobuf_schema_definition&) const {
            return schema_type::protobuf;
        }
    };
    return std::visit(get_schema_type_impl{}, def);
}

bool check_compatible(
  const raw_schema_definition& reader, const raw_schema_definition& writer) {
    return reader.type == writer.type && reader.definition == writer.definition;
}

bool check_compatible(
  const schema_definition& reader, const schema_definition& writer) {
    return reader.index() == writer.index()
           && std::visit(
             [&writer](auto const& reader) {
                 using schema_t = std::decay_t<decltype(reader)>;
                 return check_compatible(reader, std::get<schema_t>(writer));
             },
             reader);
}

result<schema_definition> validate(schema_definition def) {
    struct validator {
        result<schema_definition>
        operator()(const raw_schema_definition& def) const {
            return make_schema_definition(def);
        }
        result<schema_definition> operator()(avro_schema_definition def) const {
            return std::move(def);
        }
        result<schema_definition>
        operator()(protobuf_schema_definition def) const {
            return def;
        }
    };
    return std::visit(validator{}, std::move(def));
}

} // namespace pandaproxy::schema_registry

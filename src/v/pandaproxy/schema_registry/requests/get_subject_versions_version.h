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

#include "pandaproxy/json/rjson_util.h"
#include "pandaproxy/schema_registry/schema_util.h"
#include "pandaproxy/schema_registry/types.h"

namespace pandaproxy::schema_registry {

struct post_subject_versions_version_response {
    referenced_schema schema;
    schema_id id;
    schema_version version;
};

inline void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w,
  const post_subject_versions_version_response& res) {
    w.StartObject();
    w.Key("subject");
    ::json::rjson_serialize(w, res.schema.sub);
    w.Key("version");
    ::json::rjson_serialize(w, res.version);
    w.Key("id");
    ::json::rjson_serialize(w, res.id);
    auto type = get_schema_type(res.schema.def);
    if (type != schema_type::avro) {
        w.Key("schemaType");
        ::json::rjson_serialize(w, to_string_view(type));
    }
    if (!res.schema.references.empty()) {
        w.Key("references");
        w.StartArray();
        for (const auto& ref : res.schema.references) {
            w.StartObject();
            w.Key("name");
            ::json::rjson_serialize(w, ref.name);
            w.Key("subject");
            ::json::rjson_serialize(w, ref.sub);
            w.Key("version");
            ::json::rjson_serialize(w, ref.version);
            w.EndObject();
        }
        w.EndArray();
    }
    w.Key("schema");
    ::json::rjson_serialize(w, to_string(res.schema.def));
    w.EndObject();
}

} // namespace pandaproxy::schema_registry

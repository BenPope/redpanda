// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/json.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/testing/thread_test_case.hh>

namespace {

namespace pp = pandaproxy;
namespace pps = pp::schema_registry;

constexpr std::string_view schema_1 = R"({
  "type": "object",
  "properties": {
    "foo": { "type": "string" },
    "bar": { "type": "string" }
  }
})";

constexpr std::string_view schema_2 = R"({
  "type": "object",
  "properties": {
    "foo": { "type": "string" },
    "bar": { "type": "string" }
  },
  "additionalProperties": false
})";

bool check_compatible(
  const pps::canonical_schema_definition& r,
  const pps::canonical_schema_definition& w) {
    pps::sharded_store s;
    return check_compatible(
      pps::make_json_schema_definition(
        s, {pps::subject("r"), {r.raw(), pps::schema_type::json}})
        .get(),
      pps::make_json_schema_definition(
        s, {pps::subject("w"), {w.raw(), pps::schema_type::json}})
        .get());
}
} // namespace

SEASTAR_THREAD_TEST_CASE(test_json_parse) {
    pps::canonical_schema_definition s1{schema_1, pps::schema_type::json};
    pps::canonical_schema_definition s2{schema_2, pps::schema_type::json};
    BOOST_REQUIRE(check_compatible(s1, s2));
}

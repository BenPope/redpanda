// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#define BOOST_TEST_MODULE json_meta

#include "json/json.h"
#include "json/meta.h"

#include <boost/test/unit_test.hpp>
#include <daw/json/daw_json_data_contract.h>
#include <daw/json/daw_json_link.h>
#include <daw/json/daw_json_link_types.h>

#include <optional>
#include <string>

namespace foo {

struct bar {
    int16_t port;
    std::string host;
};

struct baz {
    int16_t port;
    std::string host;
};

} // namespace foo

namespace daw::json {

// Specializing a struct in another namespace is annoying
template<>
struct json_data_contract<foo::bar> {
    using type = json_member_list<
      json_number<"int16_t", int16_t>,
      json_string_raw<"std::string", std::string>>;
    static constexpr auto to_json_data(foo::bar const& b) {
        return std::forward_as_tuple(b.port, b.host);
    }
};

} // namespace daw::json

BOOST_AUTO_TEST_CASE(test_daw_json) {
    foo::bar bar{8080, "host"};
    BOOST_REQUIRE_EQUAL(
      daw::json::to_json(bar), R"({"int16_t":8080,"std::string":"host"})");
}

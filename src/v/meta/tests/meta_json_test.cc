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
#include "meta/fmt/meta.h"
#include "meta/json/meta.h"

#include <boost/test/unit_test.hpp>
#include <daw/json/daw_json_data_contract.h>
#include <daw/json/daw_json_link.h>
#include <daw/json/daw_json_link_types.h>

#include <optional>
#include <string>

namespace foo {

struct address {
    std::string host;
    int16_t port;
};

} // namespace foo

namespace daw::json {

// Specializing a struct in another namespace is annoying
template<>
struct json_data_contract<foo::address> {
    using type = json_member_list<
      json_string_raw<"host", std::string>,
      json_number<"port", int16_t>>;
    static constexpr auto to_json_data(foo::address const& a) {
        return std::forward_as_tuple(a.host, a.port);
    }
};

} // namespace daw::json

namespace fmt {

// Specializing a struct in another namespace is deja vu
template<>
struct formatter<foo::address> : formatter<string_view> {
    template<typename FormatContext>
    auto format(foo::address const& a, FormatContext& ctx) const {
        return format_to(ctx.out(), "host: {}, port: {}", a.host, a.port);
    }
};

} // namespace fmt

namespace foo {

struct meta_address {
    using meta_json_type = daw::json::json_member_list<
      daw::json::json_string_raw<"host", std::string>,
      daw::json::json_number<"port", int16_t>>;

    auto meta_json_data() const { return std::tie(host, port); }

    std::string host;
    int16_t port;
};

} // namespace foo

BOOST_AUTO_TEST_CASE(test_daw_json) {
    auto expected = R"({"host":"localhost","port":8080})";

    BOOST_REQUIRE_EQUAL(
      daw::json::to_json(foo::address{"localhost", 8080}), expected);

    static_assert(meta::json::has_meta<foo::meta_address>);
    BOOST_REQUIRE_EQUAL(
      daw::json::to_json(foo::meta_address{"localhost", 8080}), expected);
}

BOOST_AUTO_TEST_CASE(test_fmt) {
    auto expected = R"(host: localhost, port: 8080)";

    BOOST_REQUIRE_EQUAL(
      fmt::format("{}", foo::address{"localhost", 8080}), expected);

    static_assert(meta::fmt::has_meta<foo::meta_address>);
    BOOST_REQUIRE_EQUAL(
      fmt::format("{}", foo::meta_address{"localhost", 8080}), expected);
}

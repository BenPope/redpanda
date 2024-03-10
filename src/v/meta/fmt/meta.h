// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "meta/json/meta.h"

// clang-format off
#include <cstdlib>
// clang-format on
#include <daw/daw_fwd_pack_apply.h>
#include <daw/daw_traits.h>
#include <daw/json/daw_json_link_types.h>

#include <utility>

namespace meta::fmt {

// Temporarily cheat by reusing json meta
template<typename T>
concept has_meta = ::meta::json::has_meta<T>;

enum class format_impl { std, fmt };

template<typename T, typename B>
struct formatter;

// template<typename T>
// concept has_formatter = requires {
//     std::is_constructible_v<meta::fmt::formatter<T>>;
// };

template<typename V>
struct member : std::pair<std::string_view, V const&> {
    using underlying_t = std::pair<std::string_view, V const&>;
    using std::pair<std::string_view, V const&>::pair; // Inherit constructors
};

template<typename V>
auto make_member(std::convertible_to<std::string_view> auto k, V const& v) {
    return member<V const&>{k, v};
}

template<typename... members>
struct object : std::tuple<members...> {
    using underlying_t = std::tuple<members const&...>;
    using std::tuple<members...>::tuple;
};

template<typename... members>
auto make_object(members... m) {
    return object<members...>{m...};
}

template<typename... members>
struct list : std::array<members...> {
    using std::array<members...>::array;
};

} // namespace meta::fmt

namespace meta::fmt {

template<::meta::fmt::has_meta T, typename B>
struct formatter<T, B> : B {
    template<typename FormatContext>
    auto format(T const& t, FormatContext& ctx) const {
        using U = T::meta_json_type::i_am_a_json_member_list;
        auto u = t.meta_json_data();

        auto interleaved = [&]<std::size_t... I>(std::index_sequence<I...>) {
            // return object(member(
            return make_object(make_member(
              std::string_view{daw::pack_element_t<I, U>::name},
              std::get<I>(u))...);
        }(std::make_index_sequence<std::tuple_size_v<decltype(u)>>{});
        return B::format_to(ctx.out(), "{}", interleaved);
    }
};

} // namespace meta::fmt

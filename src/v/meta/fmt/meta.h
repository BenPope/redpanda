// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "meta/json/meta.h"

#include <daw/json/daw_json_link_types.h>
#include <fmt/format.h>

namespace meta::fmt {

// Temporarily cheat by reusing json meta
template<typename T>
concept has_meta = ::meta::json::has_meta<T>;

namespace detail {

template<typename Member>
void process_member(const Member&) {
    // Default implementation for unknown member types
    std::cout << "Processing unknown member" << std::endl;
}

template<daw::json::json_name Name, typename T>
void process_member(const daw::json::json_number<Name, T>&) {
    // Process json_number
    std::cout << "Processing json_number with Name: " << typeid(Name).name()
              << ", Type: " << typeid(T).name() << std::endl;
}

template<daw::json::json_name Name, typename T>
void process_member(const daw::json::json_string_raw<Name, T>&) {
    // Process json_string_raw
    std::cout << "Processing json_string_raw with Name: " << typeid(Name).name()
              << ", Type: " << typeid(T).name() << std::endl;
}

template<typename... Members, tuple_like Values>
void iterate_json_member_list(Values const& vals) {
    (process_member < std::forward<Members>(), ...);
}

} // namespace detail

} // namespace meta::fmt

namespace fmt {

template<::meta::fmt::has_meta T>
struct formatter<T> : formatter<string_view> {
    template<typename FormatContext>
    auto format(T const& a, FormatContext& ctx) const {
        ::meta::fmt::detail::iterate_json_member_list<T::meta_json_type>(
          a.meta_json_data());
        return format_to(ctx.out(), "host: {}, port: {}", a.host, a.port);
    }
};

} // namespace fmt

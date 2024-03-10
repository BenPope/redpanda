// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "meta/fmt/meta.h"

#include <format>

namespace meta::fmt {

struct std_format_base : std::formatter<std::string_view> {
    template<std::output_iterator<const char&> Out, typename... Args>
    static Out
    format_to(Out out, std::format_string<Args...> fmt, Args&&... args) {
        return std::format_to(
          std::move(out), std::move(fmt), std::forward<Args>(args)...);
    }
};

} // namespace meta::fmt

template<typename T>
struct std::formatter<meta::fmt::member<T>> : std::formatter<string_view> {
    template<typename FormatContext>
    auto format(meta::fmt::member<T> const& t, FormatContext& ctx) const {
        return std::format_to(ctx.out(), "{}: {}", t.first, t.second);
    }
};

template<typename... members>
struct std::formatter<meta::fmt::object<members...>>
  : std::formatter<typename meta::fmt::object<members...>::underlying_t> {
    template<typename FormatContext>
    auto
    format(meta::fmt::object<members...> const& t, FormatContext& ctx) const {
        return std::format_to(
          ctx.out(),
          "{:n}",
          static_cast<meta::fmt::object<members...>::underlying_t const&>(t));
    }
};

// Generic mapping from meta::fmt::formatter to std::formatter
template<meta::fmt::has_meta T>
struct ::std::formatter<T>
  : meta::fmt::formatter<T, meta::fmt::std_format_base> {};

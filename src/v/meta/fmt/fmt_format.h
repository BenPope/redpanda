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

#include <fmt/core.h>
#include <fmt/ranges.h>

namespace meta::fmt {

struct fmt_format_base : ::fmt::formatter<std::string_view> {
    template<std::output_iterator<const char&> Out, typename... Args>
    static Out
    format_to(Out out, ::fmt::format_string<Args...> fmt, Args&&... args) {
        return ::fmt::format_to(
          std::move(out), std::move(fmt), std::forward<Args>(args)...);
    }
};

} // namespace meta::fmt

template<typename T>
struct fmt::formatter<meta::fmt::member<T>> : fmt::formatter<std::string_view> {
    template<typename FormatContext>
    auto format(meta::fmt::member<T> const& t, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}: {}", t.first, t.second);
    }
};

template<typename... members>
struct fmt::formatter<meta::fmt::object<members...>>
  : fmt::formatter<typename meta::fmt::object<members...>::underlying_t> {
    template<typename FormatContext>
    auto
    format(meta::fmt::object<members...> const& t, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}", fmt::join(t, ", "));
    }
};

// Generic mapping from meta::fmt::formatter to fmt::formatter
template<meta::fmt::has_meta T>
struct ::fmt::formatter<T>
  : meta::fmt::formatter<T, meta::fmt::fmt_format_base> {};

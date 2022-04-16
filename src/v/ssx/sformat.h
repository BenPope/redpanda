/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include <seastar/core/sstring.hh>

#include <fmt/format.h>

template<>
struct fmt::formatter<seastar::sstring> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.end(); }
    template<typename FormatContext>
    auto format(const seastar::sstring& s, FormatContext& ctx) {
        return format_to(ctx.out(), "{}", std::string_view(s));
    }
};

#if FMT_VERSION >= 80000
template<typename... Args>
void fmt_append(
  fmt::memory_buffer& buf,
  fmt::format_string<Args...> format_str,
  Args&&... args) {
    fmt::format_to(
      std::back_inserter(buf), format_str, std::forward<Args>(args)...);
}
#else
template<typename... Args>
void fmt_append(fmt::memory_buffer& buf, Args&&... args) {
    fmt::format_to(buf, std::forward<Args>(args)...);
}
#endif

namespace ssx {

template<typename... Args>
seastar::sstring
sformat(fmt::format_string<Args...> format_str, Args&&... args) {
    auto size = fmt::formatted_size(format_str, std::forward<Args>(args)...);
    seastar::sstring buffer(seastar::sstring::initialized_later{}, size);
    fmt::format_to(buffer.data(), format_str, std::forward<Args>(args)...);
    return buffer;
}

} // namespace ssx

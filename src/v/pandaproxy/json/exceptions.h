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

#include "pandaproxy/json/requests/error_reply.h"

#include <fmt/format.h>

#include <stdexcept>
#include <string>

namespace pandaproxy::json {

struct exception_base : std::exception {
    exception_base(error_code err, std::string_view msg)
      : std::exception{}
      , error{err}
      , msg{msg} {}
    const char* what() const noexcept final { return msg.c_str(); }
    error_code error;
    std::string msg;
};

class parse_error final : public exception_base {
public:
    explicit parse_error(size_t offset)
      : exception_base(
        error_code::unprocessable_entity,
        fmt::format("parse error at offset {}", offset)) {}
    explicit parse_error(std::string_view msg)
      : exception_base(error_code::unprocessable_entity, msg) {}
};

class serialize_error final : public exception_base {
public:
    explicit serialize_error(kafka::error_code ec)
      : exception_base(map_error_code(ec), kafka::error_code_to_str(ec)) {}
};

} // namespace pandaproxy::json

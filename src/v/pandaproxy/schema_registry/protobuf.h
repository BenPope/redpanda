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

#include "pandaproxy/schema_registry/errors.h"
#include "pandaproxy/schema_registry/types.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

namespace google::protobuf {
class Descriptor;
}

namespace pandaproxy::schema_registry {

using protobuf_schema_definition = named_type<
  const google::protobuf::Descriptor*,
  struct protobuf_schema_definition_tag>;

result<protobuf_schema_definition>
make_protobuf_schema_definition(std::string_view sv);

bool check_compatible(
  const protobuf_schema_definition& reader,
  const protobuf_schema_definition& writer);

} // namespace pandaproxy::schema_registry

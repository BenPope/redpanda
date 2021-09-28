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

namespace pandaproxy::schema_registry {

class protobuf_store {
public:
    explicit protobuf_store(store& store)
      : _store{store} {}

    result<protobuf_schema_definition>
    make_schema_definition(const canonical_schema& ref);

    result<canonical_schema_definition>
    validate_schema(const canonical_schema& schema);

    result<canonical_schema> make_canonical_schema(unparsed_schema schema);

private:
    store& _store;
};

bool check_compatible(
  const protobuf_schema_definition& reader,
  const protobuf_schema_definition& writer);

} // namespace pandaproxy::schema_registry

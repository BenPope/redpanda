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

///\brief Construct a schema in the native format
result<schema_definition>
make_schema_definition(const raw_schema_definition& def);

///\brief Get the schema_type of the schema_definition.
schema_type get_schema_type(const schema_definition& def);

///\brief Convert the schema to a string suitable for reparsing.
ss::sstring to_string(const schema_definition& def);

///\brief Check the schema parses with the native format
result<schema_definition> validate(schema_definition def);

///\brief Provide a minimal check and minify the input
result<raw_schema_definition> sanitize(raw_schema_definition def);

bool check_compatible(
  const schema_definition& lhs, const schema_definition& rhs);

} // namespace pandaproxy::schema_registry

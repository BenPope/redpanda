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

#include "pandaproxy/schema_registry/schema_util.h"

#include "pandaproxy/schema_registry/avro.h"

namespace pandaproxy::schema_registry {

result<void> validate(const schema_definition& def) {
    switch (def.type()) {
    case schema_type::avro: {
        auto res = make_avro_schema_definition(def.raw()());
        if (res.has_error()) {
            return res.assume_error();
        }
        return outcome::success();
    }
    default:
        return invalid_schema_type(def.type());
    }
}

result<schema_definition> sanitize(const schema_definition& def) {
    switch (def.type()) {
    case schema_type::avro: {
        return sanitize_avro_schema_definition(def);
    }
    default:
        return invalid_schema_type(def.type());
    }
}

} // namespace pandaproxy::schema_registry

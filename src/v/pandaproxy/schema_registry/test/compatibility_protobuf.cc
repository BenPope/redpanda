// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/test/compatibility_protobuf.h"

#include "pandaproxy/schema_registry/protobuf.h"
#include "pandaproxy/schema_registry/schema_util.h"
// #include "pandaproxy/schema_registry/store.h"
#include "pandaproxy/schema_registry/types.h"

#include <boost/test/unit_test.hpp>
#include <boost/test/unit_test_log.hpp>

namespace pp = pandaproxy;
namespace pps = pp::schema_registry;

BOOST_AUTO_TEST_CASE(test_protobuf_enum) {
    // pps::store s;
    // // Adding an enum field is ok
    // auto enum2_proto = s.make_schema_definition(pps::subject{"enum2"},
    // enum2); auto enum3_proto =
    // s.make_schema_definition(pps::subject{"enum2"}, enum2);
    // BOOST_REQUIRE(check_compatible(enum3_proto.value(),
    // enum2_proto.value()));
}

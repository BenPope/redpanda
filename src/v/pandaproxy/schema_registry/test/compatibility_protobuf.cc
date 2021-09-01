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
#include "pandaproxy/schema_registry/types.h"

#include <boost/test/unit_test.hpp>
#include <boost/test/unit_test_log.hpp>

namespace pp = pandaproxy;
namespace pps = pp::schema_registry;

BOOST_AUTO_TEST_CASE(test_protobuf_enum) {
    // Adding an enum field is ok
    std::cout << enum2();
    std::cout << enum3();
    BOOST_REQUIRE(check_compatible(enum3, enum2));
}

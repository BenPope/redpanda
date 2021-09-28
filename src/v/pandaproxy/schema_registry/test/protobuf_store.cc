// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/protobuf.h"
#include "pandaproxy/schema_registry/store.h"
#include "pandaproxy/schema_registry/test/compatibility_protobuf.h"
#include "pandaproxy/schema_registry/types.h"

#include <boost/test/unit_test.hpp>
#include <boost/test/unit_test_log.hpp>

namespace pp = pandaproxy;
namespace pps = pp::schema_registry;

BOOST_AUTO_TEST_CASE(test_protobuf_store_simple) {
    pps::store s;
    // pps::protobuf_store ps{s};

    // auto simple_ins = ps.insert({pps::subject{"simple"}, simple}).value();
    // auto imported_ins = ps.insert({pps::subject{"imported"},
    // imported}).value();
}

BOOST_AUTO_TEST_CASE(test_protobuf_store_subject_duplicate) {
    pps::store s;
    // pps::protobuf_store ps{s};
    pps::subject simple1{"simple1"};
    pps::subject simple2{"simple2"};

    // auto simple1_ins = ps.insert({simple1, simple}).value();
    // BOOST_ASSERT(simple1_ins);
    // auto simple2_ins = ps.insert({simple2, simple}).value();
    // BOOST_ASSERT(simple2_ins);

    // auto get1 = ps.get(simple1).value();
    // auto get2 = ps.get(simple2).value();
    // BOOST_REQUIRE_EQUAL(get1.raw(), simple.raw());
}

BOOST_AUTO_TEST_CASE(test_protobuf_store_subject_versioned) {
    pps::store s;
    // pps::protobuf_store ps{s};
    pps::subject simple1{"simple1"};

    // auto simple1_ins = ps.insert({simple1, simple}).value();
    // BOOST_ASSERT(simple1_ins);
    // auto simple1_v2_ins = ps.insert({simple1, simple_v2}).value();
    // BOOST_ASSERT(simple1_v2_ins);
    // auto get1 = ps.get(simple1).value();
    // BOOST_REQUIRE_EQUAL(get1.raw(), simple_v2.raw());
}

const pps::canonical_schema referenced{
  pps::subject{"referenced-value"},
  imported,
  {pps::schema_reference{.name{"simple"}, .sub{"simple-value"}, .version{1}}}};

BOOST_AUTO_TEST_CASE(test_protobuf_store_subject_references) {
    pps::store s;
    // pps::protobuf_store ps{s};

    // auto simple1_ins
    //   = ps.insert({pps::subject{"simple-value"}, simple}).value();
    // auto simple2_ins = ps.insert(referenced).value();
}

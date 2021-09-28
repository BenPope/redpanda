// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/test/compatibility_protobuf.h"

#include "google/protobuf/descriptor_database.h"
#include "pandaproxy/schema_registry/protobuf.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/store.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/test/unit_test_log.hpp>
#include <google/protobuf/compiler/importer.h>

namespace pp = pandaproxy;
namespace pps = pp::schema_registry;

struct context {};

SEASTAR_THREAD_TEST_CASE(test_protobuf_imported) {
    pps::store s;

    auto schema1 = pps::canonical_schema{pps::subject{"simple"}, simple};
    auto schema2 = pps::canonical_schema{pps::subject{"imported"}, imported};
    auto schema3 = pps::canonical_schema{
      pps::subject{"imported-again"}, imported_again};

    auto sch1 = s.insert(schema1);
    BOOST_REQUIRE_EQUAL(sch1.id(), 1);
    BOOST_REQUIRE_EQUAL(sch1.version(), 1);

    auto sch2 = s.insert(schema2);
    BOOST_REQUIRE_EQUAL(sch2.id(), 2);
    BOOST_REQUIRE_EQUAL(sch2.version(), 1);

    auto sch3 = s.insert(schema3);
    BOOST_REQUIRE_EQUAL(sch3.id(), 3);
    BOOST_REQUIRE_EQUAL(sch3.version(), 1);

    auto valid_simple = s.make_valid_schema(schema1).value();
    auto valid_imported = s.make_valid_schema(schema2).value();
    auto valid_imported_again = s.make_valid_schema(schema3).value();

    s.validate_schema(schema1).value();
    s.validate_schema(schema2).value();
    s.validate_schema(schema3).value();
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_referenced) {
    pps::store s;

    auto schema1 = pps::canonical_schema{pps::subject{"simple.proto"}, simple};
    auto schema2 = pps::canonical_schema{
      pps::subject{"imported.proto"},
      imported,
      {{"simple", pps::subject{"simple.proto"}, pps::schema_version{1}}}};
    auto schema3 = pps::canonical_schema{
      pps::subject{"imported-again.proto"},
      imported_again,
      {{"imported", pps::subject{"imported.proto"}, pps::schema_version{1}}}};

    auto sch1 = s.insert(schema1);
    BOOST_REQUIRE_EQUAL(sch1.id(), 1);
    BOOST_REQUIRE_EQUAL(sch1.version(), 1);

    auto sch2 = s.insert(schema2);
    BOOST_REQUIRE_EQUAL(sch2.id(), 2);
    BOOST_REQUIRE_EQUAL(sch2.version(), 1);

    auto sch3 = s.insert(schema3);
    BOOST_REQUIRE_EQUAL(sch3.id(), 3);
    BOOST_REQUIRE_EQUAL(sch3.version(), 1);

    auto valid_simple = s.make_valid_schema(schema1).value();
    auto valid_imported = s.make_valid_schema(schema2).value();
    auto valid_imported_again = s.make_valid_schema(schema3).value();

    s.validate_schema(schema1).value();
    s.validate_schema(schema2).value();
    s.validate_schema(schema3).value();
}

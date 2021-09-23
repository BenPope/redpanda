// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/protobuf.h"
#include "pandaproxy/schema_registry/schema_util.h"
#include "pandaproxy/schema_registry/store.h"
#include "pandaproxy/schema_registry/test/compatibility_protobuf.h"
#include "pandaproxy/schema_registry/types.h"

#include <boost/test/unit_test.hpp>
#include <boost/test/unit_test_log.hpp>

namespace pp = pandaproxy;
namespace pps = pp::schema_registry;

BOOST_AUTO_TEST_CASE(test_protobuf_store_simple) {
    pps::store s;
    pps::protobuf_store ps{s};

    auto simple_ins = ps.insert({pps::subject{"simple"}, simple}).value();
    auto imported_ins = ps.insert({pps::subject{"imported"}, imported}).value();
}

BOOST_AUTO_TEST_CASE(test_protobuf_store_subject_duplicate) {
    pps::store s;
    pps::protobuf_store ps{s};

    auto simple1_ins = ps.insert({pps::subject{"simple1"}, simple}).value();
    auto simple2_ins = ps.insert({pps::subject{"simple2"}, simple}).value();
}

const pps::referenced_schema referenced{
  .sub{"referenced-value"},
  .def{imported},
  .references{pps::referenced_schema::reference{
    .name{"simple"}, .sub{"simple-value"}, .version{1}}}};

BOOST_AUTO_TEST_CASE(test_protobuf_store_subject_references) {
    pps::store s;
    pps::protobuf_store ps{s};

    auto simple1_ins
      = ps.insert({pps::subject{"simple-value"}, simple}).value();
    auto simple2_ins = ps.insert(referenced).value();
}

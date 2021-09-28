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

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/test/unit_test_log.hpp>
#include <google/protobuf/compiler/importer.h>

#include <optional>

namespace pp = pandaproxy;
namespace pps = pp::schema_registry;

struct simple_sharded_store {
    simple_sharded_store() {
        store.start(ss::default_smp_service_group()).get();
    }
    ~simple_sharded_store() { store.stop().get(); }

    pps::schema_id
    insert(const pps::canonical_schema& schema, pps::schema_version version) {
        const auto id = next_id++;
        auto res = store.upsert(
          pps::seq_marker{
            std::nullopt,
            std::nullopt,
            version,
            pps::seq_marker_key_type::schema},
          schema,
          id,
          version,
          pps::is_deleted::no);
        return id;
    }

    pps::schema_id next_id;
    pps::sharded_store store;
};

SEASTAR_THREAD_TEST_CASE(test_protobuf_imported) {
    simple_sharded_store store;

    auto schema1 = pps::canonical_schema{pps::subject{"simple"}, simple};
    auto schema2 = pps::canonical_schema{pps::subject{"imported"}, imported};
    auto schema3 = pps::canonical_schema{
      pps::subject{"imported-again"}, imported_again};

    auto sch1 = store.insert(schema1, pps::schema_version{1});
    auto sch2 = store.insert(schema2, pps::schema_version{1});
    auto sch3 = store.insert(schema3, pps::schema_version{1});

    auto valid_simple = store.store.make_valid_schema(schema1).get();
    auto valid_imported = store.store.make_valid_schema(schema2).get();
    auto valid_imported_again = store.store.make_valid_schema(schema3).get();

    store.store.validate_schema(schema1).get();
    store.store.validate_schema(schema2).get();
    store.store.validate_schema(schema3).get();
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_referenced) {
    ss::async([&]() {
        simple_sharded_store store;

        auto schema1 = pps::canonical_schema{
          pps::subject{"simple.proto"}, simple};
        auto schema2 = pps::canonical_schema{
          pps::subject{"imported.proto"},
          imported,
          {{"simple", pps::subject{"simple.proto"}, pps::schema_version{1}}}};
        auto schema3 = pps::canonical_schema{
          pps::subject{"imported-again.proto"},
          imported_again,
          {{"imported",
            pps::subject{"imported.proto"},
            pps::schema_version{1}}}};

        auto sch1 = store.insert(schema1, pps::schema_version{1});
        auto sch2 = store.insert(schema2, pps::schema_version{1});
        auto sch3 = store.insert(schema3, pps::schema_version{1});

        auto valid_simple = store.store.make_valid_schema(schema1).get();
        auto valid_imported = store.store.make_valid_schema(schema2).get();
        auto valid_imported_again
          = store.store.make_valid_schema(schema3).get();

        store.store.validate_schema(schema1).get();
        store.store.validate_schema(schema2).get();
        store.store.validate_schema(schema3).get();
    }).get();
}

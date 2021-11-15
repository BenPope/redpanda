// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/cluster_utils.h"
#include "cluster/tests/cluster_test_fixture.h"
#include "kafka/client/client.h"

#include <seastar/core/future.hh>
#include <seastar/testing/thread_test_case.hh>

namespace kc = kafka::client;

static ss::logger logger("kafka_client_retry_test");
class partition_assignment_test_fixture : public cluster_test_fixture {
public:
    using replicas_t = std::vector<model::broker_shard>;
    using history_t = std::vector<replicas_t>;

    partition_assignment_test_fixture() { start_cluster(); }

    ~partition_assignment_test_fixture() {
        controllers.clear();
        for (auto n : nodes) {
            vlog(logger.info, "terminating node: {}", n);
            remove_node_application(n);
            vlog(logger.info, "terminated node: {}", n);
        }
    }

    void start_cluster() {
        // start 3 nodes
        for (auto& n : nodes) {
            auto app = create_node_application(n);
            controllers.push_back(app->controller.get());
        }
        // wait for cluster to be stable
        tests::cooperative_spin_wait_with_timeout(60s, [this] {
            return std::all_of(
              controllers.cbegin(),
              controllers.cend(),
              [this](cluster::controller* c) {
                  return c->get_members_table().local().all_broker_ids().size()
                         == nodes.size();
              });
        }).get0();
    }

    void restart_cluster() {
        BOOST_TEST_MESSAGE("restarting cluster");
        controllers.clear();
        for (auto n : nodes) {
            vlog(logger.info, "stopping stack at node: {}", n);
            remove_node_application(n);
            vlog(logger.info, "stopped stack at node: {}", n);
        }
        start_cluster();
    }

    cluster::controller* get_leader_controller() {
        auto c = *controllers.begin();
        auto leader = c->get_partition_leaders().local().get_leader(
          model::controller_ntp);

        return controllers[(*leader)()];
    }

    void wait_for_metadata_update(
      model::ntp ntp, const std::vector<model::broker_shard>& replica_set) {
        tests::cooperative_spin_wait_with_timeout(
          5s,
          [this, ntp, &replica_set]() mutable {
              return std::all_of(
                nodes.cbegin(),
                nodes.cbegin(),
                [this, ntp = std::move(ntp), &replica_set](
                  model::node_id nid) mutable {
                    auto app = get_node_application(nid);
                    auto md = app->controller->get_topics_state()
                                .local()
                                .get_partition_assignment(ntp);

                    if (!md) {
                        return false;
                    }

                    return cluster::are_replica_sets_equal(
                      replica_set, md->replicas);
                });
          })
          .handle_exception([&replica_set](const std::exception_ptr&) {
              BOOST_FAIL(fmt::format(
                "Timeout waiting for replica set metadata update, replica set "
                "{}",
                replica_set));
          })
          .get0();
        vlog(
          logger.info,
          "SUCCESS: partition {} metadata are up to date with requested "
          "replica set: {}",
          ntp,
          replica_set);
    }

    cluster::controller* controller(int id) { return controllers[id]; }

    kc::client make_client() {
        return kc::client{instances().begin()->second->proxy_client_config()};
    }

    kc::client make_connected_client() {
        auto client = make_client();
        client.connect().get();
        return client;
    }

    std::vector<cluster::controller*> controllers;
    // 5 nodes cluster
    std::vector<model::node_id> nodes = {
      model::node_id(0),
      model::node_id(1),
      model::node_id(2),
      model::node_id(3),
      model::node_id(4)};
};

FIXTURE_TEST(test_retry_list_offsets, partition_assignment_test_fixture) {
    auto client = make_connected_client();
    client.config().retry_base_backoff.set_value(10ms);
    client.config().retries.set_value(size_t(3));
    client.connect().get();

    client.list_offsets(model::schema_registry_internal_tp).get();
}

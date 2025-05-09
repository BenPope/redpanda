# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import subprocess
import os

from rptest.services.cluster import cluster
from ducktape.mark import ignore, matrix

from rptest.services.librdkafka_test_case import LibrdkafkaTestcase
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import expect_exception
from rptest.utils.mode_checks import skip_debug_mode
from rptest.clients.python_librdkafka import PythonLibrdkafka

from confluent_kafka.admin import (AclBinding, AclBindingFilter, ResourceType,
                                   ResourcePatternType, AclOperation,
                                   AclPermissionType)
from confluent_kafka.cimpl import KafkaException


def tests_to_run():
    ignored_tests = set([
        # Broker: Not coordinator
        51,
        # timequery issue
        52,
        # consumer offsets coordinator and tx_coordinator reported to early
        61,
        # librdkafka interceptor test, not important for Redpanda
        66,
        # compaction test - lack of low water mark support
        77,
        # create topic test - topic is being created despite invalid config
        81,
        # fetch max bytes test (Invalid response size 1843717 (0..1000512): increase receive.message.max.bytes)
        82,
        # commit_metadata (control_character_present_exception)
        99,
        # transactions: (Transaction timeout is larger than the maximum value allowed by the broker's max.transaction.timeout.ms)
        103,
        # autocreate topics test - security
        109,
        # cooperative rebalance issue
        113,
        # ACL test - TODO: add security config to the test
        115,
        119,
        # store_offsets: (control_character_present_exception)
        130,
        # ssl_keys (Failed to open ssl.keystore.location: ./fixtures/ssl/client.keystore.p12: error:10000080:BIO routines::no such file)
        133,
        # barrier_batch_consume (Expected offset should be 7, but it is 9)
        137,

        #      tests that are flaky in CI
        # Autotopic (Broker: Invalid topic)
        7,
        # produce_batch
        11,
        # produce_consume
        12,
        # null_msgs (Broker: Invalid topic)
        13,
        # reconsume (Broker: Invalid topic)
        14,
        # destroy_hang (Broker: Invalid topic)
        20,
        # assign_offset ()
        29,
        # offset_commmit
        30,
        # event (Connect to ipv4#0.0.0.0:65534 failed: Connection refused )
        39,
        # io_event
        40,
        # many topics
        42,
        # (Broker: Invalid topic)
        48,
        # subscribe_adds (Local: Unknown topic)
        50,
        # bsearch
        59,
        # yield (Broker: Invalid topic)
        65,
        # empty topic
        67,
        # produce_retry ( Failed to acquire idempotence PID from broker)
        76,
        # cb_event (Local: Unknown topic)
        83,
        # destroy_flags
        84,
        # idempotence (Broker: Not coordinator)
        90,
        # fetch_queue_backoff
        127,
        # using mocked cluster, not relevant
        105,
        # retry: failing always - requires proper RCA done
        75,
    ])
    return [t for t in range(152) if t not in ignored_tests]


class LibrdkafkaTest(RedpandaTest):
    """
    Execute the librdkafka test suite against redpanda.
    """
    TESTS_DIR = "/opt/librdkafka/tests"
    CONF_FILE = os.path.join(TESTS_DIR, "test.conf")

    def __init__(self, context):
        super(LibrdkafkaTest, self).__init__(context,
                                             num_brokers=3,
                                             extra_rp_conf={
                                                 "auto_create_topics_enabled":
                                                 True,
                                                 "default_topic_partitions": 4
                                             })

    @cluster(num_nodes=4)
    @matrix(test_num=tests_to_run(), kafka_version=["2.4.0", "3.0.0"])
    @skip_debug_mode
    def test_librdkafka(self, test_num, kafka_version):
        tc = LibrdkafkaTestcase(self.test_context, self.redpanda, test_num,
                                kafka_version)
        tc.start()
        tc.wait()

        assert tc.error is None, f"Failure in librdkafka test case {test_num:04}"

    @cluster(num_nodes=3)
    def test_create_role_acl(self):
        client = PythonLibrdkafka(self.redpanda)
        admin = client.get_client()
        ROLE_NAME = "RedpandaRole:foo"
        binding = AclBinding(ResourceType.TOPIC, "*",
                             ResourcePatternType.LITERAL, ROLE_NAME, '*',
                             AclOperation.DESCRIBE, AclPermissionType.ALLOW)

        res = admin.create_acls([binding], request_timeout=10)
        for k in res:
            res[k].result()

        filter = AclBindingFilter(ResourceType.ANY, "*",
                                  ResourcePatternType.ANY, ROLE_NAME, '*',
                                  AclOperation.ANY, AclPermissionType.ALLOW)

        acls = admin.describe_acls(filter).result()
        assert len(acls) == 1, f"Wrong number of acls: {len(acls)}"
        assert acls[
            0].principal == ROLE_NAME, f"Expected principal={ROLE_NAME} got {acls[0].principal}"

    @cluster(num_nodes=3)
    def test_create_bad_acl(self):
        """
        Verify that Redpanda rejects (and librdkafka correctly handles)
        ACL bindings with a bogus principal type
        """
        client = PythonLibrdkafka(self.redpanda)
        admin = client.get_client()
        ROLE_NAME = "InvalidPrefix:foo"
        binding = AclBinding(ResourceType.TOPIC, "*",
                             ResourcePatternType.LITERAL, ROLE_NAME, '*',
                             AclOperation.DESCRIBE, AclPermissionType.ALLOW)

        with expect_exception(
                KafkaException,
                lambda e: "Invalid principal name" in e.args[0].str()):
            res = admin.create_acls([binding], request_timeout=10)
            for k in res:
                res[k].result()

        filter = AclBindingFilter(ResourceType.ANY, "*",
                                  ResourcePatternType.ANY, ROLE_NAME, '*',
                                  AclOperation.ANY, AclPermissionType.ALLOW)

        acls = admin.describe_acls(filter).result()
        assert len(
            acls) == 0, f"Expected no ACLs (binding rejected), got: {acls}"

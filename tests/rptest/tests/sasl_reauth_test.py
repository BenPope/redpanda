# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
import json

from rptest.clients.python_librdkafka import PythonLibrdkafka
from rptest.clients.rpk import RpkTool
from rptest.services.redpanda import SecurityConfig, make_redpanda_service
from rptest.services.cluster import cluster
from rptest.services.redpanda import RedpandaService
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.kerberos import KrbKdc, KrbClient, RedpandaKerberosNode, AuthenticationError, KRB5_CONF_PATH, render_krb5_config, ActiveDirectoryKdc
from rptest.tests.redpanda_kerberos_test import RedpandaKerberosTestBase

from ducktape.utils.util import wait_until

import typing

EXAMPLE_TOPIC = 'foo'

REAUTH_MSG = "SASL reauthentication"
EXPIRY_MSG = "SASL session expired"


def scrape_logs(redpanda: RedpandaService, message: str):
    result = {}
    for node in redpanda.nodes:
        result[node.name] = redpanda.count_log_node(node, message)

    return result


class SASLReauthBase(RedpandaTest):
    def __init__(self,
                 conn_max_reauth: typing.Union[int, None] = 8000,
                 **kwargs):
        security = SecurityConfig()
        security.enable_sasl = True
        super().__init__(
            num_brokers=3,
            security=security,
            extra_rp_conf={'kafka_sasl_max_reauth_ms': conn_max_reauth},
            **kwargs)

        self.su_username, self.su_password, self.su_algorithm = self.redpanda.SUPERUSER_CREDENTIALS

        self.rpk = RpkTool(self.redpanda,
                           username=self.su_username,
                           password=self.su_password,
                           sasl_mechanism=self.su_algorithm)

    def make_su_client(self):
        return PythonLibrdkafka(self.redpanda,
                                username=self.su_username,
                                password=self.su_password,
                                algorithm=self.su_algorithm)


class SASLReauthenticationTest(SASLReauthBase):
    def __init__(self, test_context, **kwargs):
        super().__init__(test_context=test_context,
                         conn_max_reauth=8000,
                         **kwargs)

    @cluster(num_nodes=3)
    def test_scram_reauth(self):
        self.rpk.create_topic(EXAMPLE_TOPIC)
        su_client = self.make_su_client()
        producer = su_client.get_producer()
        producer.poll(0.0)

        for i in range(0, 200):
            producer.poll(1.0)
            producer.produce(topic=EXAMPLE_TOPIC, key='bar', value=str(i))
            time.sleep(0.1)

        producer.flush(timeout=2)

        reauths = scrape_logs(self.redpanda, REAUTH_MSG)
        self.logger.debug(f"SCRAM reauths: {json.dumps(reauths, indent=1)}")
        assert (
            any(reauths[n.name] > 0 for n in self.redpanda.nodes)
        ), f"Expected client reauth on some broker...Reauths: {json.dumps(reauths, indent=1)}"

        exps = scrape_logs(self.redpanda, EXPIRY_MSG)
        assert (
            all(exps[n.name] == 0 for n in self.redpanda.nodes)
        ), f"Client should reauth before session expiry...Expirations: {json.dumps(exps, indent=1)}"


class GSSAPIReauthTest(RedpandaKerberosTestBase):
    def __init__(self, test_context, **kwargs):
        super().__init__(test_context, conn_max_reauth_ms=2000, **kwargs)

    @cluster(num_nodes=5)
    def test_gssapi_reauth(self):
        req_principal = "client"
        fail = False
        self.client.add_primary(primary="client")
        username, password, mechanism = self.redpanda.SUPERUSER_CREDENTIALS
        super_rpk = RpkTool(self.redpanda,
                            username=username,
                            password=password,
                            sasl_mechanism=mechanism)

        client_user_principal = f"User:client"

        # Create a topic that's visible to "client" iff acl = True
        super_rpk.create_topic("needs_acl")
        super_rpk.sasl_allow_principal(client_user_principal,
                                       ["write", "read", "describe"], "topic",
                                       "needs_acl", username, password,
                                       mechanism)

        try:
            self.client.produce(req_principal, "needs_acl")
            pass
        except AuthenticationError:
            assert fail
        except TimeoutError:
            assert fail

        reauths = scrape_logs(self.redpanda, REAUTH_MSG)
        self.logger.debug(f"GSSAPI reauths: {json.dumps(reauths, indent=1)}")
        assert (
            any(reauths[n.name] > 0 for n in self.redpanda.nodes)
        ), f"Expected client reauth on some broker...Reauths: {json.dumps(reauths, indent=1)}"

        exps = scrape_logs(self.redpanda, EXPIRY_MSG)
        assert (
            all(exps[n.name] == 0 for n in self.redpanda.nodes)
        ), f"Client should reauth before session expiry...Expirations: {json.dumps(exps, indent=1)}"


class ReauthDisabledTest(SASLReauthBase):
    """
    Tests that kafka_sasl_max_reauth_ms{null} produces original behavior
    i.e. no reauth, no session expiry
    """
    def __init__(self, test_context, **kwargs):
        super().__init__(test_context=test_context,
                         conn_max_reauth=None,
                         **kwargs)

    @cluster(num_nodes=3)
    def test_scram(self):
        self.rpk.create_topic(EXAMPLE_TOPIC)
        su_client = self.make_su_client()
        producer = su_client.get_producer()
        producer.poll(0.0)

        for i in range(0, 200):
            producer.poll(1.0)
            producer.produce(topic=EXAMPLE_TOPIC, key='bar', value=str(i))
            time.sleep(0.1)

        producer.flush(timeout=2)

        reauths = scrape_logs(self.redpanda, REAUTH_MSG)
        assert (
            all(reauths[n.name] == 0 for n in self.redpanda.nodes)
        ), f"Expected no client reauth...Reauths: {json.dumps(reauths, indent=1)}"

        exps = scrape_logs(self.redpanda, EXPIRY_MSG)
        assert (
            all(exps[n.name] == 0 for n in self.redpanda.nodes)
        ), f"SCRAM sessions should not expire...Expirations: {json.dumps(exps, indent=1)}"

    @cluster(num_nodes=3)
    def test_enable_after_start(self):
        '''
        We should be able to enable reauthentication on a live cluster
        '''
        new_cfg = {'kafka_sasl_max_reauth_ms': 4000}
        self.redpanda.set_cluster_config(new_cfg)

        self.rpk.create_topic(EXAMPLE_TOPIC)
        su_client = self.make_su_client()
        producer = su_client.get_producer()
        producer.poll(0.0)

        for i in range(0, 200):
            producer.poll(1.0)
            producer.produce(topic=EXAMPLE_TOPIC, key='bar', value=str(i))
            time.sleep(0.1)

        producer.flush(timeout=2)

        reauths = scrape_logs(self.redpanda, REAUTH_MSG)
        self.logger.debug(f"SCRAM reauths: {json.dumps(reauths, indent=1)}")
        assert (
            any(reauths[n.name] > 0 for n in self.redpanda.nodes)
        ), f"Expected client reauth on some broker...Reauths: {json.dumps(reauths, indent=1)}"

        exps = scrape_logs(self.redpanda, EXPIRY_MSG)
        assert (
            all(exps[n.name] == 0 for n in self.redpanda.nodes)
        ), f"Client should reauth before session expiry...Expirations: {json.dumps(exps, indent=1)}"

        pass

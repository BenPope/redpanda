# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import socket
import time

from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.mark import parametrize
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until
from rptest.clients.rpk import RpkTool
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.kerberos import KrbKdc, KrbClient
from rptest.services.redpanda import LoggingConfig, RedpandaService, SecurityConfig

LOG_CONFIG = LoggingConfig('info',
                           logger_levels={
                               'security': 'trace',
                               'kafka': 'trace',
                               'admin_api_server': 'trace',
                           })

REALM = "EXAMPLE.COM"


class RedpandaKerberosTestBase(Test):
    """
    Base class for tests that use the Redpanda service with Kerberos
    """
    def __init__(self,
                 test_context,
                 num_nodes=5,
                 sasl_mechanisms=["SCRAM", "GSSAPI"],
                 **kwargs):
        super(RedpandaKerberosTestBase, self).__init__(test_context, **kwargs)

        self.kdc = KrbKdc(test_context, realm=REALM)

        security = SecurityConfig()
        security.enable_sasl = True
        security.sasl_mechanisms = sasl_mechanisms
        self.redpanda = RedpandaService(
            test_context,
            # environment={"KRB5_TRACE": "/dev/stdout"},
            num_brokers=num_nodes - 2,
            log_config=LOG_CONFIG,
            security=security,
            **kwargs)

        self.client = KrbClient(test_context, self.kdc, self.redpanda)

    def _service_principal(self, primary: str, node):
        ip = socket.gethostbyname(node.account.hostname)
        out = node.account.ssh_output(cmd=f"dig -x {ip} +short")
        hostname = out.decode('utf-8').split('\n')[0].removesuffix(".")
        fqdn = node.account.ssh_output(
            cmd=f"host {hostname}").decode('utf-8').split(' ')[0]
        self.logger.warn(f"SERVICE PRINCIPAL: {primary}/{fqdn}@{REALM}")
        return f"{primary}/{fqdn}@{REALM}"

    def _client_principal(self, primary):
        return f"{primary}@{REALM}"

    def _configure_service_node(self, primary: str, node):
        self.redpanda.logger.info(
            f"Configuring Kerberos service '{primary}' on '{node.name}'")
        principal = self._service_principal(primary, node)
        self.kdc.add_principal_randkey(principal)
        self.kdc.ktadd(principal,
                       f"{self.redpanda.PERSISTENT_ROOT}/{primary}.keytab",
                       node)

    def _configure_client_node(self, primary: str, node):
        self.redpanda.logger.info(
            f"Configuring Kerberos client '{primary}' on '{node.name}'")
        principal = self._client_principal(primary)
        self.kdc.add_principal_randkey(principal)
        self.kdc.ktadd(principal,
                       f"{self.redpanda.PERSISTENT_ROOT}/{primary}.keytab",
                       node)

    def setUp(self):
        self.redpanda.logger.info("Starting KDC")
        self.kdc.start()
        self.redpanda.logger.info("Starting Redpanda")
        self.redpanda.start()
        self.redpanda.logger.info("Starting Client")
        self.client.start()

        self.redpanda.logger.info("Setting up krb5.conf on Redpanda nodes")
        for node in self.redpanda.nodes:
            self.client.start_node(node)

        for node in self.redpanda.nodes:
            self._configure_service_node("redpanda", node)

        for node in self.client.nodes:
            self._configure_client_node("client", node)


class RedpandaKerberosTest(RedpandaKerberosTestBase):
    def __init__(self, test_context, **kwargs):
        super(RedpandaKerberosTest, self).__init__(test_context, **kwargs)

    @cluster(num_nodes=5)
    @parametrize(req_principal="client",
                 acl=True,
                 topics=["needs_acl", "always_visible"],
                 fail=False)
    @parametrize(req_principal="client",
                 acl=False,
                 topics=["always_visible"],
                 fail=False)
    @parametrize(req_principal="invalid", acl=False, topics={}, fail=True)
    def test_init(self, req_principal: str, acl: bool, topics: set[str],
                  fail: bool):
        def is_auth_error(msg):
            return b'No Kerberos credentials available' in msg

        feature_name = "kafka_gssapi"
        self.redpanda.logger.info(f"Principals: {self.kdc.list_principals()}")
        admin = Admin(self.redpanda)
        admin.put_feature(feature_name, {"state": "active"})
        self.redpanda.await_feature_active(feature_name, timeout_sec=30)

        username, password, mechanism = self.redpanda.SUPERUSER_CREDENTIALS
        super_rpk = RpkTool(self.redpanda,
                            username=username,
                            password=password,
                            sasl_mechanism=mechanism)

        # Create a topic that's visible to "client" iff acl = True
        client_principal = self._client_principal("client")
        client_user_principal = f"User:{client_principal}"
        super_rpk.sasl_create_user(client_user_principal, "rp123", mechanism)

        super_rpk.create_topic("needs_acl")
        if acl:
            super_rpk.sasl_allow_principal(client_user_principal,
                                           ["write", "read", "describe"],
                                           "topic", "needs_acl", username,
                                           password, mechanism)

        # Create a topic visible to anybody
        super_rpk.create_topic("always_visible")
        super_rpk.sasl_allow_principal("*", ["write", "read", "describe"],
                                       "topic", "always_visible", username,
                                       password, mechanism)

        expected_acls = 3 * (2 if acl else 1)
        wait_until(lambda: super_rpk.acl_list().count('\n') >= expected_acls,
                   5)

        try:
            metadata = self.client.metadata(req_principal)
            self.redpanda.logger.info(f"Metadata (GSSAPI): {metadata}")
            assert len(metadata['brokers']) == 3
            assert {n['topic'] for n in metadata['topics']} == set(topics)
            assert not fail
        except RemoteCommandError as err:
            assert is_auth_error(err.msg)
            assert fail


class RedpandaKerberosLicenseTest(RedpandaKerberosTestBase):
    LICENSE_CHECK_INTERVAL_SEC = 1

    def __init__(self, test_context, num_nodes=3, **kwargs):
        super(RedpandaKerberosLicenseTest,
              self).__init__(test_context,
                             num_nodes=num_nodes,
                             sasl_mechanisms=["SCRAM"],
                             **kwargs)
        self.redpanda.set_environment({
            '__REDPANDA_LICENSE_CHECK_INTERVAL_SEC':
            f'{self.LICENSE_CHECK_INTERVAL_SEC}'
        })

    def _has_license_nag(self):
        return self.redpanda.search_log_any("Enterprise feature(s).*")

    def _license_nag_is_set(self):
        return self.redpanda.search_log_all(
            f"Overriding default license log annoy interval to: {self.LICENSE_CHECK_INTERVAL_SEC}s"
        )

    @cluster(num_nodes=3)
    def test_license_nag(self):
        wait_until(self._license_nag_is_set,
                   timeout_sec=30,
                   err_msg="Failed to set license nag internal")

        self.logger.debug("Ensuring no license nag")
        time.sleep(self.LICENSE_CHECK_INTERVAL_SEC * 2)
        assert not self._has_license_nag()

        self.logger.debug("Setting cluster config")
        self.redpanda.set_cluster_config(
            {"sasl_mechanisms": ["GSSAPI", "SCRAM"]})

        self.logger.debug("Waiting for license nag")
        wait_until(self._has_license_nag,
                   timeout_sec=self.LICENSE_CHECK_INTERVAL_SEC * 2,
                   err_msg="License nag failed to appear")

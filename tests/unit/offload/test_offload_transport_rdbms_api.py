""" TestOffloadTransportRdbmsApi: Unit tests for each Offload Transport RDBMS API.
    LICENSE_TEXT
"""

from unittest import TestCase, main

from goe.offload.offload_messages import OffloadMessages
from goe.offload.factory.offload_transport_rdbms_api_factory import (
    offload_transport_rdbms_api_factory,
)
from tests.unit.test_functions import (
    build_mock_options,
    FAKE_MSSQL_ENV,
    FAKE_NETEZZA_ENV,
    FAKE_ORACLE_ENV,
    FAKE_TERADATA_ENV,
)


class TestOffloadTransportRdbmsApi(TestCase):
    def _get_mock_config(self, mock_env: dict):
        return build_mock_options(mock_env)

    def _non_connecting_tests(self, api):
        def is_list_of_strings(v):
            self.assertIsInstance(v, list)
            if v:
                self.assertIsInstance(v[0], str)

        self.assertIsInstance(api.generate_transport_action(), str)
        self.assertIsInstance(api.get_rdbms_canary_query(), str)
        is_list_of_strings(api.sqoop_rdbms_specific_jvm_overrides([]))
        is_list_of_strings(api.sqoop_rdbms_specific_jvm_overrides(["abc=123"]))
        is_list_of_strings(api.sqoop_rdbms_specific_jvm_table_options(None))
        is_list_of_strings(api.sqoop_rdbms_specific_options())
        is_list_of_strings(
            api.sqoop_rdbms_specific_table_options("owner", "table-name")
        )
        try:
            self.assertIsInstance(api.get_transport_row_source_query_hint_block(), str)
        except NotImplementedError:
            pass
        self.assertIsInstance(api.jdbc_url(), str)
        self.assertIsInstance(api.jdbc_driver_name(), (str, type(None)))

    def test_mssql_ot_rdbms_api(self):
        """Simple check that we can instantiate the MSSQL API"""
        messages = OffloadMessages()
        config = self._get_mock_config(FAKE_MSSQL_ENV)
        rdbms_owner = "SH_TEST"
        rdbms_table = "SALES"
        api = offload_transport_rdbms_api_factory(
            rdbms_owner, rdbms_table, config, messages
        )
        self._non_connecting_tests(api)

    def test_netezza_ot_rdbms_api(self):
        """Simple check that we can instantiate the Netezza API"""
        messages = OffloadMessages()
        config = self._get_mock_config(FAKE_NETEZZA_ENV)
        rdbms_owner = "SH_TEST"
        rdbms_table = "SALES"
        api = offload_transport_rdbms_api_factory(
            rdbms_owner, rdbms_table, config, messages
        )
        self._non_connecting_tests(api)

    def test_oracle_ot_rdbms_api(self):
        """Simple check that we can instantiate the Oracle API"""
        messages = OffloadMessages()
        config = self._get_mock_config(FAKE_ORACLE_ENV)
        rdbms_owner = "SH_TEST"
        rdbms_table = "SALES"
        api = offload_transport_rdbms_api_factory(
            rdbms_owner, rdbms_table, config, messages
        )
        self._non_connecting_tests(api)

    def test_teradata_ot_rdbms_api(self):
        """Simple check that we can instantiate the Teradata API"""
        messages = OffloadMessages()
        config = self._get_mock_config(FAKE_TERADATA_ENV)
        rdbms_owner = "SH_TEST"
        rdbms_table = "SALES"
        api = offload_transport_rdbms_api_factory(
            rdbms_owner, rdbms_table, config, messages
        )
        self._non_connecting_tests(api)


if __name__ == "__main__":
    main()

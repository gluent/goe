""" TestOffloadTransportRdbmsApi: Unit tests for each Offload Transport RDBMS API.
    LICENSE_TEXT
"""

from unittest import TestCase, main

from tests.offload.unittest_functions import build_current_options, build_non_connecting_options,\
    get_real_frontend_schema_and_table

from gluentlib.offload.offload_constants import DBTYPE_MSSQL, DBTYPE_NETEZZA, DBTYPE_ORACLE, DBTYPE_TERADATA
from gluentlib.offload.offload_messages import OffloadMessages
from gluentlib.offload.factory.offload_transport_rdbms_api_factory import offload_transport_rdbms_api_factory


class TestOffloadTransportRdbmsApi(TestCase):

    def _non_connecting_tests(self, api):
        def is_list_of_strings(v):
            self.assertIsInstance(v, list)
            if v:
                self.assertIsInstance(v[0], str)

        self.assertIsInstance(api.generate_transport_action(), str)
        self.assertIsInstance(api.get_rdbms_canary_query(), str)
        self.assertIsInstance(api.get_rdbms_session_setup_hint({}, 6), str)
        is_list_of_strings(api.sqoop_rdbms_specific_jvm_overrides([]))
        is_list_of_strings(api.sqoop_rdbms_specific_jvm_overrides(['abc=123']))
        is_list_of_strings(api.sqoop_rdbms_specific_jvm_table_options(None))
        is_list_of_strings(api.sqoop_rdbms_specific_options())
        is_list_of_strings(api.sqoop_rdbms_specific_table_options('owner', 'table-name'))
        try:
            self.assertIsInstance(api.get_transport_row_source_query_hint_block(), str)
        except NotImplementedError:
            pass
        self.assertIsInstance(api.jdbc_url(), str)
        self.assertIsInstance(api.jdbc_driver_name(), (str, type(None)))

    def test_mssql_ot_rdbms_api(self):
        """ Simple check that we can instantiate the MSSQL API """
        messages = OffloadMessages()
        config = build_non_connecting_options(DBTYPE_MSSQL)
        rdbms_owner = 'SH_TEST'
        rdbms_table = 'SALES'
        api = offload_transport_rdbms_api_factory(rdbms_owner, rdbms_table, config, messages)
        self._non_connecting_tests(api)

    def test_netezza_ot_rdbms_api(self):
        """ Simple check that we can instantiate the Netezza API """
        messages = OffloadMessages()
        config = build_non_connecting_options(DBTYPE_NETEZZA)
        rdbms_owner = 'SH_TEST'
        rdbms_table = 'SALES'
        api = offload_transport_rdbms_api_factory(rdbms_owner, rdbms_table, config, messages)
        self._non_connecting_tests(api)

    def test_oracle_ot_rdbms_api(self):
        """ Simple check that we can instantiate the Oracle API """
        messages = OffloadMessages()
        config = build_non_connecting_options(DBTYPE_ORACLE)
        rdbms_owner = 'SH_TEST'
        rdbms_table = 'SALES'
        api = offload_transport_rdbms_api_factory(rdbms_owner, rdbms_table, config, messages)
        self._non_connecting_tests(api)

    def test_teradata_ot_rdbms_api(self):
        """ Simple check that we can instantiate the Teradata API """
        messages = OffloadMessages()
        config = build_non_connecting_options(DBTYPE_TERADATA)
        rdbms_owner = 'SH_TEST'
        rdbms_table = 'SALES'
        api = offload_transport_rdbms_api_factory(rdbms_owner, rdbms_table, config, messages)
        self._non_connecting_tests(api)

    def test_current_ot_rdbms_api(self):
        """ Simple check that we can instantiate the current API including making a frontend connection """
        messages = OffloadMessages()
        config = build_current_options()
        rdbms_owner, rdbms_table = get_real_frontend_schema_and_table('CHANNELS', config, messages=messages)
        api = offload_transport_rdbms_api_factory(rdbms_owner, rdbms_table, config, messages)
        try:
            self.assertIsNotNone(api.get_rdbms_scn())
        except NotImplementedError:
            pass


if __name__ == '__main__':
    main()

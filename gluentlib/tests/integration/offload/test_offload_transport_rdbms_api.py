from unittest import TestCase, main

from tests.integration.offload.unittest_functions import (
    build_current_options,
    build_non_connecting_options,
    get_real_frontend_schema_and_table,
)

from gluentlib.offload.offload_constants import (
    DBTYPE_MSSQL,
    DBTYPE_NETEZZA,
    DBTYPE_ORACLE,
    DBTYPE_TERADATA,
)
from gluentlib.offload.offload_messages import OffloadMessages
from gluentlib.offload.factory.offload_transport_rdbms_api_factory import (
    offload_transport_rdbms_api_factory,
)

from tests.unit.offload.test_offload_transport_rdbms_api import (
    TestOffloadTransportRdbmsApi,
)


class TestOffloadTransportRdbmsApiConnected(TestOffloadTransportRdbmsApi):
    def _connecting_tests(self, api):
        self.assertIsInstance(api.get_rdbms_session_setup_hint({}, 6), str)

    def test_current_ot_rdbms_api(self):
        """Simple check that we can instantiate the current API including making a frontend connection"""
        messages = OffloadMessages()
        config = build_current_options()
        rdbms_owner, rdbms_table = get_real_frontend_schema_and_table(
            "CHANNELS", config, messages=messages
        )
        api = offload_transport_rdbms_api_factory(
            rdbms_owner, rdbms_table, config, messages
        )
        try:
            self.assertIsNotNone(api.get_rdbms_scn())
        except NotImplementedError:
            pass
        self._connecting_tests(api)
        self._non_connecting_tests(api)


if __name__ == "__main__":
    main()



from datetime import datetime
from unittest import TestCase, main
import re

from numpy import datetime64

from gluentlib.offload.offload_constants import DBTYPE_SPARK
from tests.integration.test_functions import build_current_options
from tests.unit.offload.unittest_functions import get_default_test_user
from tests.unit.offload.test_backend_api import TestBackendApi


class TestCurrentBackendApi(TestBackendApi):

    def setUp(self):
        self.connect_to_backend = True
        self.config = self._build_current_options()
        super(TestCurrentBackendApi, self).setUp()

    def _build_current_options(self):
        orchestration_options = build_current_options()
        self.target = orchestration_options.target
        return orchestration_options

    def test_full_api_on_current_backend(self):
        self._run_all_tests()


class TestConnectedSparkBackendApi(TestBackendApi):

    def setUp(self):
        self.connect_to_backend = True
        self.config = self._build_current_options()
        if self._thrift_configured(self.config):
            super(TestConnectedSparkBackendApi, self).setUp()

    def _thrift_configured(self, orchestration_options):
        return bool(orchestration_options.offload_transport_spark_thrift_host
                    and orchestration_options.offload_transport_spark_thrift_port)

    def _build_current_options(self):
        orchestration_options = build_current_options()
        orchestration_options.target = DBTYPE_SPARK
        self.target = orchestration_options.target
        return orchestration_options

    def test_full_api_on_current_backend(self):
        if self._thrift_configured(self.config):
            self._run_all_tests()


if __name__ == '__main__':
    main()

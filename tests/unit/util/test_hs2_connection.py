# Copyright 2016 The GOE Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" TestHs2Connection: Unit test library to test functions from hs2_connection module.
"""
from argparse import Namespace
import os
from unittest import TestCase, main, mock
from tests.unit.test_functions import (
    FAKE_ORACLE_HIVE_ENV,
    optional_hadoop_dependency_exception,
)

try:
    from goe.util.hs2_connection import hs2_connect_using_options, hs2_cursor_user
except ModuleNotFoundError as e:
    if optional_hadoop_dependency_exception(e):
        hs2_connect_using_options = None
        hs2_cursor_user = None
    else:
        raise


class TestHs2Connection(TestCase):
    @staticmethod
    def _hs2_empty_dict():
        return {
            "ldap_user": "a-user",
            "ldap_password": None,
            "ldap_password_file": None,
            "hiveserver2_auth_mechanism": "NOSASL",
            "kerberos_service": None,
        }

    @staticmethod
    def _hs2_ldap_dict():
        return {
            "ldap_user": "a-user",
            "ldap_password": "a-password",
            "ldap_password_file": "/some/file",
        }

    @staticmethod
    def _hs2_plain_dict():
        return {"hiveserver2_auth_mechanism": "PLAIN"}

    @staticmethod
    def _hs2_kerberos_dict():
        return {"kerberos_service": "hive-service"}

    @staticmethod
    def _merge_dicts(d1, d2):
        new = d1.copy()
        new.update(d2)
        return new

    def test_hs2_connect_using_options(self):
        if not hs2_connect_using_options:
            return

        empty_dict = self._hs2_empty_dict()
        ldap_dict = self._merge_dicts(empty_dict, self._hs2_ldap_dict())
        plain_dict = self._merge_dicts(empty_dict, self._hs2_plain_dict())
        krb_dict = self._merge_dicts(empty_dict, self._hs2_kerberos_dict())

        self.assertRaises(AssertionError, lambda: hs2_connect_using_options(ldap_dict))
        self.assertRaises(
            AssertionError,
            lambda: hs2_connect_using_options(ldap_dict, ldap=True, kerberos=True),
        )
        self.assertIs(hs2_connect_using_options(ldap_dict, ldap=True), True)
        self.assertIs(hs2_connect_using_options(ldap_dict, unsecured=True), False)
        self.assertIs(hs2_connect_using_options(empty_dict, ldap=True), False)
        self.assertIs(hs2_connect_using_options(empty_dict, plain=True), False)
        k = mock.patch.dict(os.environ, FAKE_ORACLE_HIVE_ENV)
        k.start()
        self.assertIs(hs2_connect_using_options(plain_dict, plain=True), True)
        self.assertIs(hs2_connect_using_options(plain_dict, unsecured=True), False)
        k.stop()
        self.assertIs(hs2_connect_using_options(krb_dict, kerberos=True), True)
        self.assertIs(hs2_connect_using_options(krb_dict, unsecured=True), False)
        self.assertIs(hs2_connect_using_options(empty_dict, kerberos=True), False)
        self.assertIs(hs2_connect_using_options(empty_dict, unsecured=True), True)

        empty_opts = Namespace(**empty_dict)
        ldap_opts = Namespace(**ldap_dict)
        plain_opts = Namespace(**plain_dict)
        krb_opts = Namespace(**krb_dict)
        self.assertRaises(AssertionError, lambda: hs2_connect_using_options(ldap_opts))
        self.assertRaises(
            AssertionError,
            lambda: hs2_connect_using_options(ldap_opts, ldap=True, unsecured=True),
        )
        self.assertIs(hs2_connect_using_options(empty_opts, unsecured=True), True)
        self.assertIs(hs2_connect_using_options(ldap_opts, ldap=True), True)
        self.assertIs(hs2_connect_using_options(ldap_opts, unsecured=True), False)
        k = mock.patch.dict(os.environ, FAKE_ORACLE_HIVE_ENV)
        k.start()
        self.assertIs(hs2_connect_using_options(plain_opts, plain=True), True)
        self.assertIs(hs2_connect_using_options(plain_opts, unsecured=True), False)
        k.stop()
        self.assertIs(hs2_connect_using_options(krb_opts, kerberos=True), True)
        self.assertIs(hs2_connect_using_options(krb_opts, unsecured=True), False)

    def test_hs2_cursor_user(self):
        if not hs2_cursor_user:
            return

        empty_dict = self._hs2_empty_dict()
        ldap_dict = self._merge_dicts(empty_dict, self._hs2_ldap_dict())
        plain_dict = self._merge_dicts(empty_dict, self._hs2_plain_dict())
        krb_dict = self._merge_dicts(empty_dict, self._hs2_kerberos_dict())
        empty_opts = Namespace(**empty_dict)
        ldap_opts = Namespace(**ldap_dict)
        plain_opts = Namespace(**plain_dict)
        krb_opts = Namespace(**krb_dict)
        # Test with options
        self.assertIsNone(hs2_cursor_user(ldap_opts))
        self.assertIsNone(hs2_cursor_user(plain_opts))
        self.assertIsNone(hs2_cursor_user(krb_opts))
        k = mock.patch.dict(os.environ, FAKE_ORACLE_HIVE_ENV)
        k.start()
        self.assertIsNotNone(hs2_cursor_user(empty_opts))
        k.stop()
        # Test from env, we can't actually test specifics because we don't know what's in os.environ
        # Just make sure we can call it without an exception
        k = mock.patch.dict(os.environ, FAKE_ORACLE_HIVE_ENV)
        k.start()
        hs2_cursor_user()
        k.stop()


if __name__ == "__main__":
    main()

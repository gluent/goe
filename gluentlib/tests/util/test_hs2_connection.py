""" TestHs2Connection: Unit test library to test functions from hs2_connection module.
"""
from argparse import Namespace
import os
from unittest import TestCase, main

from gluentlib.util.hs2_connection import hs2_connect_using_options, hs2_cursor_user


class TestHs2Connection(TestCase):

    @staticmethod
    def _hs2_empty_dict():
        return {'ldap_user': 'a-user', 'ldap_password': None, 'ldap_password_file': None,
                'hiveserver2_auth_mechanism': 'NOSASL',
                'kerberos_service': None}

    @staticmethod
    def _hs2_ldap_dict():
        return {'ldap_user': 'a-user', 'ldap_password': 'a-password', 'ldap_password_file': '/some/file'}

    @staticmethod
    def _hs2_plain_dict():
        return {'hiveserver2_auth_mechanism': 'PLAIN'}

    @staticmethod
    def _hs2_kerberos_dict():
        return {'kerberos_service': 'hive-service'}

    @staticmethod
    def _merge_dicts(d1, d2):
        new = d1.copy()
        new.update(d2)
        return new

    def test_hs2_connect_using_options(self):
        empty_dict = self._hs2_empty_dict()
        ldap_dict = self._merge_dicts(empty_dict, self._hs2_ldap_dict())
        plain_dict = self._merge_dicts(empty_dict, self._hs2_plain_dict())
        krb_dict = self._merge_dicts(empty_dict, self._hs2_kerberos_dict())

        self.assertRaises(AssertionError, lambda: hs2_connect_using_options(ldap_dict))
        self.assertRaises(AssertionError, lambda: hs2_connect_using_options(ldap_dict, ldap=True, kerberos=True))
        self.assertIs(hs2_connect_using_options(ldap_dict, ldap=True), True)
        self.assertIs(hs2_connect_using_options(empty_dict, ldap=True), False)
        if os.environ.get('HIVE_SERVER_USER'):
            self.assertIs(hs2_connect_using_options(plain_dict, plain=True), True)
        self.assertIs(hs2_connect_using_options(empty_dict, plain=True), False)
        self.assertIs(hs2_connect_using_options(krb_dict, kerberos=True), True)
        self.assertIs(hs2_connect_using_options(empty_dict, kerberos=True), False)
        self.assertIs(hs2_connect_using_options(empty_dict, unsecured=True), True)
        self.assertIs(hs2_connect_using_options(ldap_dict, unsecured=True), False)
        if os.environ.get('HIVE_SERVER_USER'):
            self.assertIs(hs2_connect_using_options(plain_dict, unsecured=True), False)
        self.assertIs(hs2_connect_using_options(krb_dict, unsecured=True), False)

        empty_opts = Namespace(**empty_dict)
        ldap_opts = Namespace(**ldap_dict)
        plain_opts = Namespace(**plain_dict)
        krb_opts = Namespace(**krb_dict)
        self.assertRaises(AssertionError, lambda: hs2_connect_using_options(ldap_opts))
        self.assertRaises(AssertionError, lambda: hs2_connect_using_options(ldap_opts, ldap=True, unsecured=True))
        self.assertIs(hs2_connect_using_options(ldap_opts, ldap=True), True)
        if os.environ.get('HIVE_SERVER_USER'):
            self.assertIs(hs2_connect_using_options(plain_opts, plain=True), True)
        self.assertIs(hs2_connect_using_options(krb_opts, kerberos=True), True)
        self.assertIs(hs2_connect_using_options(empty_opts, unsecured=True), True)
        self.assertIs(hs2_connect_using_options(ldap_opts, unsecured=True), False)
        if os.environ.get('HIVE_SERVER_USER'):
            self.assertIs(hs2_connect_using_options(plain_opts, unsecured=True), False)
        self.assertIs(hs2_connect_using_options(krb_opts, unsecured=True), False)

    def test_hs2_cursor_user(self):
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
        if os.environ.get('HIVE_SERVER_USER'):
            self.assertIsNotNone(hs2_cursor_user(empty_opts))
        # Test from env, we can't actually test specifics because we don't know what's in os.environ
        # Just make sure we can call it without an exception
        hs2_cursor_user()


if __name__ == '__main__':
    main()

#! /usr/bin/env python3
""" Test module for gluentlib.util.linux_cmd
"""

import fcntl
import getpass
import os
import socket
import struct
import unittest

from teamcity import is_running_under_teamcity
from teamcity.unittestpy import TeamcityTestRunner

from gluentlib.util.linux_cmd import LinuxCmd, LinuxCmdException


class TestLinuxCmd(unittest.TestCase):

    def get_ip_address(self, ifname):
        assert isinstance(ifname, str)
        packed_ifname = struct.pack('256s', ifname[:15].encode())
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            ip32bit = fcntl.ioctl(
                s.fileno(),
                0x8915,  # SIOCGIFADDR
                packed_ifname
            )[20:24]
        return socket.inet_ntoa(ip32bit)

    def get_interface(self):
        """ Just get the first interface on the host to avoid hard coding eth0 throughout """
        ifs = os.listdir('/sys/class/net/')
        if 'eth0' in ifs:
            return 'eth0'
        return ifs[0]

    def setUp(self):
        self.cmd = LinuxCmd()

        self.current_user = getpass.getuser()
        self.test_user = 'gluent'
        self.bad_user = 'thisisabaduser'

        self.enabled_host = self.get_ip_address(self.get_interface())
        self.disabled_host = 'www.oracle.com'
        self.not_existing_host = 'not.existing.host'

    ###############################################################
    # Testing 'cmd' basics
    ###############################################################

    def test_cmd_not_supplied(self):
        with self.assertRaises(TypeError):
            self.cmd.execute() 
        with self.assertRaises(LinuxCmdException):
            self.cmd.execute(cmd="")

    def test_cmd_good(self):
        self.cmd.execute("echo 'proper_execution'")
        self.assertEqual(self.cmd.stdout, b"proper_execution")
        self.assertIsNone(self.cmd.stderr)
        self.assertEqual(self.cmd.returncode, 0)
        self.assertTrue(self.cmd.success)

    def test_cmd_bad(self):
        self.cmd.execute("datea")
        self.assertIsNone(self.cmd.stdout)
        self.assertIsNotNone(self.cmd.stderr)
        self.assertEqual(self.cmd.returncode, 127)
        self.assertFalse(self.cmd.success)

    def test_pipe_cmd_good(self):
        self.cmd.execute('a=$(echo "proper_execution"); echo $a')
        self.assertEqual(self.cmd.stdout, b"proper_execution")
        self.assertIsNone(self.cmd.stderr)
        self.assertEqual(self.cmd.returncode, 0)
        self.assertTrue(self.cmd.success)

    def test_pipe_cmd_remote_good(self):
        self.cmd.execute('a=$(/bin/echo \"proper_execution\"); /bin/echo $a',
                         user=self.test_user, host=self.enabled_host)
        self.assertEqual(self.cmd.stdout, b"proper_execution")
        self.assertEqual(self.cmd.returncode, 0)
        self.assertTrue(self.cmd.success)

    def test_pipe_cmd_bad(self):
        self.cmd.execute('a=$(/bin/echo \"proper_execution\"); /bin/echoa $a')
        self.assertIsNone(self.cmd.stdout)
        self.assertIsNotNone(self.cmd.stderr)
        self.assertEqual(self.cmd.returncode, 127)
        self.assertFalse(self.cmd.success)

    def test_pipe_cmd_remote_bad(self):
        self.cmd.execute('a=$(echo "proper_execution"); echoa $a',
                         user=self.test_user, host=self.enabled_host)
        self.assertIsNotNone(self.cmd.stderr)
        self.assertNotEqual(self.cmd.returncode, 0)
        self.assertFalse(self.cmd.success)

    def test_cmd_no_permissions(self):
        self.cmd.execute('cat /proc/1/environ | wc -l')
        self.assertIsNotNone(self.cmd.stderr)
        self.assertEqual(self.cmd.returncode, 1)
        self.assertFalse(self.cmd.success)

    def test_cmd_no_permissions_remote(self):
        self.cmd.execute('cat /proc/1/environ | wc -l', user=self.test_user, host=self.enabled_host)
        self.assertIsNotNone(self.cmd.stderr)
        self.assertEqual(self.cmd.returncode, 1)
        self.assertFalse(self.cmd.success)

    ###############################################################
    # Testing 'add environment'
    ###############################################################

    def test_add_environment(self):
        self.cmd.execute('echo $MARKER', environment={'MARKER': "proper_execution"})
        self.assertEqual(self.cmd.stdout, b"proper_execution")
        self.assertIsNone(self.cmd.stderr)
        self.assertEqual(self.cmd.returncode, 0)
        self.assertTrue(self.cmd.success)

    def test_add_environment_different_user(self):
        self.cmd.execute('echo $MARKER', environment={'MARKER': "proper_execution"}, user=self.test_user)
        self.assertEqual(self.cmd.stdout, b"proper_execution")
        self.assertEqual(self.cmd.returncode, 0)
        self.assertTrue(self.cmd.success)
        self.assertTrue(self.cmd.host, 'localhost')
        self.assertTrue(self.cmd.user, self.test_user)

    def test_add_environment_different_host(self):
        self.cmd.execute('echo $MARKER', environment={'MARKER': "proper_execution"},
                         user=self.test_user, host=self.enabled_host)
        self.assertEqual(self.cmd.stdout, b"proper_execution")
        self.assertEqual(self.cmd.returncode, 0)
        self.assertTrue(self.cmd.success)
        self.assertTrue(self.cmd.host, self.enabled_host)
        self.assertTrue(self.cmd.user, self.current_user)

    def test_neg_environment(self):
        self.cmd.execute('echo $MARKER', environment={'MARKER2': "proper_execution"})
        self.assertEqual(self.cmd.stdout, b'')
        self.assertIsNone(self.cmd.stderr)
        self.assertEqual(self.cmd.returncode, 0)
        self.assertTrue(self.cmd.success)

    def test_neg_environment_different_user(self):
        self.cmd.execute('echo $MARKER', environment={'MARKER2': "proper_execution"}, user=self.test_user)
        self.assertEqual(self.cmd.stdout, b'')
        self.assertEqual(self.cmd.returncode, 0)
        self.assertTrue(self.cmd.success)
        self.assertTrue(self.cmd.host, 'localhost')
        self.assertTrue(self.cmd.user, self.test_user)

    def test_neg_environment_different_host(self):
        self.cmd.execute('echo $MARKER', environment={'MARKER2': "proper_execution"},
                         user=self.test_user, host=self.enabled_host)
        self.assertEqual(self.cmd.returncode, 0)
        self.assertTrue(self.cmd.success)
        self.assertTrue(self.cmd.host, self.enabled_host)
        self.assertTrue(self.cmd.user, self.current_user)

    ###############################################################
    # Testing 'user'
    ###############################################################

    def test_user_not_supplied(self):
        self.cmd.execute('echo MARKER')
        self.assertEqual(self.cmd.stdout, b'MARKER')
        self.assertIsNone(self.cmd.stderr)
        self.assertEqual(self.cmd.returncode, 0)
        self.assertTrue(self.cmd.success)
        self.assertIsNone(self.cmd.user)
        self.assertIsNone(self.cmd.host)

    def test_user_current_local_good(self):
        self.cmd.execute('echo MARKER', host='localhost')
        self.assertEqual(self.cmd.stdout, b'MARKER')
        self.assertEqual(self.cmd.returncode, 0)
        self.assertTrue(self.cmd.success)
        self.assertEqual(self.cmd.user, self.current_user)
        self.assertEqual(self.cmd.host, 'localhost')

    def test_user_test_local_good(self):
        self.cmd.execute('echo MARKER', user=self.test_user)
        self.assertEqual(self.cmd.stdout, b'MARKER')
        self.assertEqual(self.cmd.returncode, 0)
        self.assertTrue(self.cmd.success)
        self.assertEqual(self.cmd.user, self.test_user)
        self.assertEqual(self.cmd.host, 'localhost')

    def test_user_test_local_bad(self):
        self.cmd.execute('echo MARKER', user=self.bad_user)
        self.assertIsNotNone(self.cmd.stderr)
        self.assertNotEqual(self.cmd.returncode, 0)
        self.assertFalse(self.cmd.success)
        self.assertEqual(self.cmd.user, self.bad_user)
        self.assertEqual(self.cmd.host, 'localhost')

    def test_user_test_remote_good(self):
        self.cmd.execute('echo MARKER', user=self.test_user, host=self.enabled_host)
        self.assertEqual(self.cmd.stdout, b'MARKER')
        self.assertEqual(self.cmd.returncode, 0)
        self.assertTrue(self.cmd.success)
        self.assertEqual(self.cmd.user, self.test_user)
        self.assertEqual(self.cmd.host, self.enabled_host)

    def test_user_test_remote_bad(self):
        self.cmd.execute('echo MARKER', user=self.bad_user, host=self.enabled_host)
        self.assertIsNotNone(self.cmd.stderr)
        self.assertNotEqual(self.cmd.returncode, 0)
        self.assertFalse(self.cmd.success)
        self.assertEqual(self.cmd.user, self.bad_user)
        self.assertEqual(self.cmd.host, self.enabled_host)

    ###############################################################
    # Testing 'host'
    ###############################################################

    def test_host_not_supplied(self):
        self.cmd.execute('echo MARKER')
        self.assertEqual(self.cmd.stdout, b'MARKER')
        self.assertEqual(self.cmd.returncode, 0)
        self.assertTrue(self.cmd.success)
        self.assertIsNone(self.cmd.user)
        self.assertIsNone(self.cmd.host)

    def test_host_local(self):
        self.cmd.execute('echo MARKER', host='localhost')
        self.assertEqual(self.cmd.stdout, b'MARKER')
        self.assertEqual(self.cmd.returncode, 0)
        self.assertTrue(self.cmd.success)
        self.assertEqual(self.cmd.user, self.current_user)
        self.assertEqual(self.cmd.host, 'localhost')

    def test_host_local_different_user(self):
        self.cmd.execute('echo MARKER', host='localhost', user=self.test_user)
        self.assertEqual(self.cmd.stdout, b'MARKER')
        self.assertEqual(self.cmd.returncode, 0)
        self.assertTrue(self.cmd.success)
        self.assertEqual(self.cmd.user, self.test_user)
        self.assertEqual(self.cmd.host, 'localhost')

    def test_host_not_supplied_different_user(self):
        self.cmd.execute('echo MARKER', user=self.test_user)
        self.assertEqual(self.cmd.stdout, b'MARKER')
        self.assertEqual(self.cmd.returncode, 0)
        self.assertTrue(self.cmd.success)
        self.assertEqual(self.cmd.user, self.test_user)
        self.assertEqual(self.cmd.host, 'localhost')

    def test_host_remote_enabled_good(self):
        self.cmd.execute('echo MARKER', user=self.test_user, host=self.enabled_host)
        self.assertEqual(self.cmd.stdout, b'MARKER')
        self.assertEqual(self.cmd.returncode, 0)
        self.assertTrue(self.cmd.success)
        self.assertEqual(self.cmd.user, self.test_user)
        self.assertEqual(self.cmd.host, self.enabled_host)

    def test_host_remote_enabled_bad(self):
        self.cmd.execute('echoa MARKER', user=self.test_user, host=self.enabled_host)
        self.assertIsNotNone(self.cmd.stderr)
        self.assertNotEqual(self.cmd.returncode, 0)
        self.assertFalse(self.cmd.success)
        self.assertEqual(self.cmd.user, self.test_user)
        self.assertEqual(self.cmd.host, self.enabled_host)

    def test_host_remote_disabled_good(self):
        self.cmd.execute('echo MARKER', host=self.disabled_host)
        self.assertIsNotNone(self.cmd.stderr)
        self.assertNotEqual(self.cmd.returncode, 0)
        self.assertFalse(self.cmd.success)
        self.assertEqual(self.cmd.user, self.current_user)
        self.assertEqual(self.cmd.host, self.disabled_host)

    def test_host_remote_disabled_bad(self):
        self.cmd.execute('echoa MARKER', host=self.disabled_host)
        self.assertIsNotNone(self.cmd.stderr)
        self.assertNotEqual(self.cmd.returncode, 0)
        self.assertFalse(self.cmd.success)
        self.assertEqual(self.cmd.user, self.current_user)
        self.assertEqual(self.cmd.host, self.disabled_host)

    def test_host_remote_not_existing_good(self):
        self.cmd.execute('echo MARKER', host=self.not_existing_host)
        self.assertIsNotNone(self.cmd.stderr)
        self.assertNotEqual(self.cmd.returncode, 0)
        self.assertFalse(self.cmd.success)
        self.assertEqual(self.cmd.user, self.current_user)
        self.assertEqual(self.cmd.host, self.not_existing_host)

    def test_host_remote_not_existing_bad(self):
        self.cmd.execute('echoa MARKER', host=self.not_existing_host)
        self.assertIsNotNone(self.cmd.stderr)
        self.assertNotEqual(self.cmd.returncode, 0)
        self.assertFalse(self.cmd.success)
        self.assertEqual(self.cmd.user, self.current_user)
        self.assertEqual(self.cmd.host, self.not_existing_host)


if __name__ == '__main__':
    if is_running_under_teamcity():
        runner = TeamcityTestRunner()
    else:
        runner = unittest.TextTestRunner()
    #unittest.main(testRunner=runner)

    suite = unittest.TestLoader().loadTestsFromTestCase(TestLinuxCmd)
    runner.run(suite)

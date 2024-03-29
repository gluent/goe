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

""" TestPasswordTools: Unit test library to test PasswordTools functionality
"""
import os
from unittest import TestCase, main

from goe.util.password_tools import PasswordTools


class TestPasswordTools(TestCase):
    def test_password_tools_known_key(self):
        goe_key = "E7264F9B4E92048857611F310470C7C305AF262EE09BEEBE28B46C219F9697398E3040E24C1B4FB38049678E0E77C0EC"
        pass_tool = PasswordTools()
        clear_text_pwd = "A Big Secret"
        encrypted_pwd = pass_tool.encrypt(clear_text_pwd, goe_key)
        decrypted_pwd = pass_tool.decrypt(encrypted_pwd, goe_key)
        self.assertEqual(decrypted_pwd, clear_text_pwd)

    def test_password_tools_key_file(self):
        """If PASSWORD_KEY_FILE is set in the environment then test using the contents of that"""
        if os.environ.get("PASSWORD_KEY_FILE"):
            pass_tool = PasswordTools()
            clear_text_pwd = "A Big Secret"
            encrypted_pwd = pass_tool.encrypt(clear_text_pwd)
            decrypted_pwd = pass_tool.decrypt(encrypted_pwd)
            self.assertEqual(decrypted_pwd, clear_text_pwd)


if __name__ == "__main__":
    main()

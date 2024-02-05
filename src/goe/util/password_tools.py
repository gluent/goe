#! /usr/bin/env python3

# Copyright 2016 The GOE Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" Password management
"""

import logging
import os
from base64 import b64decode
from cryptography.hazmat.primitives import padding, serialization
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes


###############################################################################
# EXCEPTIONS
###############################################################################


class PasswordToolsException(Exception):
    pass


###############################################################################
# CONSTANTS
###############################################################################

###############################################################################
# LOGGING
###############################################################################

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())  # Disabling logging by default


class PasswordTools(object):
    """Password management methods
    Limited logging as don't want anything incriminating in log files.
    """

    def __init__(self):
        """CONSTRUCTOR"""
        self._cipher = None
        logger.debug("Initialized PasswordTools() object")

    ###########################################################################
    # PRIVATE ROUTINES
    ###########################################################################

    def _get_cipher_for_goe_password(self, goe_key):
        """Setup a cipher matching the one used by C++ pass_tool tool
        goe_key is the long key stored in the keyfile (PASSWORD_KEY_FILE)
        """
        assert goe_key

        if not self._cipher:
            logger.debug("Setting up cipher for goe key")
            if len(goe_key) != 96:
                raise PasswordToolsException("Malformed encryption key")

            key = bytes.fromhex(goe_key[:64])
            iv = bytes.fromhex(goe_key[64:])

            backend = default_backend()
            self._cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=backend)

        return self._cipher

    def _get_private_key_from_pem_file(self, pem_file, pem_format, passphrase=None):
        """This code is based on Snowflake instructions for extracting a private key from a PEM file.
        https://docs.snowflake.com/en/user-guide/python-connector-example.html#using-key-pair-authentication
        It is not Snowflake specific so is help in this module in case it is useful for other tasks, however
        any future usage may require use of other options/parameters.
        """
        assert pem_file
        assert isinstance(pem_file, str)
        assert pem_format
        # Currently only supporting PKCS8
        assert pem_format in [serialization.PrivateFormat.PKCS8]

        if passphrase:
            passphrase = passphrase.encode()

        try:
            with open(pem_file, "rb") as reader:
                p_key = serialization.load_pem_private_key(
                    reader.read(), password=passphrase, backend=default_backend()
                )
        except IOError as exc:
            raise PasswordToolsException(
                "PEM file does not exist or is not readable: %s\n%s"
                % (pem_file, str(exc))
            )
        except Exception as exc:
            raise PasswordToolsException(
                "Unable to load private key from PEM file: %s\n%s"
                % (pem_file, str(exc))
            )

        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=pem_format,
            encryption_algorithm=serialization.NoEncryption(),
        )

        return pkb

    ###########################################################################
    # PUBLIC ROUTINES
    ###########################################################################

    def get_password_key_from_key_file(
        self, password_key_file=None, ignore_absent_keyfile=False
    ):
        """Get the contents of the file pointed to by PASSWORD_KEY_FILE
        No-op if PASSWORD_KEY_FILE is not set
        """
        if not password_key_file:
            password_key_file = os.environ.get("PASSWORD_KEY_FILE")
        if not password_key_file:
            return None

        password_key_file = password_key_file.strip()

        try:
            with open(password_key_file, "r") as fh:
                goe_key = fh.read().strip("\r\n")
        except IOError as exc:
            if "No such file or directory" in str(exc):
                if ignore_absent_keyfile:
                    goe_key = None
                else:
                    raise PasswordToolsException(
                        "Decryption string is not base64 encoded"
                    )
            else:
                raise

        if not goe_key:
            raise PasswordToolsException(
                "No password key retrieved from key file: %s" % password_key_file
            )
        return goe_key

    def encrypt(self, clear_text, goe_key=None):
        """Encrypt a string using same algorithm/mode as C++ pass_tool tool"""
        assert clear_text

        logger.debug("Encrypting clear text string")

        if goe_key:
            if isinstance(goe_key, bytes):
                goe_key = goe_key.decode()
        else:
            goe_key = self.get_password_key_from_key_file()

        cipher = self._get_cipher_for_goe_password(goe_key)

        # CBC (Cipher Block Chaining) requires padding
        padder = padding.PKCS7(128).padder()  # 128 bit
        clear_bytes = (
            clear_text if isinstance(clear_text, bytes) else clear_text.encode()
        )
        padded_text = padder.update(clear_bytes) + padder.finalize()
        encryptor = cipher.encryptor()
        enc_text = encryptor.update(padded_text) + encryptor.finalize()
        return enc_text

    def decrypt(self, enc_text, goe_key=None):
        """Decrypt a string using same algorithm/mode as C++ pass_tool tool"""
        assert enc_text

        logger.debug("Decrypting encrypted string")

        if goe_key:
            if isinstance(goe_key, bytes):
                goe_key = goe_key.decode()
        else:
            goe_key = self.get_password_key_from_key_file()

        cipher = self._get_cipher_for_goe_password(goe_key)

        decryptor = cipher.decryptor()
        padded_text = decryptor.update(enc_text) + decryptor.finalize()
        # CBC (Cipher Block Chaining) requires padding
        unpadder = padding.PKCS7(128).unpadder()  # 128 bit
        clear_bytes = unpadder.update(padded_text) + unpadder.finalize()
        clear_text = clear_bytes.decode()
        return clear_text

    def b64decrypt(self, enc_text, goe_key=None):
        """Base 64 decode a password before passing on for decryption"""
        assert enc_text
        try:
            decode_text = b64decode(enc_text)
        except Exception as exc:
            if any(
                _ in str(exc)
                for _ in ["Invalid base64-encoded string", "Incorrect padding"]
            ):
                raise PasswordToolsException("Decryption string is not base64 encoded")
            else:
                raise
        return self.decrypt(decode_text, goe_key=goe_key)

    def get_private_key_from_pkcs8_pem_file(self, pem_file, passphrase=None):
        """This code is based on Snowflake instructions for extracting a private key from a PEM file.
        https://docs.snowflake.com/en/user-guide/python-connector-example.html#using-key-pair-authentication
        It is not Snowflake specific so is help in this module in case it is useful for other tasks.
        """
        return self._get_private_key_from_pem_file(
            pem_file,
            pem_format=serialization.PrivateFormat.PKCS8,
            passphrase=passphrase,
        )


if __name__ == "__main__":
    import sys
    from goe.util.misc_functions import set_goelib_logging
    from base64 import b64encode

    log_level = sys.argv[-1:][0].upper()
    if log_level not in ("DEBUG", "INFO", "WARNING", "CRITICAL", "ERROR"):
        log_level = "CRITICAL"

    set_goelib_logging(log_level)

    pass_tool = PasswordTools()

    if os.environ.get("PASSWORD_KEY_FILE"):
        goe_key = None
    else:
        # any old string will do, this one came from pass_tool tool with passphrase "goe"
        goe_key = "E7264F9B4E92048857611F310470C7C305AF262EE09BEEBE28B46C219F9697398E3040E24C1B4FB38049678E0E77C0EC"

    clear_text_pwd = "A Big Secret"

    print("GOE key:", goe_key)
    encrypted_pwd = pass_tool.encrypt(clear_text_pwd, goe_key)
    print(
        "Encrypted b64:",
        b64encode(encrypted_pwd),
        [ord(c) for c in b64encode(encrypted_pwd)],
    )
    decrypted_pwd = pass_tool.decrypt(encrypted_pwd, goe_key)
    assert decrypted_pwd == clear_text_pwd
    print(decrypted_pwd, "==", clear_text_pwd)

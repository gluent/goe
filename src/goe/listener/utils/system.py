# -*- coding: utf-8 -*-

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

"""
Utility library.
"""

# Standard Library
import fcntl
import os
import socket
import struct


def get_interface_ip(ifname):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(
        fcntl.ioctl(
            s.fileno(),
            0x8915,  # SIOCGIFADDR
            struct.pack("256s", bytes(ifname[:15], "utf-8"))
            # Python 2.7: remove the second argument for the bytes call
        )[20:24]
    )


def get_lan_ip():
    ip = socket.gethostbyname(socket.gethostname())
    if ip.startswith("127.") and os.name != "nt":
        interfaces = [
            "eth0",
            "eth1",
            "eth2",
            "wlan0",
            "wlan1",
            "wifi0",
            "ath0",
            "ath1",
            "ppp0",
        ]
        for ifname in interfaces:
            try:
                ip = get_interface_ip(ifname)
                break
            except IOError:
                pass
    return ip


def get_ip_address():
    """Get IP address of the local host."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # May not work with IPv6
    s.settimeout(0)
    try:
        # doesn't even have to be reachable
        s.connect(("10.0.0.0", 1))
        ip_address = s.getsockname()[0]
    except Exception:
        ip_address = "127.0.0.1"
    finally:
        s.close()
    return ip_address


def get_host_name():
    """Get host name of the local host."""
    # return socket.gethostname()
    return socket.gethostbyname(socket.gethostname())

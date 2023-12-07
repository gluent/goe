# -*- coding: utf-8 -*-
"""
Utility library.
LICENSE_TEXT
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

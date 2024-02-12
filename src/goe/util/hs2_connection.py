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

""" Hs2Connection: HiveServer2 connection primitives
"""

import logging
import os
import random

import impala.dbapi as hs2

from argparse import Namespace

from goe.offload.offload_constants import DBTYPE_HIVE, DBTYPE_IMPALA
from goe.util.password_tools import PasswordTools


###############################################################################
# EXCEPTIONS
###############################################################################
class Hs2ConnectionException(Exception):
    pass


###############################################################################
# CONSTANTS
###############################################################################
DEFAULT_HS2_CONNECTION_TIMEOUT = "1000"  # hs2 connection timeout in seconds

# Default ports
DEFAULT_PORTS = {10000: DBTYPE_HIVE, 21050: DBTYPE_IMPALA}

# List of relevant 'options' for hiveserver
# Basically, all options that are used in this module's routines
# ATTENTION: It's super important to update the list as you add new options
HS2_OPTIONS = (
    "ca_cert",
    "hadoop_host",
    "hadoop_port",
    "hiveserver2_auth_mechanism",
    "hive_timeout_s",
    "hiveserver2_http_path",
    "hiveserver2_http_transport",
    "kerberos_service",
    "ldap_password",
    "ldap_password_file",
    "ldap_user",
    "use_ssl",
)


###############################################################################
# LOGGING
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())  # Disabling logging by default


def hs2_connect_using_options(
    opts, ldap=False, plain=False, kerberos=False, unsecured=False
):
    """Return bool if the options in opts can satisfy the type of connection we're interested in.
    For example if we pass in ldap=True then the fn returns True if we have what we need to
    actually make an LDAP connection.
    opts can be an options object or a dict.
    """
    assert opts
    assert (
        len([_ for _ in [ldap, plain, kerberos, unsecured] if _]) == 1
    ), "Only 1 of ldap, plain, kerberos, unsecured should be set"
    if isinstance(opts, dict):
        ldap_user = opts.get("ldap_user")
        ldap_password = opts.get("ldap_password")
        ldap_password_file = opts.get("ldap_password_file")
        auth_mech = opts.get("hiveserver2_auth_mechanism")
        kerberos_service = opts.get("kerberos_service")
    else:
        ldap_user = opts.ldap_user
        ldap_password = opts.ldap_password
        ldap_password_file = opts.ldap_password_file
        auth_mech = opts.hiveserver2_auth_mechanism
        kerberos_service = opts.kerberos_service
    hive_user = os.environ.get("HIVE_SERVER_USER")
    if ldap_user and (ldap_password or ldap_password_file):
        return bool(ldap)
    elif hive_user and auth_mech == "PLAIN":
        return bool(plain)
    elif kerberos_service:
        return bool(kerberos)
    else:
        return bool(unsecured)


def hs2_connection(orchestration_config, host_override=None, port_override=None):
    """Connect to HiveServer2 using "options" namespace"""
    # TODO: maxym@ 2016-08-28
    # As "options" object could be Class rather than Namespace, its .keys may not be iterable
    # Will need to either add iterable wrapper OR change option Classes to Namespaces
    #
    # REQUIRED_OPTIONS = ['hadoop_host', 'hadoop_port', 'hive_timeout_s', 'use_ssl', 'ca_cert']
    # assert options and all(_ in options for _ in REQUIRED_OPTIONS), \
    #    "Options object should include: %s required keys" % REQUIRED_OPTIONS

    assert orchestration_config
    optvars = vars(
        orchestration_config
    )  # Converting to dict as it's easier to deal with

    if host_override and port_override:
        host, port = host_override, int(port_override)
    else:
        host, port = optvars.get("hadoop_host"), int(optvars.get("hadoop_port"))
    timeout = optvars.get("hive_timeout_s")
    if timeout:
        timeout = int(timeout)
    logger.info("Connecting to HiveServer2: %s:%d" % (host, port))

    # It is important the order of the checks here matches the order in hs2_connect_using_options()
    if hs2_connect_using_options(optvars, ldap=True):
        ldap_user, ldap_password = optvars.get("ldap_user"), optvars.get(
            "ldap_password"
        )
        ldap_password_file = optvars.get("ldap_password_file")
        password_key_file = optvars.get("password_key_file")
        logger.debug("Connecting via LDAP user: %s to HiveServer2" % ldap_user)

        if ldap_password_file:
            with open(ldap_password_file, "r") as fh:
                ldap_password = fh.read().strip("\n")

        if password_key_file:
            pass_tool = PasswordTools()
            goe_key = pass_tool.get_password_key_from_key_file(password_key_file)
            logger.debug('Decrypting "%s" password' % ldap_user)
            ldap_password = pass_tool.b64decrypt(ldap_password, goe_key)

        return hs2.connect(
            host=host,
            port=port,
            use_ssl=optvars.get("use_ssl"),
            ca_cert=optvars.get("ca_cert"),
            auth_mechanism="PLAIN",
            user=ldap_user,
            password=ldap_password,
            timeout=timeout,
            use_http_transport=optvars.get("hiveserver2_http_transport"),
            http_path=optvars.get("hiveserver2_http_path"),
        )

    elif hs2_connect_using_options(optvars, plain=True):
        hive_user = os.environ.get("HIVE_SERVER_USER")
        logger.debug(
            "Connecting via PLAIN authentication with username %s to HiveServer2"
            % hive_user
        )
        return hs2.connect(
            host=host,
            port=port,
            use_ssl=optvars.get("use_ssl"),
            ca_cert=optvars.get("ca_cert"),
            auth_mechanism="PLAIN",
            user=hive_user,
            timeout=timeout,
            use_http_transport=optvars.get("hiveserver2_http_transport"),
            http_path=optvars.get("hiveserver2_http_path"),
        )

    elif hs2_connect_using_options(optvars, kerberos=True):
        kerberos_service = optvars.get("kerberos_service")
        logger.debug(
            "Connecting via Kerberos service: %s to HiveServer2" % kerberos_service
        )
        return hs2.connect(
            host=host,
            port=port,
            use_ssl=optvars.get("use_ssl"),
            ca_cert=optvars.get("ca_cert"),
            auth_mechanism="GSSAPI",
            kerberos_service_name=kerberos_service,
            timeout=timeout,
            use_http_transport=optvars.get("hiveserver2_http_transport"),
            http_path=optvars.get("hiveserver2_http_path"),
        )

    else:
        logger.debug("Establishing unauthenticated connection to HiveServer2")
        return hs2.connect(
            host=host,
            port=port,
            use_ssl=optvars.get("use_ssl"),
            ca_cert=optvars.get("ca_cert"),
            timeout=timeout,
            auth_mechanism=optvars.get("hiveserver2_auth_mechanism"),
            use_http_transport=optvars.get("hiveserver2_http_transport"),
            http_path=optvars.get("hiveserver2_http_path"),
        )


def hs2_env_options(override=None):
    """Populate HiveServer2 options from environment
    with optional override
    """
    options = Namespace()

    options.hadoop_host = os.environ.get("HIVE_SERVER_HOST", "localhost")

    options.target = (
        "hive" if os.environ.get("QUERY_ENGINE").upper() == "HIVE" else "impala"
    )
    default_port = (
        "10000"
        if "HIVE" == os.environ.get("QUERY_ENGINE", "IMPALA").upper()
        else "21050"
    )
    options.hadoop_port = os.environ.get("HIVE_SERVER_PORT", default_port)
    options.backend_distribution = os.environ.get("BACKEND_DISTRIBUTION")

    # Required defaults (which cannot be extracted from environment)
    options.hive_timeout_s = DEFAULT_HS2_CONNECTION_TIMEOUT
    options.ldap_user = os.environ.get("HIVE_SERVER_USER", None)
    options.ldap_password = os.environ.get("HIVE_SERVER_PASS", None)
    options.ldap_password_file = None

    # Environment overrides
    options.use_ssl = (
        True if "true" == os.environ.get("SSL_ACTIVE", "false").lower() else False
    )
    options.ca_cert = os.environ.get("SSL_TRUSTED_CERTS", None)
    options.kerberos_service = os.environ.get("KERBEROS_SERVICE", None)
    logger.debug("HS2 connection options from environment: %s" % options)

    # Overrides ?
    if override:
        options.__dict__.update(override.__dict__)  # Merge overrides

    # Post processing after override (options depending on other options)
    if options.kerberos_service:
        options.hiveserver2_auth_mechanism = "GSSAPI"
    elif options.ldap_user and options.ldap_password:
        options.hiveserver2_auth_mechanism = "PLAIN"
    else:
        if "hiveserver2_auth_mechanism" not in vars(options):
            options.hiveserver2_auth_mechanism = os.environ.get(
                "HIVE_SERVER_AUTH_MECHANISM", "NOSASL"
            )
    logger.debug("HS2 connection options from environment with overrides: %s" % options)

    # If multiple hiveserver2 hosts were supplied ...
    if options.hadoop_host and "," in options.hadoop_host:
        host = random.choice([_.strip() for _ in options.hadoop_host.split(",")])
        logger.debug(
            "Randomly selecting host: %s out of: %s" % (host, options.hadoop_host)
        )
        options.hadoop_host = host

    return options


def hs2_connection_from_env(override=None):
    """Connect to HiveServer2 extracting options from environment
    with opional replacement
    """
    return hs2_connection(hs2_env_options(override))


def hs2_db_type(options=None):
    """Determine and return hiveserver2 'db type'"""

    def from_port(port):
        return DEFAULT_PORTS.get(port, None)

    db_type = (
        (options and vars(options).get("target", None))
        or os.environ.get("QUERY_ENGINE", None)
        or from_port(os.environ.get("HIVE_SERVER_PORT", None))
    )

    if not db_type:
        raise Hs2ConnectionException("Unable to determine db_type")
    else:
        db_type = db_type.lower()

    if db_type not in (DBTYPE_HIVE, DBTYPE_IMPALA):
        raise Hs2ConnectionException("Unsupported db_type: %s" % db_type)

    logger.debug("Identified db type: %s" % db_type)
    return db_type


def hs2_cursor_user(options=None):
    """Return the username we can pass into Impyla.cursor(user=) parameter.
    Uses options when set, otherwise gets config from environment.
    """
    opts = options or hs2_env_options()
    hive_user = os.environ.get("HIVE_SERVER_USER")
    return hive_user if hs2_connect_using_options(opts, unsecured=True) else None

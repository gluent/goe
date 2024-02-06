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

""" OffloadTransportLivyRequests: Helper class to handle requests calls for Livy messages
"""

import requests

try:
    from requests_kerberos import HTTPKerberosAuth, REQUIRED
except ModuleNotFoundError as e:
    if "requests_kerberos" in str(e):
        # We expect the hadoop dependencies when using Kerberos
        HTTPKerberosAuth = None
        REQUIRED = None
    else:
        raise
from urllib3.exceptions import InsecureRequestWarning
from urllib3 import disable_warnings

from goe.offload.offload_messages import VVERBOSE


class OffloadTransportLivyRequestsException(Exception):
    pass


###############################################################################
# CONSTANTS
###############################################################################

###########################################################################
# GLOBAL FUNCTIONS
###########################################################################


class OffloadTransportLivyRequests(object):
    """Helper class to handle requests calls for Livy REST API calls"""

    def __init__(self, offload_options, messages):
        self._messages = messages
        self._api_user = offload_options.offload_transport_user
        self._api_verify_ssl = offload_options.offload_transport_livy_api_verify_ssl
        if self._api_verify_ssl is None:
            messages.log("Livy API has SSL disabled", detail=VVERBOSE)
        else:
            messages.log(
                "Livy API using SSL, SSL verification: %s" % self._api_verify_ssl,
                detail=VVERBOSE,
            )
        # including "X-Requested-By" header because of exception: Missing Required Header for CSRF protection
        self._api_headers = {
            "Content-Type": "application/json",
            "X-Requested-By": self._api_user,
        }
        messages.log("Livy transport headers: %s" % self._api_headers, detail=VVERBOSE)
        self._api_auth = (
            HTTPKerberosAuth(mutual_authentication=REQUIRED)
            if offload_options.kerberos_service
            else None
        )
        if offload_options.kerberos_service:
            messages.log("Livy transport is kerberized", detail=VVERBOSE)
        disable_warnings(InsecureRequestWarning)

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def get(self, url):
        """Issue requests.get() with Livy config"""
        return requests.get(
            url,
            headers=self._api_headers,
            auth=self._api_auth,
            verify=self._api_verify_ssl,
        )

    def post(self, url, data):
        """Issue requests.post() with Livy config"""
        return requests.post(
            url,
            data=data,
            headers=self._api_headers,
            auth=self._api_auth,
            verify=self._api_verify_ssl,
        )

    def delete(self, url):
        """Issue requests.delete() with Livy config"""
        return requests.delete(
            url,
            headers=self._api_headers,
            auth=self._api_auth,
            verify=self._api_verify_ssl,
        )

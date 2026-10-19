# Copyright 2026 The GOE Authors. All rights reserved.
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

"""Normalisation of custom Google API endpoints, e.g. Private Service Connect endpoints.

This is deliberately a leaf module with no dependencies on goe.config (or any module
that imports goe.config) because it is used by both goe.config and goe.util.goe_log_fh.
"""

import logging
from typing import Optional, Tuple
from urllib.parse import urlsplit

logger = logging.getLogger(__name__)

SCHEME_HTTP = "http"
SCHEME_HTTPS = "https"


def _split_endpoint(endpoint: str) -> Tuple[str, str, Optional[int]]:
    """Parse and validate an endpoint string.

    Returns a tuple of (scheme, hostname, port). scheme is an empty string if no scheme was supplied.
    Raises ValueError if the endpoint is malformed.
    """
    # Without a scheme urlsplit() does not recognise the host/port, prefixing "//" fixes that.
    has_scheme = "://" in endpoint
    parsed = urlsplit(endpoint if has_scheme else "//" + endpoint)

    if has_scheme and not parsed.scheme:
        raise ValueError(f"Missing URL scheme: {endpoint}")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError(f"User information is not supported: {endpoint}")
    if parsed.path not in ("", "/"):
        raise ValueError(f"A path is not supported: {endpoint}")
    if parsed.query or parsed.fragment or "?" in endpoint or "#" in endpoint:
        raise ValueError(f"A query string or fragment is not supported: {endpoint}")
    if not parsed.hostname:
        raise ValueError(f"Missing host name: {endpoint}")
    # Accessing .port raises ValueError for non-numeric or out of range values.
    port = parsed.port
    if ":" in parsed.netloc.rsplit("]", 1)[-1] and port is None:
        # A trailing colon with no port, e.g. "host:"
        raise ValueError(f"Missing port number: {endpoint}")
    return parsed.scheme.lower(), parsed.hostname, port


def _host_port(hostname: str, port: Optional[int]) -> str:
    if ":" in hostname:
        # IPv6 literal, urlsplit() strips the brackets.
        hostname = f"[{hostname}]"
    return f"{hostname}:{port}" if port is not None else hostname


def normalise_rest_api_endpoint(endpoint: Optional[str]) -> Optional[str]:
    """Normalise an HTTP/REST endpoint to scheme://host[:port].

    Returns None for None/empty/whitespace input, meaning use the client library default.
    Defaults the scheme to https and logs a warning for http.
    Raises ValueError for malformed input or unsupported schemes.
    """
    if endpoint is None or not endpoint.strip():
        return None
    endpoint = endpoint.strip()
    scheme, hostname, port = _split_endpoint(endpoint)
    if not scheme:
        scheme = SCHEME_HTTPS
    elif scheme == SCHEME_HTTP:
        logger.warning(
            "Insecure http:// Google API endpoint configured, credentials will be sent in cleartext: %s",
            endpoint,
        )
    elif scheme != SCHEME_HTTPS:
        raise ValueError(f"Unsupported URL scheme, expected https or http: {endpoint}")
    return f"{scheme}://{_host_port(hostname, port)}"


def normalise_grpc_api_endpoint(endpoint: Optional[str]) -> Optional[str]:
    """Normalise a gRPC endpoint to host[:port].

    Returns None for None/empty/whitespace input, meaning use the client library default.
    Strips an https:// scheme, gRPC clients always use a secure channel.
    Raises ValueError for http://, other schemes or malformed input.
    """
    if endpoint is None or not endpoint.strip():
        return None
    endpoint = endpoint.strip()
    scheme, hostname, port = _split_endpoint(endpoint)
    if scheme == SCHEME_HTTP:
        raise ValueError(
            f"Insecure http:// is not supported for gRPC endpoints, use https:// or host[:port]: {endpoint}"
        )
    elif scheme and scheme != SCHEME_HTTPS:
        raise ValueError(f"Unsupported URL scheme, expected https or none: {endpoint}")
    return _host_port(hostname, port)

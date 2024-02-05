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

# Standard Library
import gzip
import io
from typing import NoReturn

# Third Party Libraries
from brotli import MODE_FONT, MODE_GENERIC, MODE_TEXT, Compressor
from starlette.datastructures import Headers, MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send


class BrotliMode:
    """Brotli available modes."""

    generic = MODE_GENERIC
    text = MODE_TEXT
    font = MODE_FONT


class CompressionMiddleware:  # noqa: WPS230
    """Compression middleware public interface.

    Returns Brotli compressed responses or GZIP compressed as a fallback

    Based on brotli-asgi and Starlette GZip middleware
    """

    def __init__(
        self,
        app: ASGIApp,
        quality: int = 4,
        mode: str = "text",
        lgwin: int = 22,
        lgblock: int = 0,
        minimum_size: int = 400,
        gzip_fallback: bool = True,
    ) -> None:
        """
        Arguments.
        mode: The compression mode can be:
            generic, text (*default*. Used for UTF-8 format text input)
            or font (for WOFF 2.0).
        quality: Controls the compression-speed vs compression-
            density tradeoff. The higher the quality, the slower the compression.
            Range is 0 to 11.
        lgwin: Base 2 logarithm of the sliding window size. Range
            is 10 to 24.
        lgblock: Base 2 logarithm of the maximum input block size.
            Range is 16 to 24. If set to 0, the value will be set based on the
            quality.
        minimum_size: Only compress responses that are bigger than this value in bytes.
        gzip_fallback: If True, uses gzip encoding if br is not in the Accept-Encoding header.
        """
        self.app = app
        self.quality = quality
        self.mode = getattr(BrotliMode, mode)
        self.minimum_size = minimum_size
        self.lgwin = lgwin
        self.lgblock = lgblock
        self.gzip_fallback = gzip_fallback

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            headers = Headers(scope=scope)
            if "br" in headers.get("Accept-Encoding", ""):
                responder = BrotliResponder(
                    self.app,
                    self.quality,
                    self.mode,
                    self.lgwin,
                    self.lgblock,
                    self.minimum_size,
                )
                await responder(scope, receive, send)
                return
            if self.gzip_fallback and "gzip" in headers.get("Accept-Encoding", ""):
                responder = GZipResponder(self.app, self.minimum_size)
                await responder(scope, receive, send)
                return
        await self.app(scope, receive, send)


class GZipResponder:  # noqa: WPS230
    def __init__(self, app: ASGIApp, minimum_size: int, compresslevel: int = 9) -> None:
        self.app = app
        self.minimum_size = minimum_size
        self.send: Send = unattached_send
        self.initial_message: Message = {}
        self.started = False
        self.gzip_buffer = io.BytesIO()
        self.gzip_file = gzip.GzipFile(
            mode="wb", fileobj=self.gzip_buffer, compresslevel=compresslevel
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        self.send = send
        await self.app(scope, receive, self.send_with_gzip)

    async def send_with_gzip(self, message: Message) -> None:
        message_type = message["type"]
        if message_type == "http.response.start":
            # Don't send the initial message until we've determined how to
            # modify the outgoing headers correctly.
            self.initial_message = message
        elif message_type == "http.response.body" and not self.started:
            self.started = True
            body = message.get("body", b"")
            more_body = message.get("more_body", False)
            if len(body) < self.minimum_size and not more_body:
                # Don't apply GZip to small outgoing responses.
                await self.send(self.initial_message)
                await self.send(message)
            elif not more_body:  # noqa: WPS504
                # Standard GZip response.
                body = self._set_response_body(body, more_body)

                headers = MutableHeaders(raw=self.initial_message["headers"])
                headers["Content-Encoding"] = "gzip"
                headers["Content-Length"] = str(len(body))
                headers.add_vary_header("Accept-Encoding")
                message["body"] = body

                await self.send(self.initial_message)
                await self.send(message)
            else:
                # Initial body in streaming GZip response.
                headers = MutableHeaders(raw=self.initial_message["headers"])
                headers["Content-Encoding"] = "gzip"
                headers.add_vary_header("Accept-Encoding")
                del headers["Content-Length"]  # noqa: WPS420

                message["body"] = self._set_response_body(body, more_body)
                await self.send(self.initial_message)
                await self.send(message)

        elif message_type == "http.response.body":
            # Remaining body in streaming GZip response.
            body = message.get("body", b"")
            more_body = message.get("more_body", False)

            message["body"] = self._set_response_body(body, more_body)

            await self.send(message)

    def _set_response_body(self, body, more_body: bool = True) -> bytes:
        """Null byte fix

        If the response body is null and we try to get the buffervalue, a null byte is produced.
        This causes a content length mismatch.

        This issues exists in the upstream Gzip middleware in starlette.
        - https://github.com/tiangolo/fastapi/issues/4050
        - https://github.com/tiangolo/fastapi/issues/2818

        """
        if body and body not in {b"", b"null"}:
            self.gzip_file.write(body)
        if not more_body:
            self.gzip_file.close()
        value = self.gzip_buffer.getvalue()
        self.gzip_buffer.seek(0)
        self.gzip_buffer.truncate()
        return value


class BrotliResponder:  # noqa: WPS230
    """Brotli Interface."""

    def __init__(
        self,
        app: ASGIApp,
        quality: int,
        mode: BrotliMode,
        lgwin: int,
        lgblock: int,
        minimum_size: int,
    ) -> None:
        self.app = app
        self.quality = quality
        self.mode = mode
        self.lgwin = lgwin
        self.lgblock = lgblock
        self.minimum_size = minimum_size
        self.send = unattached_send
        self.initial_message = {}
        self.started = False
        self.br_file = Compressor(
            quality=self.quality, mode=self.mode, lgwin=self.lgwin, lgblock=self.lgblock
        )
        self.br_buffer = io.BytesIO()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        self.send = send
        await self.app(scope, receive, self.send_with_brotli)

    async def send_with_brotli(self, message: Message) -> None:
        """Apply compression using brotli."""
        message_type = message["type"]
        if message_type == "http.response.start":
            # Don't send the initial message until we've determined how to
            # modify the outgoing headers correctly.
            self.initial_message = message
        elif message_type == "http.response.body" and not self.started:
            self.started = True
            body = message.get("body", b"")
            more_body = message.get("more_body", False)
            if len(body) < self.minimum_size and not more_body:
                # Don't apply Brotli to small outgoing responses.
                await self.send(self.initial_message)
                await self.send(message)

            elif not more_body:  # noqa: WPS504
                # Standard Brotli response.
                body = self._process(body) + self.br_file.finish()
                headers = MutableHeaders(raw=self.initial_message["headers"])
                headers["Content-Encoding"] = "br"
                headers["Content-Length"] = str(len(body))
                headers.add_vary_header("Accept-Encoding")
                message["body"] = body

                await self.send(self.initial_message)
                await self.send(message)
            else:
                # Initial body in streaming Brotli response.
                headers = MutableHeaders(raw=self.initial_message["headers"])
                headers["Content-Encoding"] = "br"
                headers.add_vary_header("Accept-Encoding")
                del headers["Content-Length"]  # noqa: WPS420
                self.br_buffer.write(self._process(body) + self.br_file.flush())
                message["body"] = self._set_response_body(body, more_body)
                self.br_buffer.seek(0)
                self.br_buffer.truncate()
                await self.send(self.initial_message)

                await self.send(message)

        elif message_type == "http.response.body":
            # Remaining body in streaming Brotli response.
            body = message.get("body", b"")
            more_body = message.get("more_body", False)
            self.br_buffer.write(self._process(body) + self.br_file.flush())
            if not more_body:
                self.br_buffer.write(self.br_file.finish())
                message["body"] = self._set_response_body(body, more_body)
                self.br_buffer.close()
                await self.send(message)
                return
            message["body"] = self._set_response_body(body, more_body)
            self.br_buffer.seek(0)
            self.br_buffer.truncate()
            await self.send(message)

    def _set_response_body(self, body, more_body: bool = True) -> bytes:
        """Null byte fix

        If the response body is null and we try to get the buffervalue, a null byte is produced.  This causes a content length mismatch

        This issues exists in the upstream Gzip middleware in starlette.  Maybe this fix should be pushed upstream?
        - https://github.com/tiangolo/fastapi/issues/4050
        """
        # Check # [0x1f, 0x8b, 0x08] is the header for a gzip file.
        if body == b"" and more_body:
            return body
        elif body in (b"null"):
            return body.replace(b"null", b"")
        return self.br_buffer.getvalue()

    def _process(self, body):
        """Workaround to support both brotli and brotlipy
        Before the official Google brotli repository offered a Python version,
        there was a separate package to connect to brotli. These APIs are nearly
        identical except that the official Google API has Compressor.process
        while the brotlipy API has Compress.compress
        """

        if getattr(self.br_file, "process", None):
            return self.br_file.process(body)

        return self.br_file.compress(body)


async def unattached_send(message: Message) -> NoReturn:
    raise RuntimeError("send awaitable not set")  # pragma: no cover

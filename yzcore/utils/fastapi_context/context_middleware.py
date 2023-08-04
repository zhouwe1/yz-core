#!/usr/bin/python3.7+
# -*- coding:utf-8 -*-
"""
@auth: lxm
@date: 2020/12/14
@desc: ...
"""
import re
from _contextvars import Token
from starlette.requests import HTTPConnection
from starlette.responses import Response, PlainTextResponse
from starlette.types import ASGIApp, Receive, Scope, Send
from .fastapi_context import _request_scope_context_storage


class ContextMiddleware:
    def __init__(
            self,
            app: ASGIApp,
            default_error_response: Response = PlainTextResponse(content='缺少site-code请求头', status_code=412)
    ) -> None:
        self.app = app
        self.error_response = default_error_response

    async def __call__(
            self, scope: Scope, receive: Receive, send: Send
    ) -> None:
        if scope["type"] not in ("http",):  # pragma: no cover
            await self.app(scope, receive, send)
            return
        request = HTTPConnection(scope)

        if scope['path'] == '/health':
            if re.match(r'^(127\.0\.0\.1)|(localhost)|(10\.\d{1,3}\.\d{1,3}\.\d{1,3})|(172\.((1[6-9])|(2\d)|(3[01]))\.\d{1,3}\.\d{1,3})|(192\.168\.\d{1,3}\.\d{1,3})$',request.client.host):
                await self.app(scope, receive, send)
                return

        site_code = request.headers.get('site-code', "")
        token: Token = _request_scope_context_storage.set({'site_code': site_code})

        if not site_code:
            message_head = {
                "type": "http.response.start",
                "status": self.error_response.status_code,
                "headers": self.error_response.raw_headers,
            }
            await send(message_head)

            message_body = {
                "type": "http.response.body",
                "body": self.error_response.body,
            }
            await send(message_body)
            return

        try:
            await self.app(scope, receive, send)
        finally:
            _request_scope_context_storage.reset(token)

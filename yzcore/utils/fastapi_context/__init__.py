#!/usr/bin/python3.7+
# -*- coding:utf-8 -*-
"""
@auth: lxm
@date: 2020/12/14
@desc:
fastapi请求上下文功能
参考
https://github.com/tomwojcik/starlette-context
https://docs.python.org/zh-cn/3.7/library/contextvars.html#module-contextvars
"""
from .fastapi_context import context
from .context_middleware import ContextMiddleware

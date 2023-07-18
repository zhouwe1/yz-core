"""
@auth: lxm
@date：2022/04/26
@desc: ...
"""
import logging
from yzcore.utils.fastapi_context.fastapi_context import context


class ContextFilter(logging.Filter):
    """
    This is a filter which injects contextual information into the log.

    Rather than use actual contextual information, we just use random
    data in this demo.
    """

    def filter(self, record):
        record.site_code = context.data.get('site_code', '') if context.exists() else ""
        return True

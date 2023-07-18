"""
@auth: lxm
@date：2022/04/24
@desc: ...
"""
import logging
from copy import copy

from uvicorn.logging import AccessFormatter


class CustomAccessFormatter(AccessFormatter):

    def formatMessage(self, record: logging.LogRecord) -> str:
        recordcopy = copy(record)
        (
            client_addr,
            method,
            full_path,
            http_version,
            status_code,
        ) = recordcopy.args
        recordcopy.__dict__.update(
            {
                "client_addr": client_addr,
                "status_code": status_code,
                "method": method,
                "proxies": full_path,
            }
        )
        return super(AccessFormatter, self).formatMessage(recordcopy)

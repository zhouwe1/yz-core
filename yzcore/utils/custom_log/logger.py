"""
@auth: lxm
@date：2022/04/26
@desc: 自定义核心包的日志格式
"""
import logging
from yzcore.logger import InitLoggerConfig, LOGGING_CONFIG

from yzcore.utils.custom_log.filter import ContextFilter


def get_logger(app_name: str, is_debug=True):
    custom_logging_config = LOGGING_CONFIG
    custom_logging_config['formatters']['custom']['format'] = '{"now": "%(created)f", "level": "%(levelname)s", "PID": "%(process)d", "pathname": "%(pathname)s", "lineno": "%(lineno)d", "message": "%(message)s", "site_code": "%(site_code)s"}'
    InitLoggerConfig(app_name, is_debug=is_debug)
    logger_name = '%s_logger' % app_name
    logger = logging.getLogger(logger_name)
    logger.addFilter(ContextFilter())
    return logger

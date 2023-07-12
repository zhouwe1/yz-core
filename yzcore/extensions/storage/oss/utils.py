import functools
from yzcore.exceptions import NotFoundObject
try:
    from oss2.exceptions import NotFound
except ImportError:
    NotFound = None


def wrap_request_return_bool(func):
    """查询对象是否存在"""
    @functools.wraps(func)
    def wrap_func(*args, **kwargs):
        try:
            func(*args, **kwargs)
            return True
        except NotFound:
            return False

    return wrap_func


def wrap_request_raise_404(func):
    """对象不存在时抛出404"""
    @functools.wraps(func)
    def wrap_func(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except NotFound:
            raise NotFoundObject()
    return wrap_func

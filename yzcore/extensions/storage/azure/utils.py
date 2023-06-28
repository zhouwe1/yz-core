import functools
from yzcore.exceptions import NotFoundObject
try:
    from azure.core.exceptions import ResourceNotFoundError
except ImportError:
    ResourceNotFoundError = None


def wrap_request_raise_404(func):
    """对象不存在时抛出404"""
    @functools.wraps(func)
    def wrap_func(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ResourceNotFoundError:
            raise NotFoundObject()
    return wrap_func

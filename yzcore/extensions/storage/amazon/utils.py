import functools
from yzcore.exceptions import NotFoundObject
try:
    from botocore.exceptions import ClientError
except ImportError:
    ClientError = None


def wrap_request_return_bool(func):
    """查询对象是否存在"""
    @functools.wraps(func)
    def wrap_func(*args, **kwargs):
        try:
            func(*args, **kwargs)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
    return wrap_func


def wrap_request_raise_404(func):
    """对象不存在时抛出404"""
    @functools.wraps(func)
    def wrap_func(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                raise NotFoundObject()
    return wrap_func

import functools
from yzcore.exceptions import NotFoundObject
try:
    from minio.error import S3Error
except ImportError:
    S3Error = None


def wrap_request_return_bool(func):
    """"""
    @functools.wraps(func)
    def wrap_func(*args, **kwargs):
        try:
            func(*args, **kwargs)
            return True
        except S3Error as exc:
            if exc.code != "NoSuchKey":
                raise
        return False
    return wrap_func


def wrap_request_raise_404(func):
    """"""
    @functools.wraps(func)
    def wrap_func(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except S3Error as e:
            if e.code == 'NoSuchKey':
                raise NotFoundObject
            raise
    return wrap_func

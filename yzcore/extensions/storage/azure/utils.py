import functools
from typing import Union, AnyStr, IO
from hashlib import md5
from io import BytesIO
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


def get_content_md5(data: Union[IO, AnyStr]):
    """
    >>> get_content_md5("hello")
    b'^\xb6;\xbb\xe0\x1e\xee\xd0\x93\xcb"\xbb\x8fZ\xcd\xc3'
    """
    if hasattr(data, 'read'):
        content = data.read()
        data.seek(0)
    elif isinstance(data, str):
        content = data.encode()
    else:
        content = data
    return md5(content).digest()

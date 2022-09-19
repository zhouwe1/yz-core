import functools
from io import BytesIO
from yzcore.utils.crypto import get_random_string


def create_temp_file(text_length=16):
    """创建一个包含随机字符串的内存文件"""
    file = BytesIO(get_random_string(text_length).encode())
    file.seek(0)
    return file


def wrap_request_return_bool(func):
    """"""

    @functools.wraps(func)
    def wrap_func(*args, **kwargs):
        try:
            resp = func(*args, **kwargs)
            if resp.status < 300:
                return True
            else:
                return False
        except:
            import traceback
            print(traceback.format_exc())

    return wrap_func

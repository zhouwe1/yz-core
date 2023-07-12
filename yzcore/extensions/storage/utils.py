from io import BytesIO
from typing import AnyStr
from urllib.parse import unquote, urlparse
from pathlib import Path
from yzcore.utils.crypto import get_random_string


def create_temp_file(text_length=16):
    """创建一个包含随机字符串的内存文件"""
    file = BytesIO(get_random_string(text_length).encode())
    return file


def get_url_path(url, urldecode=False):
    """提取URL中的path数据"""
    if not any([url.startswith('//'), url.startswith('http')]):
        url = '//' + url
    if urldecode:
        url = unquote(url)
    return urlparse(url).path


def get_filename(path):
    """从路径中提取文件名"""
    path = unquote(path)
    return Path(path).name


def AnyStr2BytesIO(t: AnyStr):
    """bytes, str to BytesIO"""
    if isinstance(t, bytes):
        obj = BytesIO(t)
    else:
        obj = BytesIO(t.encode())
    return obj

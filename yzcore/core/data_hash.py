#!/usr/bin/python3.6.8+
# -*- coding:utf-8 -*-
"""
@auth: cml
@date: 2020-8-27
@desc: ...
"""
import json
import hashlib


def data_md5(data) -> str:
    """
    取传入数据的MD5摘要
    :param data:
    :return: 'd41d8cd98f00b204e9800998ecf8427e'
    """
    if not isinstance(data, bytes):
        if not isinstance(data, str):
            data = json.dumps(data)
        data = data.encode('utf-8')
    m = hashlib.md5(data)
    return m.hexdigest()

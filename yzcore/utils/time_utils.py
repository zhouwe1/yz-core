#!/usr/bin/python3.6.8+
# -*- coding:utf-8 -*-
"""
@auth: cml
@date: 2020-12-30
@desc: 时间方面的处理函数
"""
import time
import datetime

# 北京时区
Beijing = datetime.timezone(datetime.timedelta(hours=8))


__all__ = [
    "get_zero_time",
    "get_today_date",
    "datetime2timestamp",
    "timestamp2datetime",
    "datetime2str",
    "timestamp2str",
]


def get_zero_time(
        date_time: datetime = None,
        is_timestamp: bool = True
):
    """
    获取0点时间，默认获取今天的0点时间

    :param date_time:
    :param is_timestamp:
    :return:
    """
    if not date_time:
        date_time = datetime.date.today()
        if is_timestamp:
            return int(time.mktime(date_time.timetuple()))
        return date_time
    else:
        time_stamp = int(time.mktime(date_time.timetuple()))
        if is_timestamp:
            return time_stamp
        date_time = datetime.datetime.fromtimestamp(time_stamp)
    return date_time


def datetime2timestamp(date_time: datetime):
    return int(time.mktime(date_time.timetuple()))


def timestamp2datetime(time_stamp: int):
    _s = str(time_stamp)
    if len(_s) > 10 and '.' not in _s:
        _s = '.'.join((_s[:10], _s[10:]))
        time_stamp = float(_s)
    return datetime.datetime.fromtimestamp(time_stamp)


def get_today_date(is_strf: bool = False, format: str = "%Y-%m-%d"):
    """
    获取今天的日期

    :param is_strf:
    :param format:
    :return:
    """
    today = datetime.date.today()
    if is_strf:
        today = today.strftime(format)
    return today


def datetime2str(dt: datetime.datetime, fmt='%Y-%m-%d %H:%M:%S'):
    """
    datetime格式转字符串日期
    :param dt:
    :param fmt
    :return: "2023-04-18 18:54:59"
    """
    if dt.tzinfo:
        dt = dt.astimezone(Beijing)
    return dt.strftime(fmt)


def timestamp2str(timestamp, fmt='%Y-%m-%d %H:%M:%S'):
    """
    时间戳转字符串
    :param timestamp: int|float
    :param fmt:
    :return: "2023-04-18 18:54:59"
    """
    dt = datetime.datetime.fromtimestamp(timestamp, tz=Beijing)
    return dt.strftime(fmt)

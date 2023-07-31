#!/usr/bin/python3.7+
# -*- coding:utf-8 -*-
"""
@auth: lxm
@date: 2021/9/17
@desc: ...
"""
from sqlalchemy import Column, String
from sqlalchemy.ext.declarative import declarative_base

_Base = declarative_base()


class Base(_Base):
    __abstract__ = True

    site_code = Column(String, index=True)

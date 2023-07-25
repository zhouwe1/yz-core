#!/usr/bin/python3.7+
# -*- coding:utf-8 -*-
"""
@auth: lxm
@date: 2021/9/17
@desc: ...
"""
from sqlalchemy import Column, String
from yzcore.db.db_session import Base as _Base


class Base(_Base):
    __abstract__ = True

    site_code = Column(String, index=True)

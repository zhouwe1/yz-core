#!/usr/bin/python3.6+
# -*- coding:utf-8 -*-
"""
@auth: cml
@date: 2020-6-30
@desc: ...
"""
from urllib.parse import urlparse
from typing import Generator
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from yzcore.db.CustomQuery import Query
from yzcore.default_settings import default_setting as settings


def get_db_engine(uri=''):
    if not uri:
        uri = settings.DB_URI
    if uri is None:
        raise EnvironmentError('需要配置"DB_URI"变量！')
    connect_args = {}
    _typ = urlparse(uri).scheme
    if _typ.startswith('sqlite'):
        # 只有SQLite才需要，其他数据库不需要。SQLite 只允许一个线程与其通信
        connect_args['check_same_thread'] = False
    else:
        connect_args['connect_timeout'] = 5
    return create_engine(uri, connect_args=connect_args)


# engine = get_db_engine()
# SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)  # , query_cls=Query 私有化去掉site_code注入

Base = declarative_base()


def get_session(uri=''):
    engine = get_db_engine(uri)
    return sessionmaker(autocommit=False, autoflush=False, bind=engine)  # , query_cls=Query 私有化去掉site_code注入


def get_db() -> Generator:
    try:
        session = get_session()
        db = session()
        yield db
    finally:
        db.close()

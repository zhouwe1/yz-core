#!/usr/bin/python3.7+
# -*- coding:utf-8 -*-
"""
@auth: lxm
@date: 2021/9/17
@desc: ...
"""

from sqlalchemy.orm.query import Query as _Query
from sqlalchemy.sql.selectable import Exists, Alias
from sqlalchemy.orm.mapper import Mapper
from sqlalchemy.sql.elements import Label
from yzcore.utils.fastapi_context import context


class Query(_Query):
    def __init__(self, entities, session=None):
        super().__init__(entities, session=session)
        for entity in entities:
            if type(entity) not in (Exists, Mapper, Label, Alias):
                q = self.filter_by(site_code=context.data['site_code'])
                for k, v in q.__dict__.items():
                    self.__dict__[k] = v
                break

    def join(self, *props, **kwargs):
        return super().join(*props, **kwargs).filter_by(
            site_code=context.data['site_code'])

    def outerjoin(self, *props, **kwargs):
        for prop in props:
            if not isinstance(prop, Alias):
                return super().outerjoin(*props, **kwargs).filter_by(
                    site_code=context.data['site_code'])
            else:
                return super().outerjoin(*props, **kwargs)

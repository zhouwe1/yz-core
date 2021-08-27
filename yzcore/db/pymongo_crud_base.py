#!/usr/bin/python3.6+
# -*- coding:utf-8 -*-
"""
@auth: cml
@date: 2020-6-22
@desc: 非关系数据库的增删改查封装
"""
from typing import Any, Dict, Generic, List, Optional, Type, TypeVar, Union

from pydantic import BaseModel, AnyUrl
from pymongo.client_session import ClientSession

try:
    from pymongo import InsertOne, DeleteOne, ReplaceOne, UpdateMany, UpdateOne
    from pymongo.collection import Collection
    from pymongo import MongoClient
except:
    pass

ModelType = TypeVar("ModelType", bound=str)
CreateSchemaType = TypeVar("CreateSchemaType", bound=BaseModel)
UpdateSchemaType = TypeVar("UpdateSchemaType", bound=BaseModel)
DictorList = TypeVar("DictorList", dict, list)


# DictorList = Union[Dict, List]


class MongoCRUDBase(Generic[CreateSchemaType, UpdateSchemaType]):
    def __init__(
            self,
            collection_name: Type[ModelType],
            db_name: str = "test_db",
            db_url: AnyUrl = "mongodb://localhost:27017/",
            client: ClientSession = None
    ):
        if client:
            self.client = client
        else:
            self.client = MongoClient(db_url)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        # self.coll_name = collection_name

    def count(self, opt: dict = None, session: ClientSession = None):
        """
        统计数目

        :param opt:
        :param session: 事务操作
        :return:
        """
        if opt:
            return self.collection.count_documents(opt, session=session)
        return self.collection.estimated_document_count(session=session)

    def get(self, opt: dict = None, is_logical_del: bool = False,
            session: ClientSession = None):
        """
        查询操作

        :param opt:
        :param is_logical_del:
        :param session: 事务操作
        :return:
        """
        if is_logical_del:
            opt.update({"isDelete": False})
        return self.collection.find_one(opt, session=session)

    def list(self, opt: dict = None, select_col: DictorList = None,
             limit: int = 0, offset: int = 0, sort: List[tuple] = None,
             is_logical_del: bool = False, session: ClientSession = None
             ):
        """
        `projection`（可选）：应在结果集中返回的字段名列表，或指定要包含或排除的字段的dict。
        如果“projection”是一个列表，则始终返回“_id”。使用dict从结果中排除字段
        （例如projection={'_id'：False}）。

        :param opt:
        :param select_col: {'_id': 0, 'author': 1, 'age': 1}
        :param limit: 0
        :param offset: 0
        :param sort: [
                        ('field1', pymongo.ASCENDING),
                        ('field2', pymongo.DESCENDING)
                    ]
        :param is_logical_del:
        :param session: 事务操作
        :return:
        """
        if opt is None:
            opt = dict()
        if is_logical_del:
            opt.update({"isDelete": False})
        data = dict(
            filter=opt,
            projection=select_col,
            skip=offset,
            limit=limit,
            sort=sort
        )
        results = list(self.collection.find(**data, session=session))
        return results

    def create(self, data: DictorList, is_return_obj: bool = False,
               session: ClientSession = None):
        """
        插入操作

        :param data:
        :param is_return_obj:
        :param session: 事务操作
        :return:
        """
        if isinstance(data, dict):
            result = self.collection.insert_one(data, session=session)
            if is_return_obj:
                result = self.collection.find_one({'_id': result.inserted_id},
                                                  session=session)
        elif isinstance(data, list):
            result = self.collection.insert_many(data, session=session)
            if is_return_obj:
                result = self.list({'_id': {'$in': result.inserted_ids}},
                                   session=session)
        else:
            raise Exception('Create failed!')
        return result

    def update(self, opt, data: Dict, is_many: bool = False,
               session: ClientSession = None):
        """
        更新操作

        :param opt: 查询条件
                    opt={'field1': 'xxx'}
                    opt={'field1': 'xxx', 'field2': 123}
                    opt={'field1': {'$gt': 'a'}, 'field2': {'$regex': '^d'}}
        :param data: 需要更新的数据：
                    {'field': 'xxx'}
        :param is_many: 是否批量更新，默认为False
        :param session: 事务操作
        :return:
        """
        update = {"$set": data}
        if not is_many:
            result = self.collection.update_one(opt, update, session=session)
            # result = self.collection.find_one_and_update(opt, update)
        else:
            result = self.collection.update_many(opt, update, session=session)

        if result.acknowledged:
            return result

    def delete(self, opt, is_logical_del: bool = False, is_many: bool = False,
               session: ClientSession = None):
        """
        删除操作: 默认执行逻辑删除，当physical为True时，执行物理删除

        :param opt: 搜索条件
        :param is_logical_del: 是否逻辑删除
        :param is_many: 是否删除多个
        :param session: 事务操作
        :return:
        """
        if is_logical_del:
            update = {"$set": {"isDelete": True}}
            if not is_many:
                result = self.collection.update_one(filter=opt, update=update,
                                                    session=session)
            else:
                result = self.collection.update_many(filter=opt, update=update,
                                                     session=session)
            return result.modified_count
        else:
            if not is_many:
                result = self.collection.delete_one(filter=opt, session=session)
            else:
                result = self.collection.delete_many(filter=opt,
                                                     session=session)
            return result.deleted_count

    def batch_update(self, opt, datas: List, session: ClientSession = None):
        """
        批量更新

        :param opt:
        :param datas:
        :param session: 事务操作
        :return:
        """
        requests = []
        for data in datas:
            # TODO 有误
            requests.append(UpdateOne(opt, data))
        result = self.collection.bulk_write(requests=requests, session=session)
        return result.modified_count


if __name__ == '__main__':
    db = MongoCRUDBase('hello_cml')
    print(db.count())

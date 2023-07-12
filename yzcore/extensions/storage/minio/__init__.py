#!/usr/bin/python3.6+
# -*- coding:utf-8 -*-
"""
@author: zhouwei
@date: 2022/11/09
@desc: minio对象存储封装
"""
import json
import traceback
from datetime import timedelta, datetime
from os import PathLike
from typing import Union, IO, AnyStr

from yzcore.extensions.storage.base import StorageManagerBase, StorageRequestError, logger
from yzcore.extensions.storage.schemas import MinioConfig
from yzcore.extensions.storage.utils import AnyStr2BytesIO
from yzcore.extensions.storage.minio.utils import wrap_request_return_bool, wrap_request_raise_404
from yzcore.utils.time_utils import datetime2str


try:
    from minio import Minio
    from minio.datatypes import PostPolicy
    from minio.commonconfig import CopySource
    from minio.deleteobjects import DeleteObject
    from minio.error import S3Error
except:
    Minio = None


class MinioManager(StorageManagerBase):

    def __init__(self, conf: MinioConfig):
        super(MinioManager, self).__init__(conf)
        self.internal_endpoint = conf.internal_endpoint
        self.disable_internal_endpoint = conf.disable_internal_endpoint
        self.internal_minioClient = None

        self.__init()

    def __init(self, bucket_name=None):
        """初始化对象"""

        if Minio is None:
            raise ImportError("'minio' must be installed to use MinioManager")

        if bucket_name:
            self.bucket_name = bucket_name

        self.minioClient = Minio(
            self.endpoint,
            access_key=self.access_key_id,
            secret_key=self.access_key_secret,
            secure=True if self.scheme == 'https' else False,
        )

        if self.internal_endpoint and not self.disable_internal_endpoint:
            self.internal_minioClient = Minio(
                self.internal_endpoint,
                access_key=self.access_key_id,
                secret_key=self.access_key_secret,
                secure=False,
            )

    def _internal_minio_client_first(self):
        """优先使用内网连接minio服务"""
        if self.internal_minioClient:
            return self.internal_minioClient
        else:
            return self.minioClient

    def get_bucket_cors(self):
        """返回的内容格式和OSS/OBS差异太大"""
        client = self._internal_minio_client_first()
        result = client.get_bucket_policy(self.bucket_name)
        return json.loads(result)

    def set_bucket_cors(self, policy: dict):
        client = self._internal_minio_client_first()
        return client.set_bucket_policy(self.bucket_name, policy)

    def _cors_check(self):
        passed = False
        action_slots = ['s3:AbortMultipartUpload', 's3:DeleteObject', 's3:GetObject',
                        's3:ListMultipartUploadParts', 's3:PutObject']

        cors_dict = self.get_bucket_cors()
        logger.debug(f'_cors_check: {cors_dict}')
        for cors in cors_dict['Statement']:
            effect = cors['Effect']
            resource = cors['Resource'][0]
            actions = cors['Action']
            if effect == 'Allow':
                if resource == f'arn:aws:s3:::{self.bucket_name}/*' or resource == 'arn:aws:s3:::*':
                    if all([False for i in action_slots if i not in actions]):
                        passed = True
        return passed

    def create_bucket(self, bucket_name=None):
        """创建bucket，并且作为当前操作bucket"""
        client = self._internal_minio_client_first()
        client.make_bucket(bucket_name)
        self.bucket_name = bucket_name

    def list_buckets(self):
        client = self._internal_minio_client_first()
        return client.list_buckets()

    def is_exist_bucket(self, bucket_name=None):
        client = self._internal_minio_client_first()
        if bucket_name is None:
            bucket_name = self.bucket_name
        return client.bucket_exists(bucket_name)

    def delete_bucket(self, bucket_name=None):
        client = self._internal_minio_client_first()
        if bucket_name is None:
            bucket_name = self.bucket_name
        return client.remove_bucket(bucket_name)

    def get_sign_url(self, key, expire=0):
        expire_time = timedelta(seconds=expire or self.private_expire_time)
        url = self.minioClient.presigned_get_object(self.bucket_name, key, expires=expire_time)
        return '//' + url.split('//', 1)[-1]

    def post_sign_url(self, key):
        client = self._internal_minio_client_first()
        expire_time = datetime.now() + timedelta(seconds=self.policy_expire_time)
        policy = PostPolicy(bucket_name=self.bucket_name, expiration=expire_time)
        policy.add_starts_with_condition('$key', key)
        # policy.add_content_length_range_condition(1, 1024*1024*1024)  # 限制文件大小
        return client.presigned_post_policy(policy)

    def put_sign_url(self, key):
        return self.minioClient.presigned_put_object(self.bucket_name, key)

    def iter_objects(self, prefix='', **kwargs):
        client = self._internal_minio_client_first()
        objects = client.list_objects(self.bucket_name, prefix=prefix)
        _result = []
        for obj in objects:
            _result.append({
                'key': obj.object_name,
                'url': self.get_file_url(obj.object_name),
                'size': obj.size,
            })
        return _result

    @wrap_request_raise_404
    def get_object_meta(self, key: str):
        """获取文件基本元信息，包括该Object的ETag、Size（文件大小）、LastModified，Content-Type，并不返回其内容"""
        client = self._internal_minio_client_first()
        meta = client.stat_object(self.bucket_name, key)
        return {
            'etag': meta.etag,
            'size': meta.size,
            'last_modified': datetime2str(meta.last_modified),
            'content_type': meta.content_type,
        }

    @wrap_request_raise_404
    def _set_object_headers(self, key: str, headers: dict):
        """更新文件的metadata，主要用于更新Content-Type"""
        client = self._internal_minio_client_first()
        client.copy_object(self.bucket_name, key, CopySource(self.bucket_name, key), metadata=headers, metadata_directive='REPLACE')
        return True

    @wrap_request_return_bool
    def file_exists(self, key):
        client = self._internal_minio_client_first()
        return client.stat_object(self.bucket_name, key)

    @wrap_request_raise_404
    def download_stream(self, key, **kwargs):
        client = self._internal_minio_client_first()
        return client.get_object(self.bucket_name, key)

    @wrap_request_raise_404
    def download_file(self, key, local_name, **kwargs):
        client = self._internal_minio_client_first()
        client.fget_object(self.bucket_name, key, local_name)

    def upload_file(self, filepath: Union[str, PathLike], key: str, **kwargs):
        """上传文件"""
        client = self._internal_minio_client_first()
        try:
            content_type = self.parse_content_type(key)
            client.fput_object(self.bucket_name, key, filepath, content_type=content_type)
            return self.get_file_url(key)
        except Exception:
            logger.error(f'minio upload error: {traceback.format_exc()}')
            raise StorageRequestError('minio upload error')

    def upload_obj(self, file_obj: Union[IO, AnyStr], key: str, **kwargs):
        """上传文件流"""
        client = self._internal_minio_client_first()
        try:
            if isinstance(file_obj, (str, bytes)):
                file_obj = AnyStr2BytesIO(file_obj)
            content_type = self.parse_content_type(key)
            client.put_object(self.bucket_name, key, file_obj, length=-1, content_type=content_type,
                              part_size=1024 * 1024 * 5)
            return self.get_file_url(key)
        except Exception:
            logger.error(f'minio upload error: {traceback.format_exc()}')
            raise StorageRequestError('minio upload error')

    def delete_object(self, key: str):
        """删除文件"""
        client = self._internal_minio_client_first()
        errors = client.remove_objects(self.bucket_name, [DeleteObject(key)])
        for error in errors:
            logger.error(f'minio delete file error: {error}')
            raise StorageRequestError('minio delete file error')
        return True

    def get_policy(
            self,
            filepath: str,
            callback_url: str,
            callback_data: dict,
            **kwargs
    ):
        """
        授权给第三方上传, minio无回调功能，返回callback数据给前端发起回调请求
        :param filepath:
        :param callback_url: 对象存储的回调地址
        :param callback_data: 需要回传的参数
        :return:
        """
        form_data = self.post_sign_url(filepath)
        data = {
            'mode': self.mode,
            'dir': filepath,
            'host': f'{self.scheme}:{self.host}',
            'success_action_status': 200,
            'callback': {'url': callback_url, 'data': callback_data},
            # 'Content-Type': '上传时指定Content-Type',
            **form_data,
        }
        return data

    @property
    def host(self):
        return self._host_minio

    def get_key_from_url(self, url, urldecode=False):
        """从URL中获取对象存储key"""
        return self._get_key_from_url_minio(url, urldecode)

    def get_file_url(self, key, with_scheme=False):
        return self._get_file_url_minio(key, with_scheme)

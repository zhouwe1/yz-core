#!/usr/bin/python3.6+
# -*- coding:utf-8 -*-
"""
@author: zhouwei
@date: 2022/11/09
@desc: minio对象存储封装
"""
import json
import os
from urllib.parse import unquote

from yzcore.extensions.storage.base import StorageManagerBase, StorageRequestError, logger, IMAGE_FORMAT_SET
from yzcore.extensions.storage.schemas import MinioConfig
from yzcore.utils.time_utils import datetime2str
from datetime import timedelta, datetime


try:
    from minio import Minio
    from minio.datatypes import PostPolicy
    from minio.commonconfig import CopySource
    from minio.error import S3Error
except:
    Minio = None


class MinioManager(StorageManagerBase):

    def __init__(self, conf: MinioConfig):
        super(MinioManager, self).__init__(conf)
        self.internal_endpoint = conf.internal_endpoint
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

        if self.internal_endpoint:
            self.internal_minioClient = Minio(
                self.internal_endpoint,
                access_key=self.access_key_id,
                secret_key=self.access_key_secret,
                secure=True if self.scheme == 'https' else False,
            )

        if self.cache_path:
            try:
                os.makedirs(self.cache_path)
            except OSError:
                pass

    def _internal_minio_client_first(self):
        """优先使用内网连接minio服务"""
        if self.internal_endpoint:
            client = self.internal_minioClient
        else:
            client = self.minioClient
        return client

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
        expire_time = datetime.now() + timedelta(seconds=self.policy_expire_time)
        policy = PostPolicy(bucket_name=self.bucket_name, expiration=expire_time)
        policy.add_starts_with_condition('$key', key)
        # policy.add_content_length_range_condition(1, 1024*1024*1024)  # 限制文件大小
        return self.minioClient.presigned_post_policy(policy)

    def put_sign_url(self, key):
        return self.minioClient.presigned_put_object(self.bucket_name, key)

    def iter_objects(self, prefix='', marker=None, delimiter=None, max_keys=100):
        client = self._internal_minio_client_first()
        objects = client.list_objects(self.bucket_name, prefix=prefix, recursive=True)
        _result = []
        for obj in objects:
            _result.append({
                'key': obj.object_name,
                'url': self.get_file_url(obj.object_name),
                'size': obj.size,
            })
        return _result

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

    def update_file_headers(self, key, headers: dict):
        client = self._internal_minio_client_first()
        client.copy_object(self.bucket_name, key, CopySource(self.bucket_name, key), metadata=headers, metadata_directive='REPLACE')
        return True

    def file_exists(self, key):
        client = self._internal_minio_client_first()
        try:
            client.stat_object(self.bucket_name, key)
            return True
        except S3Error as e:
            if e.code == 'NoSuchKey':
                return False
            raise e

    def download_stream(self, key, **kwargs):
        client = self._internal_minio_client_first()
        return client.get_object(self.bucket_name, key)

    def download_file(self, key, local_name, **kwargs):
        client = self._internal_minio_client_first()
        client.fget_object(self.bucket_name, key, local_name)

    def upload(self, filepath, key: str):
        """
        文件上传
        :param filepath:
        :param key:
        """
        client = self._internal_minio_client_first()
        try:
            content_type = self.parse_content_type(key)

            if isinstance(filepath, str):
                client.fput_object(self.bucket_name, key, filepath, content_type=content_type)
            else:
                client.put_object(self.bucket_name, key, filepath, length=-1, content_type=content_type, part_size=1024*1024*5)
            return self.get_file_url(key)
        except Exception as e:
            raise StorageRequestError(f'minio upload error: {e}')

    def get_policy(
            self,
            filepath: str,
            callback_url: str,
            callback_data: dict = None,
            callback_content_type: str = "application/json",
            callback_directly: bool = False,
    ):
        """
        授权给第三方上传
        :param filepath:
        :param callback_url: 对象存储的回调地址
        :param callback_data: 需要回传的参数
        :param callback_content_type: 回调时的Content-Type
               "application/json"
               "application/x-www-form-urlencoded"
        :param callback_directly:  False 需要前端主动发起回调 minio没有回调功能，只能由前端发起
        :return:
        """
        form_data = self.post_sign_url(filepath)
        data = {
            'mode': 'minio',
            'host': f'{self.scheme}:{self.host}',
            'dir': filepath,
            'success_action_status': 200,
            'callback': {'url': callback_url, 'data': callback_data},
            # 'Content-Type': '上传时指定Content-Type',
            **form_data,
        }
        return data

    @property
    def host(self):
        return u'//{}/{}'.format(self.endpoint, self.bucket_name)

    def get_key_from_url(self, url, urldecode=False):
        """从URL中获取对象存储key"""
        path = url.split(self.bucket_name + '/')[-1]
        if urldecode:
            path = unquote(path)
        return path

    def get_file_url(self, key, with_scheme=False):
        if not any((self.image_domain, self.asset_domain)):
            resource_url = u"//{}/{}/{}".format(self.endpoint, self.bucket_name, key)
        elif key.split('.')[-1].lower() in IMAGE_FORMAT_SET:
            resource_url = u"//{domain}/{bucket}/{key}".format(
                domain=self.image_domain, bucket=self.bucket_name, key=key)
        else:
            resource_url = u"//{domain}/{bucket}/{key}".format(
                domain=self.asset_domain, bucket=self.bucket_name, key=key)
        if with_scheme:
            resource_url = self.scheme + ':' + resource_url
        return resource_url

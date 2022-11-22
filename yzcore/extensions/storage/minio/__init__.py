#!/usr/bin/python3.6+
# -*- coding:utf-8 -*-
"""
@author: zhouwei
@date: 2022/11/09
@desc: minio对象存储封装
"""
import os

from yzcore.extensions.storage.base import StorageManagerBase, StorageRequestError
from datetime import timedelta, datetime

try:
    from minio import Minio
    from minio.datatypes import PostPolicy
    from minio.commonconfig import CopySource
except:
    Minio = None


class MinioManager(StorageManagerBase):

    def __init__(self, *args, **kwargs):
        super(MinioManager, self).__init__(*args, **kwargs)
        self.__init()

    def __init(self, bucket_name=None):
        """初始化对象"""

        if Minio is None:
            raise ImportError("'minio' must be installed to use MinioManager")
        if not any((self.endpoint, self.cname)):
            raise AttributeError(
                "One of 'endpoint' and 'cname' must not be None.")

        if bucket_name:
            self.bucket_name = bucket_name

        self.minioClient = Minio(
            self.endpoint,
            access_key=self.access_key_id,
            secret_key=self.access_key_secret,
            secure=True,
        )

        if self.cache_path:
            try:
                os.makedirs(self.cache_path)
            except OSError:
                pass

    def get_bucket_cors(self):
        return self.minioClient.get_bucket_policy(self.bucket_name)

    def set_bucket_cors(self, policy: dict):
        return self.minioClient.set_bucket_policy(self.bucket_name, policy)

    def create_bucket(self, bucket_name=None):
        """创建bucket，并且作为当前操作bucket"""
        self.minioClient.make_bucket(bucket_name)
        self.bucket_name = bucket_name

    def list_buckets(self):
        return self.minioClient.list_buckets()

    def is_exist_bucket(self, bucket_name=None):
        if bucket_name is None:
            bucket_name = self.bucket_name
        return self.minioClient.bucket_exists(bucket_name)

    def delete_bucket(self, bucket_name=None):
        if bucket_name is None:
            bucket_name = self.bucket_name
        return self.minioClient.remove_bucket(bucket_name)

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
        objects = self.minioClient.list_objects(self.bucket_name, prefix=prefix, recursive=True)
        _result = []
        for obj in objects:
            _result.append({
                'key': obj.object_name,
                'url': self.get_file_url(obj.object_name),
                'size': obj.size,
            })
        return _result

    def get_object_meta(self, key: str):
        meta = self.minioClient.stat_object(self.bucket_name, key)
        return {
            'etag': meta.etag,
            'size': meta.size,
            'last_modified': meta.last_modified,
            'content_type': meta.content_type,
        }

    def update_file_headers(self, key, headers: dict):
        self.minioClient.copy_object(self.bucket_name, key, CopySource(self.bucket_name, key), metadata=headers, metadata_directive='REPLACE')

    def download(self, key, local_name=None, is_stream=False, **kwargs):
        if is_stream:
            return self.minioClient.get_object(self.bucket_name, key)
        else:
            # 下载文件到本地
            if not local_name:
                local_name = os.path.abspath(os.path.join(self.cache_path, key))
            self.make_dir(os.path.dirname(local_name))
            self.minioClient.fget_object(self.bucket_name, key, local_name)
            return local_name

    def upload(self, filepath, key=None, **kwargs):
        try:
            if key is None and filepath:
                key = filepath.split('/')[-1]
            if isinstance(filepath, str):
                self.minioClient.fput_object(self.bucket_name, key, filepath, **kwargs)
            else:
                self.minioClient.put_object(self.bucket_name, key, filepath, length=-1)
            return self.get_file_url(key)
        except Exception as e:
            raise StorageRequestError(f'minio upload error: {e}')

    def get_policy(self, filepath: str, callback_url: str = '', callback_data: dict = None,
                   callback_content_type: str = "application/json"):
        form_data = self.post_sign_url(filepath)
        data = {
            'mode': 'minio',
            'host': f"{self.scheme}://{self.endpoint}/{self.bucket_name}",
            'dir': filepath,
            'success_action_status': 200,
            **form_data,
        }
        return data

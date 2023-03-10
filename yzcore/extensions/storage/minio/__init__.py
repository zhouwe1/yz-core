#!/usr/bin/python3.6+
# -*- coding:utf-8 -*-
"""
@author: zhouwei
@date: 2022/11/09
@desc: minio对象存储封装
"""
import json
import os

from yzcore.extensions.storage.base import StorageManagerBase, StorageRequestError, logger, IMAGE_FORMAT_SET
from datetime import timedelta, datetime


try:
    from minio import Minio
    from minio.datatypes import PostPolicy
    from minio.commonconfig import CopySource
    from minio.error import S3Error
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
            secure=True if self.scheme == 'https' else False,
        )

        if self.cache_path:
            try:
                os.makedirs(self.cache_path)
            except OSError:
                pass

    def get_bucket_cors(self):
        """返回的内容格式和OSS/OBS差异太大"""
        result = self.minioClient.get_bucket_policy(self.bucket_name)
        return json.loads(result)

    def set_bucket_cors(self, policy: dict):
        return self.minioClient.set_bucket_policy(self.bucket_name, policy)

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
        """获取文件基本元信息，包括该Object的ETag、Size（文件大小）、LastModified，Content-Type，并不返回其内容"""
        meta = self.minioClient.stat_object(self.bucket_name, key)
        return {
            'etag': meta.etag,
            'size': meta.size,
            'last_modified': meta.last_modified,
            'content_type': meta.content_type,
        }

    def update_file_headers(self, key, headers: dict):
        self.minioClient.copy_object(self.bucket_name, key, CopySource(self.bucket_name, key), metadata=headers, metadata_directive='REPLACE')
        return True

    def file_exists(self, key):
        try:
            self.minioClient.stat_object(self.bucket_name, key)
            return True
        except S3Error as e:
            if e.code == 'NoSuchKey':
                return False
            raise e

    def download_stream(self, key, **kwargs):
        return self.minioClient.get_object(self.bucket_name, key)

    def download_file(self, key, local_name, **kwargs):
        self.minioClient.fget_object(self.bucket_name, key, local_name)

    def upload(self, filepath, key: str):
        """
        文件上传
        :param filepath:
        :param key:
        """
        try:
            content_type = self.parse_content_type(key)

            if isinstance(filepath, str):
                self.minioClient.fput_object(self.bucket_name, key, filepath, content_type=content_type)
            else:
                self.minioClient.put_object(self.bucket_name, key, filepath, length=-1, content_type=content_type, part_size=1024*1024*5)
            return self.get_file_url(key)
        except Exception as e:
            raise StorageRequestError(f'minio upload error: {e}')

    def get_policy(self, filepath: str, callback_url: str, callback_data: dict = None,
                   callback_content_type: str = "application/json"):
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

    def get_key_from_url(self, url):
        """从URL中获取对象存储key"""
        return url.split(self.bucket_name + '/')[-1]

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

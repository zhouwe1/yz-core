#!/usr/bin/python3.6+
# -*- coding:utf-8 -*-
"""
@author: zhouwei
@date: 2023/06/15
@desc: minio对象存储封装
"""
import traceback
from typing import Union, IO, AnyStr
from os import PathLike

from yzcore.extensions.storage.base import StorageManagerBase, StorageRequestError, logger
from yzcore.extensions.storage.schemas import S3Config
from yzcore.extensions.storage.utils import AnyStr2BytesIO
from yzcore.extensions.storage.amazon.utils import wrap_request_return_bool, wrap_request_raise_404
from yzcore.utils import datetime2str

try:
    import boto3
    from boto3.session import Session
    from botocore.exceptions import ClientError
except ImportError:
    boto3 = None


class S3Manager(StorageManagerBase):

    def __init__(self, conf: S3Config):
        super(S3Manager, self).__init__(conf)
        self.endpoint_url = f'{self.scheme}://{self.endpoint}'

        self.__init()

    def __init(self, bucket_name=None):
        """初始化对象"""
        if boto3 is None:
            raise ImportError("'boto3' must be installed to use AmazonS3Manager")

        if bucket_name:
            self.bucket_name = bucket_name

        self.client = boto3.client(
            's3',
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.access_key_secret,
            endpoint_url=self.endpoint_url,
        )

    def create_bucket(self, bucket_name):
        pass

    def get_bucket_cors(self):
        pass

    def list_buckets(self):
        return self.client.list_buckets()['Buckets']

    @wrap_request_return_bool
    def is_exist_bucket(self, bucket_name=None):
        if bucket_name is None:
            bucket_name = self.bucket_name
        return self.client.head_bucket(Bucket=bucket_name)

    def delete_bucket(self, bucket_name=None):
        pass

    def get_sign_url(self, key, expire=0):
        url = self.client.generate_presigned_url(
            ClientMethod='get_object',
            Params={'Bucket': self.bucket_name, 'Key': key},
            ExpiresIn=expire or self.private_expire_time,
            HttpMethod='GET',
        )
        return '//' + url.split('//', 1)[-1]

    def post_sign_url(self, key):
        """"
        获取post上传文件时需要的参数
        :param key: 这里的key作为将来key的前缀
        :return: {'key': key, 'AWSAccessKeyId': '', 'policy': '', 'signature': ''}
        """
        return self.client.generate_presigned_post(
            Bucket=self.bucket_name,
            Key=key+'${filename}',
            ExpiresIn=self.policy_expire_time,
        )

    def put_sign_url(self, key):
        return self.client.generate_presigned_url(
            ClientMethod='put_object',
            Params={'Bucket': self.bucket_name, 'Key': key},
            ExpiresIn=self.policy_expire_time,
            HttpMethod='PUT',
        )

    def iter_objects(self, prefix='', delimiter='', max_keys=100, **kwargs):
        result = []
        for obj in self.client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix, Delimiter=delimiter, MaxKeys=max_keys).get('Contents', []):
            result.append({
                'key': obj.get('Key'),
                'url': self.get_file_url(obj.get('Key')),
                'size': obj.get('Size'),
            })
        return result

    @wrap_request_raise_404
    def get_object_meta(self, key: str):
        response = self.client.head_object(Bucket=self.bucket_name, Key=key)
        return {
            'etag': response['ETag'].strip('"').lower(),
            'size': response['ContentLength'],
            'last_modified': datetime2str(response['LastModified']),
            'content_type': response['ContentType'],
        }

    @wrap_request_raise_404
    def _set_object_headers(self, key: str, headers: dict):
        content_type = headers.pop('Content-Type')
        self.client.copy_object(
            Bucket=self.bucket_name,
            Key=key,
            CopySource={'Bucket': self.bucket_name, 'Key': key},
            ContentType=content_type,
            Metadata=headers,
            MetadataDirective='REPLACE',
        )
        return True

    @wrap_request_return_bool
    def file_exists(self, key):
        return self.client.head_object(Bucket=self.bucket_name, Key=key)

    @wrap_request_raise_404
    def download_stream(self, key, **kwargs):
        return self.client.get_object(Bucket=self.bucket_name, Key=key)['Body']

    @wrap_request_raise_404
    def download_file(self, key, local_name, **kwargs):
        self.client.download_file(Bucket=self.bucket_name, Key=key, Filename=local_name)

    def upload_file(self, filepath: Union[str, PathLike], key: str, **kwargs):
        """上传文件"""
        extra_args = {'ContentType': self.parse_content_type(key)}
        try:
            self.client.upload_file(Bucket=self.bucket_name, Key=key, Filename=filepath, ExtraArgs=extra_args)
            return self.get_file_url(key)
        except Exception:
            logger.error(f's3 upload error: {traceback.format_exc()}')
            raise StorageRequestError(f's3 upload error')

    def upload_obj(self, file_obj: Union[IO, AnyStr], key: str, **kwargs):
        """上传文件流"""
        extra_args = {'ContentType': self.parse_content_type(key)}
        try:
            if isinstance(file_obj, (str, bytes)):
                file_obj = AnyStr2BytesIO(file_obj)
            self.client.upload_fileobj(Bucket=self.bucket_name, Key=key, Fileobj=file_obj, ExtraArgs=extra_args)
            return self.get_file_url(key)
        except Exception:
            logger.error(f's3 upload error: {traceback.format_exc()}')
            raise StorageRequestError(f's3 upload error')

    def delete_object(self, key: str):
        """删除文件"""
        self.client.delete_object(Bucket=self.bucket_name, Key=key)
        return True

    def get_policy(
            self,
            filepath: str,
            callback_url: str,
            callback_data: dict,
            **kwargs
    ):
        form_data = self.post_sign_url(filepath)
        data = {
            'mode': self.mode,
            'host': form_data['url'],
            'dir': filepath,
            'callback': {'url': callback_url, 'data': callback_data},
            **form_data['fields'],
        }
        data.pop('key')
        return data

    @property
    def host(self):
        return self._host_minio

    def get_file_url(self, key, with_scheme=False):
        return self._get_file_url_minio(key, with_scheme)

    def get_key_from_url(self, url, urldecode=False):
        """从URL中获取对象存储key"""
        return self._get_key_from_url_minio(url, urldecode)

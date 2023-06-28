#!/usr/bin/python3.6+
# -*- coding:utf-8 -*-
"""
@author: zhouwei
@date: 2023/04/17
@desc: azure blob对象存储封装
"""
import traceback
from datetime import datetime, timedelta
from io import BytesIO
from typing import Union, IO, AnyStr
from os import PathLike

from yzcore.extensions.storage.base import StorageManagerBase, StorageRequestError, logger
from yzcore.extensions.storage.schemas import AzureConfig
from yzcore.extensions.storage.azure.utils import wrap_request_raise_404, get_content_md5
from yzcore.utils.time_utils import datetime2str


try:
    from azure.storage.blob import BlobServiceClient, ContentSettings, ContainerClient, generate_blob_sas,\
        BlobSasPermissions
    from azure.core.exceptions import ResourceExistsError
except:
    BlobServiceClient = None


class AzureManager(StorageManagerBase):

    def __init__(self, conf: AzureConfig):
        super(AzureManager, self).__init__(conf)
        self.connection_string = conf.connection_string
        self.account_key = conf.account_key
        self.account_name = conf.account_name

        self.__init()

    def __init(self, bucket_name=None):
        """初始化对象"""

        if BlobServiceClient is None:
            raise ImportError("'azure-storage-blob' must be installed to use AzureManager")

        if bucket_name:
            self.bucket_name = bucket_name

        self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        self.container_client = self.blob_service_client.get_container_client(self.bucket_name)

    def create_bucket(self, bucket_name):
        try:
            self.blob_service_client.create_container(bucket_name)
        except ResourceExistsError:
            pass
        self.bucket_name = bucket_name

    def get_bucket_cors(self):
        cors_dict = {
            'allowed_origins': [],
            'allowed_methods': [],
            'allowed_headers': [],
        }
        for cors_rule in self.blob_service_client.get_service_properties()['cors']:
            cors_dict['allowed_origins'] = cors_rule.allowed_origins
            cors_dict['allowed_methods'] = cors_rule.allowed_methods
            cors_dict['allowed_headers'] = cors_rule.allowed_headers
        return cors_dict

    def list_buckets(self):
        return self.blob_service_client.list_containers()

    def is_exist_bucket(self, bucket_name=None):
        if bucket_name is None:
            bucket_name = self.bucket_name
        container_client = self.blob_service_client.get_container_client(bucket_name)
        return container_client.exists()

    def delete_bucket(self, bucket_name=None):
        if bucket_name is None:
            bucket_name = self.bucket_name
        self.blob_service_client.delete_container(bucket_name)

    def get_sign_url(self, key, expire=0):
        expire_time = datetime.utcnow() + timedelta(seconds=expire or self.private_expire_time)
        blob_client = self.container_client.get_blob_client(blob=key)
        sas_sign = generate_blob_sas(
            account_name=self.account_name, container_name=self.bucket_name, blob_name=key, account_key=self.account_key,
            expiry=expire_time, permission=BlobSasPermissions(read=True)
        )
        url = f'{blob_client.url}?{sas_sign}'
        return '//' + url.split('//', 1)[-1]

    def post_sign_url(self, key):
        pass

    def put_sign_url(self, key):
        """
        获取put上传文件的链接
        请求必填的headers: x-ms-blob-type:BlockBlob
        文件直接放入 body：binary中
        """
        expire_time = datetime.utcnow() + timedelta(seconds=self.policy_expire_time)
        blob_client = self.container_client.get_blob_client(blob=key)
        sas_sign = generate_blob_sas(
            account_name=self.account_name, container_name=self.bucket_name, blob_name=key, account_key=self.account_key,
            expiry=expire_time, permission=BlobSasPermissions(write=True)
        )
        return f'{blob_client.url}?{sas_sign}'

    def get_file_url(self, key, with_scheme=False):
        return self._get_file_url_minio(key, with_scheme)

    @property
    def host(self):
        return self._host_minio

    def get_key_from_url(self, url, urldecode=False):
        """从URL中获取对象存储key"""
        return self._get_key_from_url_minio(url, urldecode)

    def iter_objects(self, prefix='', marker=None, delimiter=None, max_keys=100):
        objects = self.container_client.list_blobs(name_starts_with=prefix, results_per_page=max_keys)
        _result = []
        for obj in objects:
            _result.append({
                'key': obj.name,
                'url': self.get_file_url(obj.name),
                'size': obj.size,
            })
        return _result

    @wrap_request_raise_404
    def get_object_meta(self, key: str):
        """azure的etag不像 oss/obs/minio 是文件的md5，而content_md5需要在上传时指定"""
        blob_client = self.container_client.get_blob_client(blob=key)
        metadata = blob_client.get_blob_properties()
        content_md5 = metadata['content_settings']['content_md5'] or ''
        if isinstance(content_md5, bytearray):
            content_md5 = content_md5.hex()
        return {
            'etag': content_md5.lower(),  # metadata['etag'].strip('"').lower()
            'size': metadata['size'],
            'last_modified': datetime2str(metadata['last_modified']),
            'content_type': metadata['content_settings']['content_type']
        }

    @wrap_request_raise_404
    def _set_object_headers(self, key: str, headers: dict):
        blob_client = self.container_client.get_blob_client(blob=key)
        if not any([headers.get('content_md5'), headers.get('Content-MD5')]):
            metadata = blob_client.get_blob_properties()
            headers['content_md5'] = metadata['content_settings']['content_md5']
        blob_client.set_http_headers(ContentSettings(**headers))
        return True

    def file_exists(self, key):
        blob_client = self.container_client.get_blob_client(blob=key)
        return blob_client.exists()

    @wrap_request_raise_404
    def download_stream(self, key, **kwargs):
        blob_client = self.container_client.get_blob_client(blob=key)
        stream = BytesIO()
        blob_client.download_blob().readinto(stream)
        stream.seek(0)
        return stream

    @wrap_request_raise_404
    def download_file(self, key, local_name, **kwargs):
        blob_client = self.container_client.get_blob_client(blob=key)
        with open(local_name, 'wb') as f:
            f.write(blob_client.download_blob().readall())

    def upload_file(self, filepath: Union[str, PathLike], key: str, **kwargs):
        """上传文件流"""
        with open(filepath, 'rb') as f:
            return self.upload_obj(f, key)

    def upload_obj(self, file_obj: Union[IO, AnyStr], key: str, **kwargs):
        """上传文件流"""
        try:
            content_settings = ContentSettings(
                content_type=self.parse_content_type(key),
                content_md5=get_content_md5(file_obj),
            )
            blob_client = self.container_client.get_blob_client(blob=key)
            blob_client.upload_blob(file_obj, overwrite=True, content_settings=content_settings)
            return self.get_file_url(key)
        except Exception:
            logger.error(f'azure blob upload error: {traceback.format_exc()}')
            raise StorageRequestError(f'azure blob upload error')

    def delete_object(self, key: str):
        """删除文件"""
        blob_client = self.container_client.get_blob_client(blob=key)
        blob_client.delete_blob(delete_snapshots='include')
        return True

    def get_policy(
            self,
            filepath: str,
            callback_url: str,
            callback_data: dict = None,
            **kwargs
    ):
        """
        授权给第三方上传
        :param filepath: 需要在前一步拼接好完整的key
        :param callback_url: 对象存储的回调地址
        :param callback_data: 需要回传的参数
        :return:
        """
        return {
            'mode': self.mode,
            'host': self.put_sign_url(filepath),
            'headers': {'x-ms-blob-type': 'BlockBlob'},
            'callback': {'url': callback_url, 'data': callback_data},
        }

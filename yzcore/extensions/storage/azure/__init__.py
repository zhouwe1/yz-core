#!/usr/bin/python3.6+
# -*- coding:utf-8 -*-
"""
@author: zhouwei
@date: 2023/04/17
@desc: azure blob对象存储封装
"""

from datetime import datetime, timedelta
import os
from io import BufferedReader, BytesIO
from typing import Union

from yzcore.extensions.storage.base import StorageManagerBase, StorageRequestError, IMAGE_FORMAT_SET
from yzcore.extensions.storage.schemas import AzureConfig
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
            raise ImportError("'azure-storage-blob' must be installed to use AzureBlobManager")

        if bucket_name:
            self.bucket_name = bucket_name

        self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        self.container_client = self.blob_service_client.get_container_client(self.bucket_name)

        if self.cache_path:
            try:
                os.makedirs(self.cache_path)
            except OSError:
                pass

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
        return f'{blob_client.url}?{sas_sign}'

    def post_sign_url(self, key):
        pass

    def put_sign_url(self, key):
        pass

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

    @property
    def host(self):
        return u'//{}/{}'.format(self.endpoint, self.bucket_name)

    def get_key_from_url(self, url):
        """从URL中获取对象存储key"""
        return url.split(self.bucket_name + '/')[-1]

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

    def get_object_meta(self, key: str):
        blob_client = self.container_client.get_blob_client(blob=key)
        metadata = blob_client.get_blob_properties()
        return {
            'etag': '',  # metadata['etag'].strip('"').lower(), azure的etag计算方式和oss/obs/minio 不一样
            'size': metadata['size'],
            'last_modified': datetime2str(metadata['last_modified']),
            'content_type': metadata['content_settings']['content_type']
        }

    def update_file_headers(self, key, headers: dict):
        blob_client = self.container_client.get_blob_client(blob=key)
        blob_client.set_http_headers(ContentSettings(**headers))
        return True

    def file_exists(self, key):
        blob_client = self.container_client.get_blob_client(blob=key)
        return blob_client.exists()

    def download_stream(self, key, **kwargs):
        blob_client = self.container_client.get_blob_client(blob=key)
        stream = BytesIO()
        blob_client.download_blob().readinto(stream)
        stream.seek(0)
        return stream

    def download_file(self, key, local_name, **kwargs):
        blob_client = self.container_client.get_blob_client(blob=key)
        with open(local_name, 'wb') as f:
            f.write(blob_client.download_blob().readall())

    def upload(self, filepath: Union[str, BufferedReader], key: str):
        try:
            blob_client = self.container_client.get_blob_client(blob=key)
            if isinstance(filepath, str):
                with open(filepath, 'rb') as f:
                    blob_client.upload_blob(f)
            else:
                blob_client.upload_blob(filepath)
            return self.get_file_url(key)
        except Exception as e:
            raise StorageRequestError(f'azure blob upload error: {e}')

    def get_policy(self, filepath: str, callback_url: str, callback_data: dict = None,
                   callback_content_type: str = "application/json"):
        pass

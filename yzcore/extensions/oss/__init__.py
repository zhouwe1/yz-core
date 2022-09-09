#!/usr/bin/python3.7+
# -*- coding:utf-8 -*-
"""
@auth: cml
@date: 2021/3/8
@desc: ...

AccessKeyId         OSSAccessKeyId
file                file
key                 key
policy              policy
signature           Signature
x-obs-acl           x-oss-object-acl
x-obs-grant-read
x-obs-grant-read-acp
x-obs-grant-write-acp
x-obs-grant-full-control
x-obs-storage-class
x-obs-meta-*                                    x-oss-meta-*
x-obs-website-redirect-location
x-obs-server-side-encryption                    x-oss-server-side-encryption
x-obs-server-side-encryption-kms-key-id         x-oss-server-side-encryption-key-id
x-obs-server-side-encryption-customer-algorithm
x-obs-server-side-encryption-customer-key
x-obs-server-side-encryption-customer-key-MD5
x-obs-expires
success_action_redirect         success_action_redirect
success_action_status           success_action_status
                                x-oss-content-type
token                           x-oss-security-token
"""
import os
import json
import hmac
import base64
import hashlib
import datetime
from importlib import import_module
from abc import ABCMeta, abstractmethod
from yzcore.utils.check_storage import create_temp_file
from yzcore.exceptions import StorageError

IMAGE_FORMAT_SET = [
    'bmp', 'jpg', 'jpeg', 'png', 'tif', 'gif', 'pcx', 'tga',
    'exif', 'fpx', 'svg', 'psd', 'cdr', 'pcd', 'dxf', 'ufo',
    'eps', 'ai', 'raw', 'WMF', 'webp', 'tiff'
]


class OssManagerError(ValueError):
    """"""


class OssRequestError(Exception):
    """"""


class OssManagerBase(metaclass=ABCMeta):
    def __init__(
            self,
            access_key_id,
            access_key_secret,
            bucket_name,
            endpoint=None,
            cname=None,
            cache_path='.',
            expire_time=30,
            mode=None,
            **kwargs
    ):
        self.access_key_id = access_key_id
        self.access_key_secret = access_key_secret
        self.bucket_name = bucket_name
        self.endpoint = endpoint

        self.cache_path = cache_path
        self.scheme = kwargs.get("scheme", "https")
        self.image_domain = kwargs.get("image_domain")
        self.asset_domain = kwargs.get("asset_domain")
        self.policy_expire_time = kwargs.get("policy_expire_time", expire_time)
        self.private_expire_time = kwargs.get("private_expire_time", expire_time)

        self.cname = cname
        self.mode = mode
        self.bucket = None

    @abstractmethod
    def create_bucket(self):
        """创建bucket"""

    @abstractmethod
    def list_buckets(self):
        """查询bucket列表"""

    @abstractmethod
    def is_exist_bucket(self, bucket_name=None):
        """判断bucket是否存在"""

    @abstractmethod
    def delete_bucket(self, bucket_name=None):
        """删除bucket"""

    @abstractmethod
    def get_sign_url(self, key, expire=10):
        """生成下载对象的带授权信息的URL"""

    @abstractmethod
    def post_sign_url(self, key, expire=10):
        """生成上传对象的带授权信息的URL"""

    @abstractmethod
    def iter_objects(self, prefix='', marker=None, delimiter=None, max_keys=100):
        """遍历存储桶内的文件"""

    @abstractmethod
    def download(self, *args, **kwargs):
        """下载文件"""

    @abstractmethod
    def upload(self, *args, **kwargs):
        """"""

    def _get_policy_encode(self, key, redirect_url):
        expire_time = datetime.datetime.now() + datetime.timedelta(
            seconds=self.policy_expire_time
        )
        policy_dict = dict(
            expiration=expire_time.isoformat() + "Z",
            conditions=[
                # {"acl": "public-read"},
                # {"x-obs-acl": "public-read"},
                # {"x-obs-security-token": "YwkaRTbdY8g7q...."},
                {"bucket": "yzcore"},
                {"success_action_redirect": redirect_url},
                ["starts-with", "$key", key],                         # 指定值开始
                # ["eq", "$success_action_redirect", "public-read"],  # 精确匹配
                # ["content-length-range", 1, 1024*1024*1024]         # 对象大小限制
            ],
        )
        policy = json.dumps(policy_dict).strip().encode()
        return base64.b64encode(policy)

    @abstractmethod
    def get_policy(
            self,
            filepath: str,
            callback_url: str,
            callback_data: dict = None,
            callback_content_type: str = "application/json"
    ):
        """
        授权给第三方上传
        :param filepath:
        :param callback_url: 对象存储的回调地址
        :param callback_data: 需要回传的参数
        :param callback_content_type: 回调时的Content-Type
               "application/json"
               "application/x-www-form-urlencoded"
        :return:
        """

    def get_file_url(self, filepath=None, key=''):
        if not isinstance(filepath, str):
            filepath = key
        if not any((self.image_domain, self.asset_domain)):
            resource_url = u"//{}.{}/{}".format(self.bucket_name, self.endpoint, key).replace("-internal", "")
        elif filepath.split('.')[-1].lower() in IMAGE_FORMAT_SET:
            resource_url = u"//{domain}/{key}".format(
                domain=self.image_domain, key=key)
        else:
            resource_url = u"//{domain}/{key}".format(
                domain=self.asset_domain, key=key)
        return resource_url

    def delete_cache_file(self, filename):
        """删除文件缓存"""
        filepath = os.path.abspath(os.path.join(self.cache_path, filename))
        assert os.path.isfile(filepath), '非文件或文件不存在'
        os.remove(filepath)

    def search_cache_file(self, filename):
        """文件缓存搜索"""
        # 拼接绝对路径
        filepath = os.path.abspath(os.path.join(self.cache_path, filename))
        if os.path.isfile(filepath):
            return filepath
        else:
            return None

    def make_dir(self, dir_path):
        """新建目录"""
        try:
            os.makedirs(dir_path)
        except OSError:
            pass

    def check(self):
        """通过上传和下载检查对象存储配置是否正确"""
        verify = False
        # 生成一个内存文件
        temp_file = create_temp_file(text_length=32)
        text = temp_file.getvalue().decode()
        # 上传
        self.upload(temp_file, key=f'storage_check_{text}.txt')
        # 下载
        download_file = self.download(key=f'storage_check_{text}.txt')

        with open(download_file, 'rb') as f:
            if text == f.read().decode():
                verify = True
        os.remove(download_file)
        if not verify:
            raise StorageError('对象存储配置校验未通过，请检查配置')
        return True

    @abstractmethod
    def get_object_meta(self, key: str):
        """获取文件基本元信息，包括该Object的ETag、Size（文件大小）、LastModified，并不返回其内容"""


class OssManagerProxy:
    def __init__(self, oss_type, **kwargs):
        # self.oss_type = oss_type
        self.client = self.select_oss(oss_type, **kwargs)

    def select_oss(self, oss_type, **kwargs):
        _module = import_module(f"yzcore.extensions.oss.{oss_type}")
        return _module.OssManager(**kwargs)

    def __getattr__(self, item):
        return getattr(self.client, item)



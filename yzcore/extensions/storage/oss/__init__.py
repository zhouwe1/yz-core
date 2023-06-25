#!/usr/bin/python3.6.8+
# -*- coding:utf-8 -*-
"""
@auth: cml
@date: 2020-10-20
@desc: 对阿里云oss的封装，依赖oss2
"""

import os
import json
import base64
import hmac
import datetime
import hashlib
from urllib import parse
from typing import Union, IO, AnyStr
from os import PathLike
from yzcore.extensions.storage.base import StorageManagerBase, StorageRequestError
from yzcore.extensions.storage.oss.const import *
from yzcore.extensions.storage.oss.utils import wrap_request_return_bool, wrap_request_raise_404
from yzcore.extensions.storage.schemas import OssConfig

try:
    import oss2
    from oss2 import CaseInsensitiveDict
except:
    oss2 = None


class OssManager(StorageManagerBase):

    def __init__(self, conf: OssConfig):
        super(OssManager, self).__init__(conf)
        self.internal_endpoint = conf.internal_endpoint
        self.bucket = None
        self.service = None

        self.__init()

    def __init(self, bucket_name=None):
        """初始化对象"""

        if oss2 is None:
            raise ImportError("'oss2' must be installed to use OssManager")
        if bucket_name:
            self.bucket_name = bucket_name

        self.auth = oss2.Auth(self.access_key_id, self.access_key_secret)

        # 优先内网endpoint
        if self.internal_endpoint:
            self.bucket = oss2.Bucket(self.auth, self.internal_endpoint, self.bucket_name)
        else:
            self.bucket = oss2.Bucket(self.auth, self.endpoint, self.bucket_name)

    def reload_oss(self, **kwargs):
        """重新加载oss配置"""
        self.access_key_id = kwargs.get("access_key_id")
        self.access_key_secret = kwargs.get("access_key_secret")
        self.bucket_name = kwargs.get("bucket_name")
        self.endpoint = kwargs.get("endpoint")
        self.__init()

    def create_bucket(self, bucket_name=None,
                      acl_type='private',
                      storage_type='standard',
                      redundancy_type='zrs'):
        """创建bucket，并且作为当前操作bucket"""
        permission = ACL_TYPE.get(acl_type)
        config = oss2.models.BucketCreateConfig(
            storage_class=STORAGE_CLS.get(storage_type),
            data_redundancy_type=REDUNDANCY_TYPE.get(redundancy_type)
        )
        result = self.bucket.create_bucket(permission, input=config)
        self.__init(bucket_name=bucket_name)
        return result

    def get_bucket_cors(self):
        """获取存储桶的CORS配置"""
        cors_dict = {
            'allowed_origins': [],
            'allowed_methods': [],
            'allowed_headers': [],
        }
        try:
            cors = self.bucket.get_bucket_cors()
            for rule in cors.rules:
                cors_dict['allowed_origins'] = rule.allowed_origins
                cors_dict['allowed_headers'] = rule.allowed_headers
                cors_dict['allowed_methods'] = rule.allowed_methods
        except oss2.exceptions.NoSuchCors:
            pass
        return cors_dict

    def list_buckets(self, prefix='', marker='', max_keys=100, params=None):
        """根据前缀罗列用户的Bucket。

        :param str prefix: 只罗列Bucket名为该前缀的Bucket，空串表示罗列所有的Bucket
        :param str marker: 分页标志。首次调用传空串，后续使用返回值中的next_marker
        :param int max_keys: 每次调用最多返回的Bucket数目
        :param dict params: list操作参数，传入'tag-key','tag-value'对结果进行过滤

        :return: 罗列的结果
        :rtype: oss2.models.ListBucketsResult
        """
        if not hasattr(self, 'service'):
            self.service = oss2.Service(self.auth, self.endpoint)
        return self.service.list_buckets(
            prefix=prefix, marker=marker, max_keys=max_keys, params=params)

    @wrap_request_return_bool
    def is_exist_bucket(self, **kwargs):
        """判断存储空间是否存在"""
        return self.bucket.get_bucket_info()

    def delete_bucket(self, **kwargs):
        """删除bucket"""
        try:
            resp = self.bucket.delete_bucket()
            if resp.status < 300:
                return True
            elif resp.status == 404:
                return False
        except:
            import traceback
            print(traceback.format_exc())

    def encrypt_bucket(self):
        """加密bucket"""
        # 创建Bucket加密配置，以AES256加密为例。
        rule = oss2.models.ServerSideEncryptionRule()
        rule.sse_algorithm = oss2.SERVER_SIDE_ENCRYPTION_AES256
        # 设置KMS密钥ID，加密方式为KMS可设置此项。
        # 如需使用指定的密钥加密，需输入指定的CMK ID；
        # 若使用OSS托管的CMK进行加密，此项为空。使用AES256进行加密时，此项必须为空。
        rule.kms_master_keyid = ""

        # 设置Bucket加密。
        result = self.bucket.put_bucket_encryption(rule)
        # 查看HTTP返回码。
        print('http response code:', result.status)
        return result

    def delete_encrypt_bucket(self):
        # 删除Bucket加密配置。
        result = self.bucket.delete_bucket_encryption()
        # 查看HTTP返回码。
        print('http status:', result.status)
        return result

    def get_sign_url(self, key, expire=0):
        url = self.bucket.sign_url("GET", key, expire or self.private_expire_time)
        return '//' + url.split('//', 1)[-1]

    def post_sign_url(self, key):
        pass

    def put_sign_url(self, key):
        return self.bucket.sign_url("PUT", key, self.policy_expire_time)

    def iter_objects(self, prefix='', marker='', delimiter='', max_keys=100):
        """
        遍历bucket下的文件
        :param prefix: key前缀
        :param marker:
        :param delimiter:
        :param max_keys:
        :return: dict
        """
        _result = []
        for obj in oss2.ObjectIterator(self.bucket, prefix=prefix, marker=marker, delimiter=delimiter, max_keys=max_keys):
            _result.append({
                'key': obj.key,
                'url': self.get_file_url(key=obj.key),
                'size': obj.size,
            })
        return _result

    @wrap_request_raise_404
    def download_stream(self, key, process=None):
        return self.bucket.get_object(key, process=process)

    @wrap_request_raise_404
    def download_file(self, key, local_name, process=None):
        self.bucket.get_object_to_file(key, local_name, process=process)

    def upload_file(self, filepath: Union[str, PathLike], key: str, *, num_threads=2, multipart_threshold=None):
        """
        上传文件流
        :param filepath: 文件路径
        :param key:
        :param num_threads:
        :param multipart_threshold:
        """
        headers = CaseInsensitiveDict({'Content-Type': self.parse_content_type(key)})
        result = oss2.resumable_upload(
            self.bucket, key, filepath,
            headers=headers,
            num_threads=num_threads,
            multipart_threshold=multipart_threshold,
        )
        if result.status // 100 != 2:
            raise StorageRequestError(f'oss upload error: {result.resp}')
        # 返回下载链接
        return self.get_file_url(key)

    def upload_obj(self, file_obj: Union[IO, AnyStr], key: str, **kwargs):
        """上传文件流"""
        headers = CaseInsensitiveDict({'Content-Type': self.parse_content_type(key)})
        result = self.bucket.put_object(key, file_obj, headers=headers)
        if result.status // 100 != 2:
            raise StorageRequestError(f'oss upload error: {result.resp}')
        # 返回下载链接
        return self.get_file_url(key)

    def delete_object(self, key: str):
        """删除文件"""
        self.bucket.delete_object(key)
        return True

    def get_policy(
            self,
            filepath: str,
            callback_url: str,
            callback_data: dict,
            callback_content_type: str = "application/x-www-form-urlencoded",
            **kwargs
    ):
        """
        授权给第三方上传

        :param filepath:
        :param callback_url:
        :param callback_data: 需要回传的参数
        :param callback_content_type: 回调时的Content-Type
               "application/json"
               "application/x-www-form-urlencoded"
        :return:
        """
        params = parse.urlencode(
            dict(data=json.dumps(callback_data)))
        policy_encode = self._get_policy_encode(filepath)
        sign = self.get_signature(policy_encode)

        callback_dict = dict()
        callback_dict["callbackUrl"] = callback_url
        callback_dict["callbackBody"] = (
            "filepath=${object}&size=${size}&mime_type=${mimeType}"
            "&etag=${etag}"
            "&img_height=${imageInfo.height}&img_width=${imageInfo.width}"
            "&img_format=${imageInfo.format}&" + params
        )
        callback_dict["callbackBodyType"] = callback_content_type

        callback_param = json.dumps(callback_dict).strip().encode()
        base64_callback_body = base64.b64encode(callback_param)

        return dict(
            mode=self.mode,
            dir=filepath,
            OSSAccessKeyId=self.access_key_id,
            host=f'{self.scheme}:{self.host}',
            policy=policy_encode.decode(),
            signature=sign,
            callback=base64_callback_body.decode(),
        )

    def _get_policy_encode(self, filepath):
        expire_time = datetime.datetime.now() + datetime.timedelta(
            seconds=self.policy_expire_time
        )
        policy_dict = dict(
            expiration=expire_time.isoformat() + "Z",
            conditions=[
                ["starts-with", "$key", filepath],                    # 指定值开始
                # ["eq", "$success_action_redirect", "public-read"],  # 精确匹配
                # ["content-length-range", 1, 1024*1024*1024]         # 对象大小限制
            ],
        )
        policy = json.dumps(policy_dict).strip().encode()
        return base64.b64encode(policy)

    def get_signature(self, policy_encode):
        """
        获取签名
        :param policy_encode:
        :return:
        """
        h = hmac.new(
            self.access_key_secret.encode("utf-8"), policy_encode, hashlib.sha1
        )
        sign_result = base64.encodebytes(h.digest()).strip()
        return sign_result.decode()

    @wrap_request_raise_404
    def _set_object_headers(self, key: str, headers: dict):
        self.bucket.update_object_meta(key, headers)
        return True

    def file_exists(self, key):
        """检查文件是否存在"""
        return self.bucket.object_exists(key)

    @wrap_request_raise_404
    def get_object_meta(self, key: str):
        """获取文件基本元信息，包括该Object的ETag、Size（文件大小）、LastModified，Content-Type，并不返回其内容"""
        # meta = self.bucket.get_object_meta(key)  # get_object_meta获取到的信息有限
        meta = self.bucket.head_object(key)
        return {
            'etag': meta.etag.lower(),
            'size': meta.content_length,
            'last_modified': meta.headers['Last-Modified'],
            'content_type': meta.headers['Content-Type']
        }

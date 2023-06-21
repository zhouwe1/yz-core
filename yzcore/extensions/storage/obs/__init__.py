#!/usr/bin/python3.6+
# -*- coding:utf-8 -*-
"""
@author: zhouwei
@date: 2022/08/17
@desc: 华为云obs封装，依赖obs
"""
import base64
import json
from typing import Union, IO, AnyStr
from os import PathLike

from yzcore.extensions.storage.base import StorageManagerBase, StorageRequestError
from yzcore.extensions.storage.obs.utils import wrap_request_return_bool
from yzcore.extensions.storage.schemas import ObsConfig
from yzcore.exceptions import NotFoundObject

try:
    import obs
    from obs import SetObjectMetadataHeader
    from .obs_inherit import ObsClient
except:
    obs = None


class ObsManager(StorageManagerBase):

    def __init__(self, conf: ObsConfig):
        super(ObsManager, self).__init__(conf)

        self.__init()

    def __init(self, bucket_name=None):
        """初始化对象"""
        if obs is None:
            raise ImportError("'esdk-obs-python' must be installed to use ObsManager")

        if bucket_name:
            self.bucket_name = bucket_name

        # 创建ObsClient实例
        self.obsClient = ObsClient(
            access_key_id=self.access_key_id,
            secret_access_key=self.access_key_secret,
            server=self.endpoint,
        )

    def create_bucket(self, bucket_name=None, location='cn-south-1'):
        """创建bucket，并且作为当前操作bucket"""
        resp = self.obsClient.createBucket(bucket_name, location=location)
        if resp.status < 300:
            self.bucket_name = bucket_name
            return resp
        else:
            raise StorageRequestError(
                f"static_code: {resp.status}, errorCode: {resp.errorCode}. Message: {resp.errorMessage}.")

    def list_buckets(self):
        resp = self.obsClient.listBuckets(isQueryLocation=True)
        if resp.status < 300:
            return resp.body.buckets
        else:
            raise StorageRequestError(
                f"static_code: {resp.status}, errorCode: {resp.errorCode}. Message: {resp.errorMessage}.")

    @wrap_request_return_bool
    def is_exist_bucket(self, bucket_name=None):
        if bucket_name is None:
            bucket_name = self.bucket_name
        return self.obsClient.headBucket(bucket_name)

    def delete_bucket(self, bucket_name=None):
        if bucket_name is None:
            bucket_name = self.bucket_name
        return self.obsClient.deleteBucket(bucket_name)

    def get_sign_url(self, key, expire=0):
        res = self.obsClient.createSignedUrl("GET", self.bucket_name, objectKey=key, expires=expire or self.private_expire_time)
        return '//' + res.signedUrl.split('//', 1)[-1]

    def post_sign_url(self, key, form_param=None):
        return self.obsClient.createPostSignature(
            self.bucket_name, objectKey=key, expires=self.policy_expire_time, formParams=form_param)

    def put_sign_url(self, key):
        res = self.obsClient.createSignedUrl(
            "PUT", self.bucket_name, objectKey=key, expires=self.policy_expire_time)
        return res.signedUrl

    def iter_objects(self, prefix='', marker=None, delimiter=None, max_keys=100):
        """
        遍历bucket下的文件
        :param prefix: key前缀
        :param marker:
        :param delimiter:
        :param max_keys:
        :return: dict
        """
        _result = []
        resp = self.obsClient.listObjects(self.bucket_name, prefix=prefix, marker=marker, delimiter=delimiter,
                                          max_keys=max_keys)
        if resp.status >= 300:
            raise StorageRequestError(
                f"static_code: {resp.status}, errorCode: {resp.errorCode}. Message: {resp.errorMessage}.")
        for obj in resp.body.contents:
            _result.append({
                'key': obj['key'],
                'url': self.get_file_url(key=obj['key']),
                'size': obj['size']
            })
        return _result

    def download_stream(self, key, **kwargs):
        resp = self.obsClient.getObject(self.bucket_name, key, loadStreamInMemory=False)
        if resp.status == 404:
            raise NotFoundObject()
        return resp.body.response

    def download_file(self, key, local_name, progress_callback=None):
        resp = self.obsClient.getObject(
            self.bucket_name, key,
            downloadPath=local_name,
            progressCallback=progress_callback
        )
        if resp.status == 404:
            raise NotFoundObject()

    def upload_file(self, filepath: Union[str, PathLike], key: str, **kwargs):
        """上传文件"""
        headers = obs.PutObjectHeader(contentType=self.parse_content_type(key))
        resp = self.obsClient.putFile(
            self.bucket_name, key, filepath, headers=headers)
        if resp.status >= 300:
            msg = resp.errorMessage
            raise StorageRequestError(f'obs upload error: {msg}')
        return self.get_file_url(key)

    def upload_obj(self, file_obj: Union[IO, AnyStr], key: str, **kwargs):
        """上传文件流"""
        headers = obs.PutObjectHeader(contentType=self.parse_content_type(key))
        resp = self.obsClient.putContent(
            self.bucket_name, key, content=file_obj, headers=headers)
        if resp.status >= 300:
            msg = resp.errorMessage
            raise StorageRequestError(f'obs upload error: {msg}')
        return self.get_file_url(key)

    def delete_object(self, key: str):
        """删除文件"""
        self.obsClient.deleteObject(self.bucket_name, key)
        return True

    def get_policy(
            self,
            filepath: str,
            callback_url: str,
            callback_data: dict,
            callback_content_type: str = "application/json",
            callback_directly: bool = True,
    ):
        """
        授权给第三方上传
        :param filepath:
        :param callback_url:
        :param callback_data: 需要回传的参数
        :param callback_content_type: 回调时的Content-Type
               "application/json"
               "application/x-www-form-urlencoded"
                华为云目前只能用application/json格式，用x-www-form-urlencoded时回调数据会在url中
        :param callback_directly: True OBS直接发起回调 / False 由前端发起回调
                由于华为云只有单az模式支持回调，多az不支持，可以根据不同情况选择obs回调或者前端发起回调
        :return:
        """
        if callback_directly:
            callback_body = '{"filepath":"$(key)","etag":"$(etag)","size":$(fsize),"mime_type":"$(ext)",' \
                            '"data":' \
                            + json.dumps(callback_data) + '}'

            callback_body_plain = json.dumps(callback_body).strip().encode()
            base64_callback_body = base64.b64encode(callback_body_plain)

            form_param = {
                'body': base64_callback_body.decode(),
                'url': callback_url,
                'body-type': callback_content_type,
                # 'success_action_status': '200',
            }
        else:
            form_param = {}

        res = self.post_sign_url(key=filepath, form_param=form_param)

        data = dict(
            mode=self.mode,
            AccessKeyId=self.access_key_id,
            host=f'{self.scheme}:{self.host}',
            policy=res.policy,
            signature=res.signature,
            dir=filepath
        )
        if not callback_directly:
            data['callback'] = {'url': callback_url, 'data': callback_data}
        return data

    def _set_object_headers(self, key: str, headers: dict):
        # 兼容 oss.update_file_headers
        obs_headers = SetObjectMetadataHeader()
        obs_headers.contentType = headers['Content-Type']  # oss 和 obs的参数名称不相同
        resp = self.obsClient.setObjectMetadata(self.bucket_name, key, headers=obs_headers)
        if resp.status == 404:
            raise NotFoundObject()
        return True

    @wrap_request_return_bool
    def file_exists(self, key):
        """检查文件是否存在"""
        return self.obsClient.headObject(self.bucket_name, key)

    def get_object_meta(self, key: str):
        """获取文件基本元信息，包括该Object的ETag、Size（文件大小）、LastModified，Content-Type，并不返回其内容"""
        resp = self.obsClient.getObjectMetadata(self.bucket_name, key)
        if resp.status == 404:
            raise NotFoundObject()
        return {
            'etag': resp.body.etag.strip('"').lower(),
            'size': resp.body.contentLength,
            'last_modified': resp.body.lastModified,
            'content_type': resp.body.contentType,
        }

    def get_bucket_cors(self):
        cors_dict = {
            'allowed_origins': [],
            'allowed_methods': [],
            'allowed_headers': [],
        }
        resp = self.obsClient.getBucketCors(self.bucket_name)
        if resp.status < 300:
            for rule in resp.body:
                cors_dict['allowed_origins'] = rule.allowedOrigin
                cors_dict['allowed_headers'] = rule.allowedHeader
                cors_dict['allowed_methods'] = rule.allowedMethod
            return cors_dict
        else:
            raise StorageRequestError(
                f"static_code: {resp.status}, errorCode: {resp.errorCode}. Message: {resp.errorMessage}.")

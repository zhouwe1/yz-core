#!/usr/bin/python3.6+
# -*- coding:utf-8 -*-
"""
@author: zhouwei
@date: 2022/08/17
@desc: 华为云obs封装，依赖obs
"""
import base64
import json
import os
import functools
from obs import ObsClient as _ObsClient, const, util, client
from yzcore.extensions.oss import OssManagerBase, OssRequestError
from yzcore.exceptions import StorageError

try:
    import obs
except:
    obs = None


def wrap_request_return_bool(func):
    """"""

    @functools.wraps(func)
    def wrap_func(*args, **kwargs):
        try:
            resp = func(*args, **kwargs)
            if resp.status < 300:
                return True
            else:
                return False
        except:
            import traceback
            print(traceback.format_exc())

    return wrap_func


class ObsClient(_ObsClient):
    pass

    def createPostSignature(self, bucketName=None, objectKey=None, expires=300, formParams=None):
        return self._createPostSignature(bucketName, objectKey, expires, formParams, self.signature.lower() == 'v4')

    def _createPostSignature(self, bucketName=None, objectKey=None, expires=300, formParams=None, is_v4=False):
        from datetime import datetime, timedelta

        date = datetime.utcnow()
        shortDate = date.strftime(const.SHORT_DATE_FORMAT)
        longDate = date.strftime(const.LONG_DATE_FORMAT)
        securityProvider = self._get_token()

        expires = 300 if expires is None else util.to_int(expires)
        expires = date + timedelta(seconds=expires)

        expires = expires.strftime(const.EXPIRATION_DATE_FORMAT)

        formParams = self._parse_post_params(formParams, securityProvider, is_v4,
                                             bucketName, objectKey, longDate, shortDate)

        policy = ['{"expiration":"']
        policy.append(expires)
        policy.append('", "callback":[')

        # 添加callback数据
        policy.append('{"url":"' + formParams.get('url', '') + '"},')
        policy.append('{"body":"' + formParams.get('body', '') + '"},')
        policy.append('{"body-type":"' + formParams.get('body-type', '') + '"},')
        policy.append('], "conditions":[')

        matchAnyBucket = True
        matchAnyKey = True

        conditionAllowKeys = ['acl', 'bucket', 'key', 'success_action_redirect', 'redirect', 'success_action_status']

        for key, value in formParams.items():
            if key:
                key = util.to_string(key).lower()

                if key == 'bucket':
                    matchAnyBucket = False
                elif key == 'key':
                    matchAnyKey = False

                if key not in const.ALLOWED_REQUEST_HTTP_HEADER_METADATA_NAMES \
                        and not key.startswith(self.ha._get_header_prefix()) \
                        and not key.startswith(const.OBS_HEADER_PREFIX) and key not in conditionAllowKeys:
                    continue

                policy.append('{"')
                policy.append(key)
                policy.append('":"')
                policy.append(util.to_string(value))
                policy.append('"},')

        if matchAnyBucket:
            policy.append('["starts-with", "$bucket", ""],')

        if matchAnyKey:
            policy.append('["starts-with", "$key", ""],')

        policy.append(']}')

        originPolicy = ''.join(policy)

        policy = util.base64_encode(originPolicy)

        result = self._parse_post_signature_type(is_v4, securityProvider, originPolicy,
                                                 policy, formParams, shortDate, longDate)
        return client._CreatePostSignatureResponse(**result)


class ObsManager(OssManagerBase):
    acl_type = {
        "private": obs.HeadPermission.PRIVATE,
        "onlyread": obs.HeadPermission.PUBLIC_READ,
        "readwrite": obs.HeadPermission.PUBLIC_READ_WRITE,
        "bucket_read": obs.HeadPermission.PUBLIC_READ_DELIVERED,  # 桶公共读，桶内对象公共读。
        "bucket_readwrite": obs.HeadPermission.PUBLIC_READ_WRITE_DELIVERED,  # 桶公共读写，桶内对象公共读写。
        "owner_full_control": obs.HeadPermission.BUCKET_OWNER_FULL_CONTROL,  # 桶或对象所有者拥有完全控制权限。
    }
    # 存储类型
    storage_cls = {
        "standard": obs.StorageClass.STANDARD,  # 标准类型
        "ia": obs.StorageClass.WARM,  # 低频访问类型
        # "archive": oss2.BUCKET_STORAGE_CLASS_ARCHIVE,  # 归档类型
        "cold_archive": obs.StorageClass.COLD,  # 冷归档类型
    }

    # 冗余类型
    # redundancy_type = {
    #     "lrs": oss2.BUCKET_DATA_REDUNDANCY_TYPE_LRS,  # 本地冗余
    #     "zrs": oss2.BUCKET_DATA_REDUNDANCY_TYPE_ZRS,  # 同城冗余（跨机房）
    # }

    def __init__(self, *args, **kwargs):
        super(ObsManager, self).__init__(*args, **kwargs)
        self.__init()

    def __init(self, *args, **kwargs):
        """"""
        if obs is None:
            raise ImportError("'esdk-obs-python' must be installed to use OssManager")
        # 创建ObsClient实例
        self.obsClient = ObsClient(
            access_key_id=self.access_key_id,
            secret_access_key=self.access_key_secret,
            server=self.endpoint
        )
        # self.bucket = self.obsClient.bucketClient(self.bucket_name)

    def create_bucket(
            self, bucket_name=None, location='cn-south-1'
    ):
        """"""
        if bucket_name is None:
            bucket_name = self.bucket_name
        resp = self.obsClient.createBucket(bucket_name, location=location)
        if resp.status < 300:
            return True
        else:
            raise OssRequestError(
                f"errorCode: {resp.errorCode}. Message: {resp.errorMessage}.")

    def list_buckets(self):
        resp = self.obsClient.listBuckets(isQueryLocation=True)
        if resp.status < 300:
            return resp.body.buckets
        else:
            raise OssRequestError(
                f"errorCode: {resp.errorCode}. Message: {resp.errorMessage}.")

    @wrap_request_return_bool
    def is_exist_bucket(self, bucket_name=None):
        if bucket_name is None:
            bucket_name = self.bucket_name
        return self.obsClient.headBucket(bucket_name)

    @wrap_request_return_bool
    def delete_bucket(self, bucket_name=None):
        if bucket_name is None:
            bucket_name = self.bucket_name
        return self.obsClient.deleteBucket(bucket_name)

    def get_sign_url(self, key, expire=10):
        res = self.obsClient.createSignedUrl("GET", self.bucket_name, key, expire)
        return res.signedUrl

    def post_sign_url(self, key, expire=10, form_param=None):
        if form_param:
            return self.obsClient.createPostSignature(
                self.bucket_name, objectKey=key, expires=expire, formParams=form_param)
        else:
            res = self.obsClient.createSignedUrl(
                "PUT", self.bucket_name, objectKey=key, expires=expire)
            return res.signedUrl

    def iter_objects(self, prefix='', marker=None, delimiter=None, max_keys=100):
        """遍历bucket下的文件"""
        resp = self.obsClient.listObjects(self.bucket_name, prefix=prefix, marker=marker,delimiter=delimiter, max_keys=max_keys)
        return resp.body.contents

    def download(self, key, local_name=None, is_stream=False, progress_callback=None):
        if is_stream:
            return self.get_file_stream(key)
        else:
            if not local_name:
                local_name = os.path.abspath(os.path.join(self.cache_path, key))
            self.make_dir(os.path.dirname(local_name))
            self.obsClient.getObject(
                self.bucket_name, key,
                downloadPath=local_name,
                progressCallback=progress_callback
            )
            return local_name

    def get_file_stream(self, key):

        resp = self.obsClient.getObject(
            self.bucket_name, key,
            loadStreamInMemory=True,
        )
        # 获取对象内容
        return resp.body.buffer

    def upload(self, filepath, key=None, **kwargs):
        """上传文件"""
        if key is None and filepath:
            key = filepath.split('/')[-1]

        if isinstance(filepath, str):
            headers = None
            if filepath.endswith(".dds"):
                headers = obs.PutObjectHeader(contentType="application/octet-stream")
            resp = self.obsClient.putFile(
                self.bucket_name, key, filepath, headers=headers)
        else:
            resp = self.obsClient.putContent(
                self.bucket_name, key, content=filepath)

        if resp.status > 200:
            msg = resp.errorMessage
            raise StorageError(f'obs error: {msg}')

        # 返回下载链接
        if not any((self.image_domain, self.asset_domain)):
            return '//{}.{}/{}'.format(self.bucket_name, self.endpoint, key)
        else:
            return self.get_file_url(filepath, key)

    def get_policy(
            self,
            filepath,
            callback_url='http://47.106.69.126:8012/oss_callback?a=1&b=2',
            callback_data=None,
            callback_content_type="application/json"):
        """
        授权给第三方上传

        :param filepath:
        :param callback_url:
        :param callback_data: 需要回传的参数
        :param callback_content_type: 回调时的Content-Type
               "application/json"
               "application/x-www-form-urlencoded"
                华为云目前只能用application/json格式，用x-www-form-urlencoded时回调数据会在url中
        :return:
        """
        # params = parse.urlencode(dict(data=json.dumps(callback_data)))
        # callback_body = 'filepath=$(key)&etag=$(etag)&data=$(etag)&size=$(fsize)&mime_type=$(ext)&' + params
        callback_body = '{"filepath":"$(key)","etag":"$(etag)","size":$(fsize),"mime_type":"$(ext)",' \
                        '"data":' \
                        + json.dumps(callback_data) + '}'

        callback_body_plain = json.dumps(callback_body).strip().encode()
        base64_callback_body = base64.b64encode(callback_body_plain)

        form_param = {
            'body': base64_callback_body.decode(),
            'url': callback_url,
            'body-type': callback_content_type,
            'success_action_status': '200',
        }
        res = self.post_sign_url(key=None, expire=self.policy_expire_time, form_param=form_param)

        data = dict(
            accessid=self.access_key_id,
            host=f"{self.scheme}://{self.bucket_name}.{self.endpoint}",
            policy=res.policy,
            signature=res.signature,
            dir=filepath
        )
        return data

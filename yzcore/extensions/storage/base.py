import os
from abc import ABCMeta, abstractmethod
from yzcore.utils.check_storage import create_temp_file
from yzcore.extensions.storage.const import IMAGE_FORMAT_SET
from yzcore.exceptions import StorageError
from urllib.request import urlopen
from urllib.error import URLError
from ssl import SSLCertVerificationError


class StorageManagerBase(metaclass=ABCMeta):
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
    def get_bucket_cors(self):
        """
        获取CORS配置
        :return: {
            'allowed_origins': [],
            'allowed_methods': [],
            'allowed_headers': [],
        }
        """

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
    def download(self, key, local_name=None, is_stream=False, **kwargs):
        """下载文件"""

    @abstractmethod
    def upload(self, *args, **kwargs):
        """"""

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

    @staticmethod
    def make_dir(dir_path):
        """新建目录"""
        try:
            os.makedirs(dir_path)
        except OSError:
            pass

    def check(self):
        """通过上传和下载检查对象存储配置是否正确"""
        try:
            # 检查bucket是否正确
            assert self.is_exist_bucket(), f'{self.bucket_name}: No Such Bucket'
            # CORS 检查
            assert self._cors_check(), f'{self.bucket_name}: CORS设置错误'

            # 生成一个带有随机字符串的内存文件
            temp_file = create_temp_file(text_length=32)
            text = temp_file.getvalue().decode()

            key = f'storage_check_{text}.txt'
            # 上传
            file_url = self.upload(temp_file, key=key)
            assert file_url, f'{self.bucket_name}: Upload Failed'
            # 加签url
            assert self._check_sign_url(key), f'{self.bucket_name}: Sign Url Error'
            # 下载
            download_file = self.download(key=f'storage_check_{text}.txt', is_stream=True)
            download_text = download_file.read().decode()
            assert download_text == text, f'{self.bucket_name}: DownloadFailed'
            return True
        except AssertionError as e:
            raise StorageError(e)

    @abstractmethod
    def get_object_meta(self, key: str):
        """获取文件基本元信息，包括该Object的ETag、Size（文件大小）、LastModified，并不返回其内容"""

    def _cors_check(self):
        """检查存储桶的CORS配置是否设置正确"""
        allowed_methods = {'GET', 'PUT', 'POST', 'DELETE', 'HEAD'}
        cors_dict = self.get_bucket_cors()
        if set(cors_dict['allowed_methods']) != allowed_methods:
            raise StorageError('CORS设置错误')
        if cors_dict['allowed_headers'] != ['*']:
            raise StorageError('CORS设置错误')
        if cors_dict['allowed_origins'] != ['*']:
            raise StorageError('CORS设置错误')
        return True

    def _check_sign_url(self, key):
        """判断加签url是否可以正常打开，并且配置了https"""
        try:
            sign_url = self.get_sign_url(key=key, expire=600)
            resp = urlopen('https:' + sign_url)
            assert resp.status < 300, f'{self.bucket_name}: Sign Url Error, {sign_url}'
        except URLError as e:
            if isinstance(e.reason, SSLCertVerificationError):
                raise StorageError(f'{self.bucket_name}: 未开启https')
            raise StorageError(f'{self.bucket_name}: Sign Url Error')
        return True


class StorageRequestError(Exception):
    """请求对象存储服务时遇到的异常"""

import os
import shutil
from typing import Union
from io import BufferedReader
from abc import ABCMeta, abstractmethod
from yzcore.extensions.storage.utils import create_temp_file
from yzcore.extensions.storage.const import IMAGE_FORMAT_SET, CONTENT_TYPE
from yzcore.logger import get_logger
from urllib.request import urlopen
from urllib.error import URLError
from urllib.parse import urlparse, unquote
from ssl import SSLCertVerificationError

logger = get_logger(__name__)


class StorageRequestError(Exception):
    """请求外部对象存储服务时遇到的错误或异常"""


class StorageManagerBase(metaclass=ABCMeta):
    def __init__(
            self,
            mode,
            access_key_id,
            access_key_secret,
            bucket_name,
            endpoint=None,
            internal_endpoint=None,
            cname=None,
            cache_path='.',
            expire_time=30,
            scheme='https',
            **kwargs
    ):
        self.mode = mode
        self.access_key_id = access_key_id
        self.access_key_secret = access_key_secret
        self.bucket_name = bucket_name
        self.endpoint = endpoint
        self.internal_endpoint = internal_endpoint

        self.cache_path = cache_path
        self.scheme = scheme
        self.image_domain = kwargs.get("image_domain")
        self.asset_domain = kwargs.get("asset_domain")
        self.policy_expire_time = kwargs.get("policy_expire_time", expire_time)
        self.private_expire_time = kwargs.get("private_expire_time", expire_time)

        self.cname = cname
        self.is_cname = False

    @abstractmethod
    def create_bucket(self, bucket_name):
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
    def get_sign_url(self, key, expire=0):
        """生成下载对象的带授权信息的URL"""

    @abstractmethod
    def post_sign_url(self, key):
        """生成POST上传对象的授权信息"""

    @abstractmethod
    def put_sign_url(self, key):
        """生成PUT上传对象的带授权信息的URL"""

    @abstractmethod
    def iter_objects(self, prefix='', marker=None, delimiter=None, max_keys=100):
        """
        遍历存储桶内的文件
        目前返回的字段：
            [{
                'key': '',
                'url: '',
                'size': '',
            }]
        """

    @abstractmethod
    def get_object_meta(self, key: str):
        """获取文件基本元信息，包括该Object的ETag、Size（文件大小）、LastModified，Content-Type，并不返回其内容"""

    @abstractmethod
    def update_file_headers(self, key, headers: dict):
        """更改Object的元数据信息，包括Content-Type这类标准的HTTP头部"""

    @abstractmethod
    def file_exists(self, key):
        """检查文件是否存在"""

    def download(self, key, local_name=None, path=None, is_stream=False, **kwargs):
        """
        下载文件
        :param key:
        :param local_name: 下载的文件在本地的路径
        :param path: 文件下载路径
        :param is_stream:
            is_stream = True:
                >>> result = self.download('readme.txt', is_stream=True)
                >>> print(result.read())
                'hello world'
            is_stream = False:
                >>> result = self.download('readme.txt', '/tmp/cache/readme.txt')
                >>> print(result)
                '/tmp/cache/readme.txt'
        :return: 文件对象或文件下载后的本地路径
        """
        if is_stream:
            return self.download_stream(key, **kwargs)
        else:
            if not local_name:
                if path:
                    local_name = os.path.abspath(os.path.join(self.cache_path, path, self.get_filename(key)))
                else:
                    local_name = os.path.abspath(os.path.join(self.cache_path, key))
            self.make_dir(os.path.dirname(local_name))
            self.download_file(key, local_name)
            return local_name

    @abstractmethod
    def download_stream(self, key, **kwargs):
        """下载文件流"""
        pass

    @abstractmethod
    def download_file(self, key, local_name, **kwargs):
        """下载文件"""
        pass

    @abstractmethod
    def upload(self, filepath: Union[str, BufferedReader], key: str):
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

    @property
    def host(self):
        if self.cname:
            return u'//{}'.format(self.cname)
        else:
            return u'//{}.{}'.format(self.bucket_name, self.endpoint)

    def get_file_url(self, key, with_scheme=False):
        if not any((self.image_domain, self.asset_domain)):
            if self.is_cname:
                resource_url = u"//{}/{}".format(self.cname, key)
            else:
                resource_url = u"//{}.{}/{}".format(self.bucket_name, self.endpoint, key)
        elif key.split('.')[-1].lower() in IMAGE_FORMAT_SET:
            resource_url = u"//{domain}/{key}".format(domain=self.image_domain, key=key)
        else:
            resource_url = u"//{domain}/{key}".format(domain=self.asset_domain, key=key)
        if with_scheme:
            resource_url = self.scheme + ':' + resource_url
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

    @classmethod
    def make_dir(cls, dir_path):
        """新建目录"""
        try:
            os.makedirs(dir_path)
        except OSError:
            pass

    @classmethod
    def copy_file(cls, src, dst):
        """拷贝文件"""
        dst_dir = os.path.dirname(dst)
        cls.make_dir(dst_dir)
        shutil.copy(src, dst)

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
            logger.debug(f'upload: {file_url}')
            assert file_url, f'{self.bucket_name}: Upload Failed'
            # 加签url
            assert self._check_sign_url(key), f'{self.bucket_name}: Sign Url Error'
            # 下载
            download_file = self.download(key=f'storage_check_{text}.txt', is_stream=True)
            download_text = download_file.read().decode()
            assert download_text == text, f'{self.bucket_name}: DownloadFailed'

            # 获取文件元数据
            metadata = self.get_object_meta(key)
            logger.debug(f'get_object_meta: {metadata}')
            assert metadata, f'{self.bucket_name}: Get object metadata Failed'
            # 修改文件元数据
            assert self.update_file_headers(key, {'Content-Type': 'text/plain'}), f'{self.bucket_name}: Update object metadata Failed'
            logger.debug(f'update_file_headers: {self.get_object_meta(key)}')

            # 遍历文件
            objects = self.iter_objects(key)
            logger.debug(f'iter_objects: {objects}')
            assert objects, f'{self.bucket_name} iter objects Failed'

            return True
        except AssertionError as e:
            raise StorageRequestError(e)

    def _cors_check(self):
        """检查存储桶的CORS配置是否设置正确"""
        allowed_methods = {'GET', 'PUT', 'POST', 'DELETE', 'HEAD'}
        cors_dict = self.get_bucket_cors()
        logger.debug(f'_cors_check: {cors_dict}')
        if set(cors_dict['allowed_methods']) != allowed_methods:
            raise StorageRequestError(f'{self.bucket_name}: CORS设置错误')
        if cors_dict['allowed_headers'] != ['*']:
            raise StorageRequestError(f'{self.bucket_name}: CORS设置错误')
        if cors_dict['allowed_origins'] != ['*']:
            raise StorageRequestError(f'{self.bucket_name}: CORS设置错误')
        return True

    def _check_sign_url(self, key):
        """判断加签url是否可以正常打开，并且配置了https"""
        try:
            sign_url = self.get_sign_url(key=key, expire=600)
            resp = urlopen(self.scheme + ':' + sign_url)
            assert resp.status < 300, f'{self.bucket_name}: Sign Url Error, {sign_url}'
        except URLError as e:
            if isinstance(e.reason, SSLCertVerificationError):
                raise StorageRequestError(f'{self.bucket_name}: 未开启https')
            raise StorageRequestError(f'{self.bucket_name}: Sign Url Error')
        return True

    @staticmethod
    def parse_content_type(filename):
        ext = filename.split('.')[-1].lower()
        return CONTENT_TYPE.get(ext, 'application/octet-stream')

    def get_key_from_url(self, url):
        """从URL中获取对象存储key"""
        if url.startswith('//'):
            url = 'https:' + url
        elif not url.startswith('http'):
            url = 'https://' + url
        url_parse = urlparse(url)
        return url_parse.path[1:]  # 去掉最前面的 /

    @staticmethod
    def get_filename(key):
        """从key中提取文件名，不包含路径"""
        key = unquote(key)
        return key.split('/')[-1]

from yzcore.extensions.storage.oss import OssManager
from yzcore.extensions.storage.obs import ObsManager
from yzcore.extensions.storage.minio import MinioManager
from yzcore.extensions.storage.amazon import S3Manager
from yzcore.extensions.storage.azure import AzureManager
from yzcore.extensions.storage.base import StorageRequestError
from yzcore.extensions.storage.const import IMAGE_FORMAT_SET, StorageMode
from yzcore.extensions.storage.schemas import OssConfig, ObsConfig, MinioConfig, S3Config, AzureConfig


__all__ = [
    'IMAGE_FORMAT_SET',
    'StorageManage',
    'StorageRequestError',
]


class StorageManage(object):
    """
    通用的对象存储封装，根据mode选择oss/obs等等
    mode,
    access_key_id,
    access_key_secret,
    bucket_name,
    endpoint=None,
    cname=None,
    cache_path='.',
    private_expire_time=30,  # 上传签名有效时间
    private_expire_time=30,  # 加签URl有效时间

    """

    def __new__(cls, storage_conf: dict):
        try:
            mode = StorageMode.__getitem__(storage_conf['mode'].lower()).value
            storage_conf['mode'] = mode
        except KeyError:
            raise KeyError(f'storage mode must be one of [oss|obs|minio|s3|azure], current is "{storage_conf["mode"]}"')

        if mode == 'obs':
            storage_manage = ObsManager(ObsConfig(**storage_conf))
        elif mode == 'oss':
            storage_manage = OssManager(OssConfig(**storage_conf))
        elif mode == 'minio':
            storage_manage = MinioManager(MinioConfig(**storage_conf))
        elif mode == 's3':
            storage_manage = S3Manager(S3Config(**storage_conf))
        elif mode == 'azure':
            storage_manage = AzureManager(AzureConfig(**storage_conf))
        else:
            storage_manage = None
        return storage_manage

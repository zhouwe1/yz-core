from yzcore.extensions.storage.oss import OssManager
from yzcore.extensions.storage.obs import ObsManager
from yzcore.exceptions import StorageError
from yzcore.extensions.storage.const import IMAGE_FORMAT_SET


__all__ = [
    'IMAGE_FORMAT_SET',
    'StorageManage',
    'StorageError',
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

    def __new__(cls, **kwargs):
        if kwargs['mode'].lower() == 'obs':
            storage_manage = ObsManager(**kwargs)
        elif kwargs['mode'].lower() == 'oss':
            storage_manage = OssManager(**kwargs)
        else:
            storage_manage = None
        return storage_manage
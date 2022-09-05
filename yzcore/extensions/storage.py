from yzcore.extensions.aliyun_oss import OssManager
from yzcore.extensions.huawei_obs import ObsManager


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
    expire_time=30,

    """

    def __new__(cls, **kwargs):
        if kwargs['mode'] == 'obs':
            storage_manage = ObsManager(**kwargs)
        elif kwargs['mode'] == 'oss':
            storage_manage = OssManager(**kwargs)
        else:
            storage_manage = None
        return storage_manage

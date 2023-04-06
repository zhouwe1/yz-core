from yzcore.extensions.storage import StorageManage, StorageRequestError
from yzcore.default_settings import default_setting as settings
from abc import ABCMeta, abstractmethod


__all__ = [
    'StorageRequestError',
    'StorageController',
    'StorageManage',
]


class StorageController(metaclass=ABCMeta):
    """
    对象存储控制器
    组织自定义对象存储
    >>> storage_ctrl = await StorageController.init(organiz_id='organiz_id')
    >>> storage_ctrl.organiz_storage_conf  # 组织自定义对象存储配置，未配置则为空字典
    >>> storage_ctrl.storage_conf  # 组织自定义对象存储配置或全局配置
    >>> storage_ctrl.public_storage_manage  # 非加密存储控制器
    >>> storage_ctrl.private_storage_manage  # 加密存储控制器
    全局对象存储
    >>> global_storage_manage = StorageController.sync_init()
    >>> global_storage_manage.public_storage_manage  # 全局非加密存储控制器
    >>> global_storage_manage.private_storage_manage  # 全局加密存储控制器
    """

    global_storage_conf = settings.STORAGE_CONF  # 全局对象存储配置

    def __init__(self, organiz_id):
        """
        不可直接使用，请在 StorageManage.init 初始化
        """
        self.organiz_id = organiz_id
        self.organiz_storage_conf = dict()  # 组织自定义对象存储
        self.storage_conf = dict()  # 对象存储实际使用的配置
        self.storage_mode = ''

    @classmethod
    async def init(cls, organiz_id: str = ''):
        """
        organiz_id为空的情况下返回全局的对象存储
        因在 __init__ 方法中不能调用异步方法，需要在这里初始化"""
        storage_ctrl = cls(organiz_id)
        # 获取组织自定义对象存储配置
        if organiz_id:
            organiz_storage_conf = await storage_ctrl._get_organiz_storage_conf()
            if organiz_storage_conf:
                # 覆盖回全局对象存储配置，避免丢失全局配置
                _storage_conf = cls.global_storage_conf.copy()
                _storage_conf.update(organiz_storage_conf)
                storage_ctrl.organiz_storage_conf = _storage_conf
        storage_ctrl.storage_conf = storage_ctrl.organiz_storage_conf or cls.global_storage_conf
        storage_ctrl.storage_mode = storage_ctrl.storage_conf['mode']
        return storage_ctrl

    @classmethod
    def sync_init(cls):
        """同步方式初始化，只适用于全局存储的初始化"""
        storage_ctrl = cls('')
        storage_ctrl.storage_conf = cls.global_storage_conf
        storage_ctrl.storage_mode = storage_ctrl.storage_conf['mode']
        return storage_ctrl

    @abstractmethod
    async def _get_organiz_storage_conf(self):
        """
        获取组织的自定义对象存储配置
        :return: dict(**organiz_storage_conf) or None
        """

    @property
    def public_storage_manage(self):
        """非加密存储桶控制器"""
        return self._init_public_storage_manage(self.storage_conf)

    @property
    def private_storage_manage(self):
        """加密存储桶控制器"""
        return self._init_private_storage_manage(self.storage_conf)

    @classmethod
    async def check_organiz_conf(cls, organiz_conf: dict):
        """检查自定义对象存储配置是否有效"""
        public_storage = cls._init_public_storage_manage(organiz_conf)
        private_storage = cls._init_private_storage_manage(organiz_conf)
        public_storage.check()
        private_storage.check()

    @classmethod
    def _init_public_storage_manage(cls, storage_conf: dict):
        """
        初始化非加密存储桶
        :param storage_conf:
            字段:
            mode: str
            access_key_id: str
            access_key_secret: str
            endpoint: str
            public_bucket_name: str             # 非加密桶使用
            private_bucket_name: Optional[str]  # 加密桶使用
            image_domain: Optional[str]         # 非加密桶使用，可选
            asset_domain: Optional[str]         # 非加密桶使用，可选
            private_domain: Optional[str]       # 加密桶使用，可选
            private_cname: Optional[str]        # 加密桶使用，可选

        :return: _StorageManage实例，即 ObsManager 或 OssManager
        """
        return StorageManage(
            mode=storage_conf['mode'],
            access_key_id=storage_conf['access_key_id'],
            access_key_secret=storage_conf['access_key_secret'],
            endpoint=storage_conf['endpoint'],
            bucket_name=storage_conf['public_bucket_name'],  # 注意区分加密/非加密存储桶
            image_domain=storage_conf['image_domain'],
            asset_domain=storage_conf['asset_domain'],
            scheme=storage_conf.get('scheme', 'https'),
            cache_path=cls.global_storage_conf['cache_path'],  # 来自全局配置
            policy_expire_time=cls.global_storage_conf['policy_expire_time'],  # 来自全局配置
            private_expire_time=cls.global_storage_conf['private_expire_time'],  # 来自全局配置
        )

    @classmethod
    def _init_private_storage_manage(cls, storage_conf: dict):
        """
        初始化加密存储桶
        :param storage_conf:
            字段:
            mode: str
            access_key_id: str
            access_key_secret: str
            endpoint: str
            public_bucket_name: Optional[str]   # 非加密桶使用
            private_bucket_name: str            # 加密桶使用
            image_domain: Optional[str]         # 非加密桶使用，可选
            asset_domain: Optional[str]         # 非加密桶使用，可选
            private_domain: Optional[str]       # 加密桶使用，可选
            private_cname: Optional[str]        # 加密桶使用，可选

        :return: _StorageManage实例，即 ObsManager 或 OssManager
        """
        return StorageManage(
            mode=storage_conf['mode'],
            access_key_id=storage_conf['access_key_id'],
            access_key_secret=storage_conf['access_key_secret'],
            endpoint=storage_conf['endpoint'],
            bucket_name=storage_conf['private_bucket_name'],  # 注意区分加密/非加密存储桶
            image_domain=storage_conf['private_domain'],
            asset_domain=storage_conf['private_domain'],
            cname=storage_conf['private_cname'],
            scheme=storage_conf.get('scheme', 'https'),
            cache_path=cls.global_storage_conf['cache_path'],  # 来自全局配置
            policy_expire_time=cls.global_storage_conf['policy_expire_time'],  # 来自全局配置
            private_expire_time=cls.global_storage_conf['private_expire_time'],  # 来自全局配置
        )

from pydantic import BaseModel, ValidationError
from .const import StorageMode


class BaseConfig(BaseModel):
    mode: StorageMode
    access_key_id: str
    access_key_secret: str
    bucket_name: str
    endpoint: str
    scheme: str = 'https'
    image_domain: str = None
    asset_domain: str = None
    expire_time: int = 30

    cache_path: str = '.'
    policy_expire_time: int = expire_time  # 上传签名有效时间
    private_expire_time: int = expire_time  # 私有桶访问链接有效时间


class OssConfig(BaseConfig):
    internal_endpoint: str = None


class ObsConfig(BaseConfig):
    pass


class MinioConfig(BaseConfig):
    pass

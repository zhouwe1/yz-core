from typing import Optional
from pydantic import BaseModel, root_validator
from .const import StorageMode, Scheme


class BaseConfig(BaseModel):
    mode: StorageMode
    access_key_id: str
    access_key_secret: str
    bucket_name: str
    endpoint: str
    scheme: Optional[Scheme] = Scheme.https
    image_domain: Optional[str] = None
    asset_domain: Optional[str] = None

    cache_path: Optional[str] = '.'
    policy_expire_time: Optional[int]  # 上传签名有效时间
    private_expire_time: Optional[int]  # 私有桶访问链接有效时间

    @root_validator
    def base_validator(cls, values):
        values['mode'] = values['mode'].value
        if not values['policy_expire_time']:
            values['policy_expire_time'] = 30
        if not values['private_expire_time']:
            values['private_expire_time'] = 3600
        return values


class OssConfig(BaseConfig):
    internal_endpoint: str = None


class ObsConfig(BaseConfig):
    pass


class MinioConfig(BaseConfig):
    internal_endpoint: str = None


class AzureConfig(BaseConfig):
    connection_string: str
    account_key: str
    account_name: str
    access_key_id: Optional[str] = None
    access_key_secret: Optional[str] = None

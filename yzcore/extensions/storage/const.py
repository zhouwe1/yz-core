from enum import Enum

IMAGE_FORMAT_SET = [
    'bmp', 'jpg', 'jpeg', 'png', 'tif', 'gif', 'pcx', 'tga',
    'exif', 'fpx', 'svg', 'psd', 'cdr', 'pcd', 'dxf', 'ufo',
    'eps', 'ai', 'raw', 'WMF', 'webp', 'tiff'
]

DEFAULT_CONTENT_TYPE = 'application/octet-stream'

CONTENT_TYPE = {
    'jpg': 'image/jpeg',
    'jpeg': 'image/jpeg',
    'jfif': 'image/jpeg',
    'jpe': 'image/jpeg',
    'gif': 'image/gif',
    'png': 'image/png',
    'tif': 'image/tiff',
    'tiff': 'image/tiff',

    'txt': 'text/plain',
    'dds': 'application/octet-stream',
}


class StorageMode(Enum):
    oss = 'oss'
    obs = 'obs'
    minio = 'minio'
    s3 = 's3'
    azure = 'azure'


class Scheme(Enum):
    http = 'http'
    https = 'https'

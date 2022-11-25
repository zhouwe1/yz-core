try:
    import oss2

    ACL_TYPE = {
        "private": oss2.BUCKET_ACL_PRIVATE,
        "onlyread": oss2.BUCKET_ACL_PUBLIC_READ,
        "readwrite": oss2.BUCKET_ACL_PUBLIC_READ_WRITE,
    }
    # 存储类型
    STORAGE_CLS = {
        "standard": oss2.BUCKET_STORAGE_CLASS_STANDARD,  # 标准类型
        "ia": oss2.BUCKET_STORAGE_CLASS_IA,  # 低频访问类型
        "archive": oss2.BUCKET_STORAGE_CLASS_ARCHIVE,  # 归档类型
        "cold_archive": oss2.BUCKET_STORAGE_CLASS_COLD_ARCHIVE,  # 冷归档类型
    }
    # 冗余类型
    REDUNDANCY_TYPE = {
        "lrs": oss2.BUCKET_DATA_REDUNDANCY_TYPE_LRS,  # 本地冗余
        "zrs": oss2.BUCKET_DATA_REDUNDANCY_TYPE_ZRS,  # 同城冗余（跨机房）
    }

except ImportError:
    pass

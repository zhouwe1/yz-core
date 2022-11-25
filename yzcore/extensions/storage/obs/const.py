try:
    import obs

    ACL_TYPE = {
        "private": obs.HeadPermission.PRIVATE,
        "onlyread": obs.HeadPermission.PUBLIC_READ,
        "readwrite": obs.HeadPermission.PUBLIC_READ_WRITE,
        "bucket_read": obs.HeadPermission.PUBLIC_READ_DELIVERED,  # 桶公共读，桶内对象公共读。
        "bucket_readwrite": obs.HeadPermission.PUBLIC_READ_WRITE_DELIVERED,  # 桶公共读写，桶内对象公共读写。
        "owner_full_control": obs.HeadPermission.BUCKET_OWNER_FULL_CONTROL,  # 桶或对象所有者拥有完全控制权限。
    }
    # 存储类型
    STORAGE_CLS = {
        "standard": obs.StorageClass.STANDARD,  # 标准类型
        "ia": obs.StorageClass.WARM,  # 低频访问类型
        # "archive": oss2.BUCKET_STORAGE_CLASS_ARCHIVE,  # 归档类型
        "cold_archive": obs.StorageClass.COLD,  # 冷归档类型
    }

    # 冗余类型
    # REDUNDANCY_TYPE = {
    #     "lrs": oss2.BUCKET_DATA_REDUNDANCY_TYPE_LRS,  # 本地冗余
    #     "zrs": oss2.BUCKET_DATA_REDUNDANCY_TYPE_ZRS,  # 同城冗余（跨机房）
    # }

except ImportError:
    pass

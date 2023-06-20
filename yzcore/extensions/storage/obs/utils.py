import functools
from yzcore.extensions.storage.base import StorageRequestError


def wrap_request_return_bool(func):
    """"""

    @functools.wraps(func)
    def wrap_func(*args, **kwargs):
        try:
            resp = func(*args, **kwargs)
            if resp.status < 300:
                return True
            elif resp.status == 404:
                return False
            else:
                raise StorageRequestError(
                    f"static_code: {resp.status}, errorCode: {resp.errorCode}. Message: {resp.errorMessage}.")
        except:
            import traceback
            print(traceback.format_exc())

    return wrap_func

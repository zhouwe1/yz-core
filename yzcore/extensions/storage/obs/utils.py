import functools
from yzcore.extensions.storage.base import StorageRequestError


def wrap_request_return_bool(func):
    """"""

    @functools.wraps(func)
    def wrap_func(*args, **kwargs):
        try:
            resp = func(*args, **kwargs)
            print(resp)
            if resp.status < 300:
                return True
            if resp.status == 403:
                raise StorageRequestError(
                    f"static_code: {resp.status}, errorCode: {resp.errorCode}. Message: {resp.errorMessage}.")
            else:
                return False
        except:
            import traceback
            print(traceback.format_exc())

    return wrap_func

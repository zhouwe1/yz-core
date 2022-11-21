import functools


def wrap_request_return_bool(func):
    """"""

    @functools.wraps(func)
    def wrap_func(*args, **kwargs):
        try:
            resp = func(*args, **kwargs)
            if resp.status < 300:
                return True
            else:
                return False
        except:
            import traceback
            print(traceback.format_exc())

    return wrap_func

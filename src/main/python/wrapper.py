import time
from functools import wraps

def timer(func):
    """Time wrapper to calcaulate the time use for running the functions.

    Args:
        func: the measured function.

    Returns:
        The wrapped function.
    """
    @wraps(func)
    def wrap(*args, **kw):
        ts = time.time()
        result = func(*args, **kw)
        te = time.time()
        print(f"Total run time = {te-ts}")
        return result
    return wrap
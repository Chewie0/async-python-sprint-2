from functools import wraps

EVENTS = {
    'pending_jobs': (),
    'error_jobs': (),
    'done_jobs': (),
    'started_jobs': ()
}


def coroutine(f):
    @wraps(f)  # https://docs.python.org/3/library/functools.html#functools.wraps
    def wrap(*args, **kwargs):
        gen = f(*args, **kwargs)
        gen.send(None)
        return gen

    return wrap

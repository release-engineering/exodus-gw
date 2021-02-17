import threading
from functools import wraps


def assert_not_main_thread(message):
    """Assert that we are not currently in the main thread.

    'message' should briefly summarize what we are currently doing (e.g. "call foo").
    It'll be included in the error message.
    """

    thread = threading.currentThread()
    if thread != threading.main_thread():
        # OK, fine
        return

    raise AssertionError(
        "Attempted to %s from within main thread - using blocking function from async?"
        % message
    )


def ensuring_nonblock(fn):
    """Return a version of 'fn' wrapped with an assertion that we're not in the
    main thread.
    """

    @wraps(fn)
    def new_fn(*args, **kwargs):
        assert_not_main_thread("invoke %s" % fn)
        return fn(*args, **kwargs)

    return new_fn


class BlockDetector:
    """A helper to wrap an object with assertions protecting against
    certain kinds of async bugs.

    Some objects, such as an sqlalchemy session, are not designed with
    an async API and will perform potentially slow operations during
    use, e.g. communicating with a remote server during 'commit'.
    Those objects shouldn't be used from within an 'async' endpoint,
    or if used, should be done by calling out to a separate thread.

    This wrapper helps to enforce correct usage of such objects while
    tests are running. When accessed through the main thread, an assertion
    will be raised, generally indicating that an object was wrongly used
    from within an 'async' function. When accessed through any other
    thread, it behaves as the underlying object, as if there were no
    wrapper.
    """

    def __init__(self, delegate):
        self.__delegate = delegate

    def __getattr__(self, name):
        # Get attribute from the real object.
        out = getattr(self.__delegate, name)

        # If it was a function, wrap it such that it will raise if called
        # from within the main thread.
        if callable(out):
            out = ensuring_nonblock(out)

        return out

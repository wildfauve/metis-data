from metis_job.util import logger, singleton

class Initialiser(singleton.Singleton):
    init_fns = []

    def add_initialiser(self, f, order):
        self.init_fns.append((f, order))

    def invoke_fns(self):
        [self._invoke(f) for f, _ in sorted(self.init_fns, key=lambda f: f[1])]

    def _invoke(self, f):
        result = f()
        if result.is_right():
            status = "ok"
        else:
            status = f"fail: error: {result.error().message}"
            logger.info(f"Calling Initialisation fn: {f.__name__} with result: {status}")
        return result


def register(order: int):
    """
    Decorator for registering initialisers to be run prior to the main handler execution.  Note that the module containing
    the random_initialisers must be imported before the runner entry point is called.

    @helpers.register(order=1)
    def session_builder():
        pass

    All registered initialisers are invoked, in the order defined by the order arg
    """

    def inner(f):
        Initialiser().add_initialiser(f=f, order=order)

    return inner


def initialisation_runner():
    Initialiser().invoke_fns()

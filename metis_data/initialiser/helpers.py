from metis_data.util import logger, singleton


class Initialiser(singleton.Singleton):
    init_fns = []

    def add_initialiser(self, f, order, log_state):
        self.init_fns.append((f, order, log_state))

    def invoke_fns(self):
        [self._invoke(f, log_state) for f, _, log_state in sorted(self.init_fns, key=lambda f: f[1])]

    def _invoke(self, f, log_state):
        result = f()
        self.log_result(result, log_state)
        return result

    def log_result(self, result, log_state):
        if result.is_right():
            msg = f"Called Initialisation fn: {f.__name__} with result: OK"
        else:
            status = f"fail: error: {result.error().message}"
            msg = f"Called Initialisation fn: {f.__name__} with result: {status}"

        if log_state:
            logger.info(msg)


def register(order: int, log_state: bool = True):
    """
    Decorator for registering initialisers to be run prior to the main handler execution.  Note that the module containing
    the initialisers must be imported before the runner entry point is called.

    @helpers.register(order=1)
    def session_builder():
        pass

    All registered initialisers are invoked, in the order defined by the order arg
    """

    def inner(f):
        Initialiser().add_initialiser(f=f, order=order, log_state=log_state)

    return inner


def initialisation_runner():
    Initialiser().invoke_fns()

from typing import Callable, Protocol, Any

from metis_data import runner
from metis_data.util import logger, mod, singleton, monad


class InitialisationResultProtocol(Protocol):
    @property
    def results(self) -> list[Any]:
        """
        The result collection
        """
        ...

    def add_initialisation_result(self, result: monad.Either):
        """
        Appends the result emitted from the initialiser to a result collection
        """
        ...


def job(initialiser_module: str = None,
        initialisation_result_state: InitialisationResultProtocol = None):
    """
    Job provides a decorator which wraps the execution of a spark runner.  You use the decorator at the entry point of the runner

    @spark_job.runner(initialiser_module="tests.shared.initialisers")
    def execute(args=None, location_partitioner: Callable = date_partitioner) -> monad.EitherMonad[value.JobState]:
        pass

    The initialiser_module provided to the decorator is a module in import path format.  All non "__init__.py" modules
    in this module will be dynamically imported, and those which are decorated with @spark_job.register() will be
    executed before the runner starts.  This is a great place to include any state, functions, etc, which need to be
    initialised before starting the runner.

    Job does the following:
    + It calls the random_initialisers to run all the initialisations registered
    + It then invokes the runner function with all args and kwargs.
    + At runner completion it simply returns whatever the runner function returned.

    """

    def inner(fn):
        def invoke(*args, **kwargs):
            init_mod = kwargs.get('initialiser_module', None) or initialiser_module
            if init_mod:
                mod.import_module(init_mod)
                initialisation_runner(initialisation_result_state)
            result = fn(*args, **kwargs)
            return result

        return invoke

    return inner


def simple_spark_job(from_input: Callable,
                     to_table: Callable,
                     transformer: Callable):
    """
    """

    def inner(fn):
        def invoke(*args, **kwargs):
            job_runner = runner.build_simple_run(from_input=from_input,
                                                 transformer=transformer,
                                                 to_table=to_table)

            pre_run_result = fn(**{**kwargs, **{"runner": job_runner}})

            if isinstance(pre_run_result, tuple):
                job_runner, ctx, callback = pre_run_result
                job_runner.with_run_ctx(ctx).after_run_callback(callback)

            result = job_runner.run()

            if job_runner.callback and callable(job_runner.callback):
                return job_runner.callback(result)
            return result

        return invoke

    return inner


# def simple_streaming_job(from_table,
#                          to_table,
#                          transformer: Callable,
#                          write_type: model.StreamWriteType,
#                          from_reader_options: Set[repo.ReaderSwitch] = None,
#                          options: List[repo.SparkOption] = None):
#     """
#     """
#     def inner(fn):
#         def invoke(*args, **kwargs):
#             result = simple_streamer.run(from_table=from_table,
#                                          to_table=to_table,
#                                          transformer=transformer,
#                                          write_type=write_type,
#                                          from_reader_options=from_reader_options,
#                                          options=options)
#
#             return fn(result=result)
#
#         return invoke
#
#     return inner


class Initialiser(singleton.Singleton):
    init_fns: list[tuple[callable, int, bool]] = []

    def add_initialiser(self, f, order, log_state):
        self.init_fns.append((f, order, log_state))

    def invoke_fns(self, initialisation_result_state: InitialisationResultProtocol = None):
        [self._invoke(f, log_state, initialisation_result_state) for f, _, log_state in
         sorted(self.init_fns, key=lambda f: f[1])]

    def _invoke(self,
                f,
                log_state,
                initialisation_result_state: InitialisationResultProtocol = None):
        if initialisation_result_state:
            result = f(initialisation_result_state)
            initialisation_result_state.add_initialisation_result(result)
        else:
            result = f()
        self.log_result(f, result, log_state)
        return result

    def log_result(self, f, result, log_state):
        if result.is_right():
            msg = f"Called Initialisation fn: {f.__name__} with result: OK"
        else:
            status = f"fail: error: {result.error().message}"
            msg = f"Called Initialisation fn: {f.__name__} with result: {status}"

        if log_state:
            logger.info(msg)


def initialiser_register(order: int, log_state: bool = True):
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


def initialisation_runner(initialisation_result_state: InitialisationResultProtocol = None):
    Initialiser().invoke_fns(initialisation_result_state)

import pytest
from bevy import dependency, inject

import metis_data

from metis_data.util import error, monad, fn
from metis_data.job import Initialiser

from tests.shared import init_state_spy, data, namespaces_and_tables


class InitialisationState(metis_data.InitialisationResultProtocol):
    init_results: list = []

    @property
    def results(self):
        return self.init_results

    def add_initialisation_result(self, result):
        self.results.append(result)

init_state = InitialisationState()

def setup_function():
    init_state_spy.InitState().clear_state()
    Initialiser.init_fns = []


def it_runs_a_job_and_runs_init_fn_in_order():
    result = setup_noop_job()()

    assert result.is_right()

    init_state = init_state_spy.InitState().state

    assert init_state == ['random-random_initialisers-1-run', 'random-random_initialisers-2-run']


def it_provides_state_based_init():
    init_state.init_results = []
    result = setup_state_based_job()()

    assert fn.bool_fn_with_predicate(init_state.results, all, monad.maybe_value_ok)
    assert list(map(monad.Lift, init_state.results)) == ["OK", "OK"]


def it_runs_a_simple_batch_job(namespace_wrapper):
    result = set_up_batch_job()(args=sys_args_())

    assert result.is_right()

    assert result.value.input_df
    assert result.value.transformed_df

    df = my_table_2().read()
    assert df.count() == 2


# Helpers

def clear_init_fns():
    Initialiser.init_fns = []

def get_some_input(ctx):
    return monad.Right(data.my_table_df())


def write_to_table(df, ctx):
    return my_table_2().try_upsert(df)


def noop_transformer(df, ctx):
    return monad.Right(df)


def setup_noop_job():
    @metis_data.job(initialiser_module='tests.shared.random_initialisers')
    def run_noop_job():
        return monad.Right(True)
    return run_noop_job


def setup_state_based_job():
    @metis_data.job(initialiser_module='tests.shared.state_based_initialisers',
                    initialisation_result_state=init_state)
    def run_noop_job_with_state_based_init():
        return monad.Right(True)
    return run_noop_job_with_state_based_init


def set_up_batch_job():
    @metis_data.job(initialiser_module='tests.shared.job_initialisers')
    @metis_data.simple_spark_job(from_input=get_some_input,
                                 transformer=noop_transformer,
                                 to_table=write_to_table)
    def run_batch_job(args, runner: metis_data.SimpleJob):
        return runner, "-".join(args), run_job_callback
    return run_batch_job


def run_job_callback(result: monad.Either[error.BaseError, metis_data.SimpleJobValue]):
    job = result.value
    return monad.Right(job.replace('run_ctx', f"{job.run_ctx}-->Done!"))


@inject
def my_table_2(table: namespaces_and_tables.MyTable2 = dependency()):
    return table


def sys_args_():
    return ["--some-system-args"]

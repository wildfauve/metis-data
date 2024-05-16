from bevy import dependency, inject

import metis_job

from metis_job.util import error, monad
from tests.shared import init_state_spy, data, namespaces_and_tables


def setup_function():
    init_state_spy.InitState().clear_state()


def it_runs_a_job():
    result = run_noop_job()

    assert result.is_right()


def it_runs_the_initialisers_in_order():
    run_noop_job()

    init_state = init_state_spy.InitState().state

    assert init_state == ['random-random_initialisers-1-run', 'random-random_initialisers-2-run']


def it_runs_a_simple_batch_job(namespace_wrapper):
    result = run_batch_job(args=sys_args_())

    assert result.is_right()

    assert result.value.input_df
    assert result.value.transformed_df

    df = my_table_2().read()
    assert df.count() == 2


# Helpers

def get_some_input(ctx):
    return monad.Right(data.my_table_df())


def write_to_table(df, ctx):
    return my_table_2().try_upsert(df)


def noop_transformer(df, ctx):
    return monad.Right(df)


@metis_job.job(initialiser_module='tests.shared.random_initialisers')
def run_noop_job():
    return monad.Right(True)


@metis_job.job(initialiser_module='tests.shared.job_initialisers')
@metis_job.simple_spark_job(from_input=get_some_input,
                            transformer=noop_transformer,
                            to_table=write_to_table)
def run_batch_job(args, runner: metis_job.SimpleJob):
    return runner, "-".join(args), run_job_callback


def run_job_callback(result: monad.Either[error.BaseError, metis_job.SimpleJobValue]):
    job = result.value
    return monad.Right(job.replace('run_ctx', f"{job.run_ctx}-->Done!"))


@inject
def my_table_2(table: namespaces_and_tables.MyTable2 = dependency()):
    return table


def sys_args_():
    return ["--some-system-args"]

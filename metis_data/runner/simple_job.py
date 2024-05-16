from typing import Callable

from . import job_runner


def build_simple_run(from_input: Callable,
                     transformer: Callable,
                     to_table: Callable):
    return run_model(from_input, to_table, transformer)


def run_model(from_input: Callable,
              to_table: Callable,
              transformer: Callable):
    return (job_runner.SimpleJob().from_input(from_input)
            .with_transformer(transformer)
            .to_table(to_table))

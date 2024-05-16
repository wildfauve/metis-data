from typing import Callable, Tuple, Dict, List, Optional, Set
from enum import Enum
from uuid import uuid4

from pyspark.sql import dataframe

from . import value
from metis_data.util import error, monad


class SimpleJob:

    def __init__(self):
        self.input_fn = None
        self.transformer_fn = None
        self.writer_fn = None
        self.runner = Runner()
        self.batcher = None
        self.ctx = None
        self.callback = None

    def from_input(self, input_fn: Callable):
        self.input_fn = input_fn
        return self

    def with_transformer(self, transformer_fn: Callable):
        self.transformer_fn = transformer_fn
        return self

    def to_table(self, table_fn: Callable):
        self.writer_fn = table_fn
        return self

    def with_run_ctx(self, ctx):
        self.ctx = ctx
        return self

    def after_run_callback(self, callback: Callable):
        self.callback = callback
        return self

    def run(self) -> monad.Either:
        result = self.runner.run(self)
        if result.is_left():
            return monad.Left(result.error())
        return result

    # def __repr__(self):
    #     return f"""{self.__class__}
    #     From Input Fn: {self.input_fn}
    #     To Table Fn: {self.writer_fn}
    #     Transformer:
    #             |_ Fn: {self.transformer_fn}
    #     """


class Runner:

    def run(self, batch):
        return (self.setup_value(batch)
                >> self.data_getter
                >> self.transformer
                >> self.writer)

    def setup_value(self, batch):
        val = value.SimpleJobValue(config=batch)
        val.run_ctx = batch.ctx
        return monad.Right(val)

    def data_getter(self, val: value.SimpleJobValue):
        result = val.config.input_fn(**self._kw_args(val))
        if result.is_left():
            val.error = result.error()
            return monad.Left(val)
        return monad.Right(val.replace('input_df', result.value))

    def transformer(self, val: value.SimpleJobValue):
        result = val.config.transformer_fn(val.input_df, **self._kw_args(val))
        if result.is_left():
            val.error = result.error()
            return monad.Left(val)
        return monad.Right(val.replace('transformed_df', result.value))

    def writer(self, val: value.SimpleJobValue):
        result = val.config.writer_fn(val.transformed_df, **self._kw_args(val))
        if result.is_left():
            val.error = result.error()
            return monad.Left(val)
        return monad.Right(val)

    def _kw_args(self, val: value.SimpleJobValue):
        if val.run_ctx:
            return {'ctx': val.run_ctx}
        return {}
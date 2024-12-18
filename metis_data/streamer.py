from dataclasses import dataclass
from typing import Callable, Tuple, Dict, List, Optional, Set, Any
from enum import Enum
from uuid import uuid4
from pyspark.sql import dataframe

import metis_data
from metis_data import const
from metis_data.util import monad, error, logger
from metis_data import repo

logging_ns = f"{const.NS}.streamer"


@dataclass
class DataClassAbstract:
    def replace(self, key, value):
        setattr(self, key, value)
        return self


@dataclass
class StreamState(DataClassAbstract):
    """
    stream_configuration: The model.Streamer object created to configure the stream.
    stream_input_dataframe:  The DF used as input into the stream (the read of the to_table)
    stream_transformed_dataframe:  The DF generated as output from the transformation
    error:  An optional error object subclassed from Exception.
    """
    stream_configuration: Any
    streaming_input_dataframe: Optional[dataframe.DataFrame] = None
    stream_transformed_dataframe: Optional[dataframe.DataFrame] = None
    exception: Optional[error.BaseError] = None


class StreamWriteType(Enum):
    APPEND = "try_write_stream"
    UPSERT = "try_stream_write_via_delta_upsert"


def dataframe_not_streaming():
    return error.generate_error(error.RepoConfigurationError, ("streamer", 1))


def stream_writer_error(exception: error.BaseError, table_name, f_name):
    return error.generate_error(error.StreamerWriterError, ("streamer", 2),
                                cause=exception.message,
                                table_name=table_name,
                                function_name=f_name,
                                traceback=exception.traceback)


def stream_reader_error(exception: error.BaseError):
    return error.generate_error(error.TableStreamReadError, ("streamer", 3),
                                cause=exception.message,
                                traceback=exception.traceback)


class StreamToPair:

    def __init__(self):
        self.stream_to_table = None
        self.transformer = None
        self.transformer_context = None
        self.transformed_df = None
        self.stream_write_type = None
        self.stream_write_options = []

    def stream_to(self,
                  table: metis_data.DomainTable,
                  write_type: StreamWriteType = StreamWriteType.APPEND,
                  options: Optional[List[metis_data.SparkOption]] = None,
                  stream_trigger_condition: dict | None = None):
        self.stream_to_table = table
        self.stream_write_type = write_type
        self.stream_write_options = options if options else []
        self.stream_trigger_condition = stream_trigger_condition
        return self

    def with_transformer(self, transformer: Callable, **kwargs):
        self.transformer = transformer
        self.transformer_context = kwargs
        return self

    def apply_transformer(self, input_df):
        self.transformed_df = self.transformer(input_df, **self.transformer_context)

        if not (isinstance(self.transformed_df, dataframe.DataFrame) and self.transformed_df.isStreaming):
            return monad.Left(dataframe_not_streaming())
        return monad.Right(self.transformed_df)

    def run_stream(self):
        """
        Invokes the repo function to start and run the stream, providing the transformation df as an input.
        The stream_write_type enum value provides the streaming write type to call on the repo, either an append
        or an upsert.

        :return:
        """
        return (getattr(self.stream_to_table, self.stream_write_type.value)
                (stream=self.transformed_df,
                 trigger=self.stream_trigger_condition,
                 options=self.stream_write_options))

    def await_termination(self):
        return self.stream_to_table.await_termination(options_for_unsetting=self.stream_write_options)


class MultiStreamer:
    def __init__(self,
                 stream_from_table: metis_data.DomainTable = None,
                 stream_from_reader_options: set[repo.ReaderSwitch] = None):
        self.stream_id = str(uuid4())
        self.stream_from_table = stream_from_table
        self.stream_from_reader_options = stream_from_reader_options
        self.runner = Runner()
        self.stream_pairs = []
        self.multi = True

    def stream_from(self, table: metis_data.DomainTable, stream_from_reader_options: set[repo.ReaderSwitch] = None):
        self.stream_from_table = table
        self.stream_from_reader_options = stream_from_reader_options
        return self

    def with_stream_to_pair(self, stream_to_pair: StreamToPair):
        self.stream_pairs.append(stream_to_pair)
        return self

    def run(self) -> monad.EitherMonad[StreamState]:
        result = self.runner.run(self)
        if result.is_left():
            return monad.Left(result.error)
        return result


class Streamer:

    def __init__(self,
                 stream_from_table: metis_data.DomainTable = None,
                 stream_from_reader_options: set[repo.ReaderSwitch] = None,
                 stream_from_to: metis_data.DomainTable = None,
                 transformer: Callable = None,
                 transformer_context: Dict = None,
                 partition_with: Tuple = None):
        self.stream_id = str(uuid4())
        self.runner = Runner()
        self.stream_to_table = stream_from_to
        self.stream_from_table = stream_from_table
        self.stream_from_reader_options = stream_from_reader_options
        self.transformer = transformer
        self.transformer_context = transformer_context if transformer_context else dict()
        self.stream_write_type = None
        self.stream_write_options = []
        self.multi = False

    def stream_from(self,
                    table: metis_data.DomainTable | metis_data.CloudFiles,
                    stream_from_reader_options: Set[repo.ReaderSwitch] = None):
        self.stream_from_table = table
        self.stream_from_reader_options = stream_from_reader_options
        return self

    def stream_to(self,
                  table: metis_data.DomainTable,
                  partition_columns: Tuple[str] = tuple(),
                  write_type: StreamWriteType = StreamWriteType.APPEND,
                  options: list[repo.SparkOption] = None,
                  stream_trigger_condition: dict | None = None):
        self.stream_to_table = table
        self.partition_with = partition_columns
        self.stream_write_type = write_type
        self.stream_write_options = options if options else []
        self.stream_trigger_condition = stream_trigger_condition
        return self

    def with_transformer(self, transformer: Callable, **kwargs):
        self.transformer = transformer
        self.transformer_context = kwargs
        return self

    def run(self) -> monad.EitherMonad[StreamState]:
        result = self.runner.run(self)
        if result.is_left():
            return monad.Left(result.error())
        return result

    def __repr__(self):
        return f"""{self.__class__}
        StreamId: {self.stream_id}
        Stream From Table: {self.stream_from_table}
        Stream To: 
                |_ Table: {self.stream_to_table}
                |_ Partition: {self.partition_with}
                |_ WriteType: {self.stream_write_type}
                |_ Options: {self.stream_write_options}
        Transformer:
                |_ Fn: {self.transformer}
                |_ ctx: {self.transformer_context}
        """


class Runner:

    def run(self, stream):
        return (self.setup_value(stream)
                >> self.stream_initiator
                >> self.transformer_strategy
                >> self.write_stream_strategy)

    def setup_value(self, stream):
        return monad.Right(StreamState(stream_configuration=stream))

    def stream_initiator(self, val: StreamState) -> monad.EitherMonad[StreamState]:
        result = (val.stream_configuration
                  .stream_from_table
                  .try_read_stream(val.stream_configuration.stream_from_reader_options))
        if result.is_left():
            error = stream_reader_error(result.error())
            return monad.Left(val.replace('exception', error))
        if not (isinstance(result.value, dataframe.DataFrame) and result.value.isStreaming):
            return monad.Left(val.replace('exception', dataframe_not_streaming()))
        return monad.Right(val.replace('streaming_input_dataframe', result.value))

    def transformer_strategy(self, val: StreamState) -> monad.EitherMonad[StreamState]:
        if val.stream_configuration.multi:
            return self.apply_multi_transformers(val)
        return self.apply_transformer(val)

    def apply_multi_transformers(self, val: StreamState) -> monad.EitherMonad[StreamState]:
        results = [pair.apply_transformer(val.streaming_input_dataframe) for pair in
                   val.stream_configuration.stream_pairs]

        if not all(map(monad.maybe_value_ok, results)):
            monad.Left(val.replace('exception', dataframe_not_streaming()))
        return monad.Right(val)

    def apply_transformer(self, val: StreamState) -> monad.EitherMonad[StreamState]:
        result = self.try_transformer(val)

        if result.error():
            return monad.Left(val.replace('exception', result.error()))
        if not (isinstance(result.value, dataframe.DataFrame) and result.value.isStreaming):
            return monad.Left(val.replace('exception', dataframe_not_streaming()))
        return monad.Right(val.replace('stream_transformed_dataframe', result.value))

    @monad.Try(error_cls=error.StreamerTransformerError)
    def try_transformer(self, val: StreamState) -> monad.EitherMonad:
        return (val.stream_configuration.transformer(val.streaming_input_dataframe,
                                                     **val.stream_configuration.transformer_context))

    def write_stream_strategy(self, val: StreamState) -> monad.EitherMonad[StreamState]:
        if val.stream_configuration.multi:
            return self.start_and_run_multi_streams(val)
        return self.run_and_write_stream(val)

    def start_and_run_multi_streams(self, val: StreamState) -> monad.EitherMonad[StreamState]:
        results = [pair.run_stream() for pair in val.stream_configuration.stream_pairs]

        if not all(map(monad.maybe_value_ok, results)):
            monad.Left(val.replace('exception', monad.Left("Boom!")))
        return monad.Right(val)

    def run_and_write_stream(self, val: StreamState) -> monad.EitherMonad[StreamState]:
        """
        Invokes the repo function to start and run (write to) the stream, providing the transformation df as an input.
        The stream_write_type enum value provides the streaming write type to call on the repo, either an append
        or an upsert.

        :param val:
        :return:
        """
        result = (getattr(val.stream_configuration.stream_to_table, val.stream_configuration.stream_write_type.value)
                  (df=val.stream_transformed_dataframe,
                   trigger_condition=val.stream_configuration.stream_trigger_condition,
                   spark_options=val.stream_configuration.stream_write_options))

        if result.is_left():
            logger.error(
                f"{logging_ns}.Runner.run_and_write_streams: Error {result.error().message}")
            error = stream_writer_error(result.error(),
                                        val.stream_configuration.stream_to_table.table_name,
                                        val.stream_configuration.stream_write_type.value)
            return monad.Left(val.replace('exception', error))
        return monad.Right(val)

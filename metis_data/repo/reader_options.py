from __future__ import annotations

from enum import Enum
from functools import reduce, partial
from typing import Optional

from metis_data.util import fn


class ReaderSwitch(Enum):
    READ_STREAM_WITH_SCHEMA_ON = ('read_stream_with_schema', True)  # Read a stream with a schema applied.
    READ_STREAM_WITH_SCHEMA_OFF = ('read_stream_with_schema', False)  # with no schema applied.

    GENERATE_DF_ON = ("generate_df", True)  # for Delta Table reads, return a DF rather than the delta table object
    GENERATE_DF_OFF = ("generate_df", False)  # return a delta table object

    @classmethod
    def merge_options(cls, defaults: Optional[set], overrides: Optional[set] = None) -> set[ReaderSwitch]:
        if overrides is None:
            return defaults

        return reduce(cls.merge_switch, overrides, defaults)

    @classmethod
    def merge_switch(cls, options, override):
        default_with_override = fn.find(partial(cls.option_predicate, override.value[0]), options)
        if not default_with_override:
            options.add(override)
            return options
        options.remove(default_with_override)
        options.add(override)
        return options

    @classmethod
    def option_predicate(cls, option_name, option):
        return option_name == option.value[0]

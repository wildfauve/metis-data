from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pyspark.sql import DataFrame

from metis_data.util import dataklass, error

from . import job_runner


@dataclass
class SimpleJobValue(dataklass.BaseValue):
    """
    """
    config: job_runner.SimpleJob
    input_df: DataFrame | None = None
    transformed_df: DataFrame | None = None
    run_ctx: Any | None = None
    error: error.BaseError | None = None

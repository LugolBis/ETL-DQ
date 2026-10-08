from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl
from polars import DataFrame


def compute_completude(df: DataFrame, columns: list[str]) -> DataFrame:
    """Return : Outpout schema {'column_name': str, 'completude_percent': float, 'timestamp': timestamp}"""
    completude = df.select(pl.col(columns).is_not_null().mean()).to_dicts()[0]
    timestamp = datetime.now(ZoneInfo("UTC"))

    return pl.DataFrame(
        {
            "column_name": list(completude.keys()),
            "completude_percent": list(completude.values()),
            "timestamp": timestamp,
        }
    )

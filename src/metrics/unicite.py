from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl
from polars import DataFrame


def compute_unicite(df: DataFrame, columns: list[str], source: str) -> DataFrame:

    unicite = df.select(pl.col(columns)).is_unique().mean()
    timestamp = datetime.now(ZoneInfo("UTC"))

    return pl.DataFrame(
        {
            "source": source,
            "column_name": ", ".join(columns),
            "unicite_percent": unicite,
            "timestamp": timestamp,
        }
    )
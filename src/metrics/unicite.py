from datetime import datetime

import polars as pl
from polars import DataFrame


def compute_unicite(
    df: DataFrame, columns: list[str], source: str, timestamp: datetime
) -> DataFrame:

    unicite = df.select(pl.col(columns)).is_unique().mean()

    return pl.DataFrame(
        {
            "source": source,
            "column_name": ", ".join(columns),
            "unicite_percent": unicite,
            "timestamp": timestamp,
        }
    )

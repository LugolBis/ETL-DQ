from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl
from polars import DataFrame


def text_format(
    df: pl.DataFrame,
    columns: list[str],
    regex: str,
    source: str,
) -> DataFrame:
    n = df.height
    timestamp = datetime.now(ZoneInfo("UTC"))

    counts = df.select(pl.col(columns).cast(pl.String).str.contains(regex).sum())

    result = counts.unpivot(
        on=columns,
        variable_name="column_name",
        value_name="match_count",
    )

    return result.select(
        pl.lit(source).alias("source"),
        pl.col("column_name"),
        (pl.col("match_count") / n).alias("format_percent"),
        pl.lit(timestamp).alias("timestamp"),
    )


def label_set(
    df: DataFrame, columns: list[str], labels: list[str], source: str
) -> DataFrame:
    n = df.height
    timestamp = datetime.now(ZoneInfo("UTC"))

    labels_percent = [df[col].is_in(labels).sum() / n for col in columns]

    return pl.DataFrame(
        {
            "source": source,
            "column_name": columns,
            "labels_percent": labels_percent,
            "timestamp": timestamp,
        }
    )


def interval_validity(
    df: DataFrame, columns: list[str], min_val: float, max_val: float, source: str
) -> DataFrame:
    n = df.height
    timestamp = datetime.now(ZoneInfo("UTC"))

    count = df.select(
        ((pl.col(columns) >= min_val) & (pl.col(columns) <= max_val)).sum()
    )

    result = count.unpivot(
        on=columns,
        variable_name="column_name",
        value_name="match_count",
    )

    return result.select(
        pl.lit(source).alias("source"),
        pl.col("column_name"),
        (pl.col("match_count") / n).alias("interval_percent"),
        pl.lit(timestamp).alias("timestamp"),
    )

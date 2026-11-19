from datetime import datetime

import polars as pl
from polars import DataFrame


def compute_completude(
    df: DataFrame,
    columns: list[str],
    source: str,
    timestamp: datetime,
) -> tuple[DataFrame, DataFrame]:
    """Return (df_rids, df_metric).

    `df_rids` schema:
        {'row_id': int, 'source': str, 'column_name': str}
        One row per (tuple, column) where tuple[column] is Null.

    `df_metric` schema:
        {'source': str, 'column_name': str,
         'completude_percent': float, 'timestamp': datetime}
    """
    n = df.height

    df_rids = (
        df.with_row_index("row_id")
        .select(["row_id", *columns])
        .unpivot(
            index="row_id",
            on=columns,
            variable_name="column_name",
            value_name="value",
        )
        .filter(pl.col("value").is_null())
        .drop("value")
        .with_columns(pl.lit(source).alias("source"))
        .select(["source", "column_name", "row_id"])
    )

    df_nulls_count = df_rids.group_by("column_name").agg(pl.len().alias("null_count"))

    df_metric = (
        pl.DataFrame({"column_name": columns})
        .join(df_nulls_count, on="column_name", how="left", maintain_order="left")
        .with_columns(pl.col("null_count").fill_null(0))
        .with_columns(
            ((pl.lit(n) - pl.col("null_count")) / pl.lit(n))
            .cast(pl.Float64)
            .alias("completude_percent")
        )
        .select(
            pl.lit(source).alias("source"),
            "column_name",
            "completude_percent",
            pl.lit(timestamp).alias("timestamp"),
        )
    )

    return df_rids, df_metric

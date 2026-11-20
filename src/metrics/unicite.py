from datetime import datetime

import polars as pl
from polars import DataFrame


def compute_unicite(
    df: DataFrame,
    columns: list[str],
    source: str,
    timestamp: datetime,
) -> tuple[DataFrame, DataFrame]:
    """
    Return (`df_rids`, `df_metric`) :

    `df_rids` schema :
        {'source': str, 'column_name': str, 'row_id': int}

    `df_metric` schema :
        {'source': str, 'column_name': str, 'unicite_percent': float, 'timestamp': datetime}
    """
    n = df.height
    column_label = ", ".join(columns)

    df_bitmap = df.with_row_index("row_id").select(
        pl.col("row_id").cast(pl.Int64),
        pl.struct(columns).is_duplicated().alias("is_failed"),
    )

    df_rids = df_bitmap.filter(pl.col("is_failed")).select(
        "row_id",
        pl.lit(source).alias("source"),
        pl.lit(column_label).alias("column_name"),
    )

    df_metric = pl.select(
        pl.lit(source).alias("source"),
        pl.lit(column_label).alias("column_name"),
        (pl.lit(1.0) - pl.lit(df_rids.height) / n).alias("unicite_percent"),
        pl.lit(timestamp).alias("timestamp"),
    )

    return df_rids, df_metric

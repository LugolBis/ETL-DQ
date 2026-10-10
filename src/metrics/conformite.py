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
    df: DataFrame, columns: list[str], labels: list[str | int | float], source: str
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
    df: DataFrame, columns: list[tuple[str, str, str]], source: str
) -> DataFrame:
    n = df.height
    timestamp = datetime.now(ZoneInfo("UTC"))

    exprs: list[pl.Expr] = []
    for c0, c1, c2 in columns:
        exprs.append((~((pl.col(c0) <= pl.col(c1)) & (pl.col(c1) <= pl.col(c2)))).sum())

    counts = df.select(exprs).row(0)  # tuple des comptes

    results = []
    for (c0, c1, c2), count in zip(columns, counts):
        results.append(
            {
                "column_name_min": c0,
                "column_name_target": c1,
                "column_name_max": c2,
                "count": count,
            }
        )

    result = pl.DataFrame(results)

    return result.select(
        pl.lit(source).alias("source"),
        pl.col("column_name_min"),
        pl.col("column_name_target"),
        pl.col("column_name_max"),
        (pl.col("count") / n).alias("interval_percent"),
        pl.lit(timestamp).alias("timestamp"),
    )


def foreign_key_validity(
    df1: DataFrame,
    df2: DataFrame,
    columns: list[tuple[str, str]],
    sources: tuple[str, str],
) -> DataFrame:
    """
    We check the percent of values for the `columns` pairs, in the `df1` that are included in the `df2`.
    Note that `columns` is a list of pair of columns, the first one refer to a coluln of `df1` (and the second one to `df2`).
    """

    n = df1.height
    timestamp = datetime.now(ZoneInfo("UTC"))
    src1, src2 = sources

    result = pl.concat(
        [
            df1.select(pl.col(col1).alias("value"))
            .join(
                df2.select(pl.col(col2).alias("value")).unique(),
                on="value",
                how="semi",
            )
            .select(
                pl.lit(src1).alias("source_1"),
                pl.lit(src2).alias("source_2"),
                pl.lit(col1).alias("column_1"),
                pl.lit(col2).alias("column_2"),
                (pl.len() / n).alias("fk_percent"),
            )
            for col1, col2 in columns
        ]
    )

    return result.with_columns(pl.lit(timestamp).alias("timestamp"))

from collections.abc import Sequence
from datetime import datetime

import polars as pl
from polars import DataFrame

from orchestration.tasks.transform.helpers import expand_column, fuzzy_join


def _get_df_rids(df_bitmap: DataFrame, columns: list[str], source: str) -> DataFrame:
    return (
        df_bitmap.unpivot(
            on=columns,
            variable_name="column_name",
            value_name="is_failed",
        )
        .filter(pl.col("is_failed"))
        .select(
            pl.lit(source).alias("source"),
            "column_name",
            "row_id",
        )
    )


def text_format(
    df: DataFrame, columns: list[str], regex: str, source: str, timestamp: datetime
) -> tuple[DataFrame, DataFrame]:
    """
    Return (`df_rids`, `df_metric`) :

    `df_rids` schema :
        {'source': str, 'column_name': str, 'row_id': int, 'regex': str}

    `df_metric` schema :
        {'source': str, 'column_name': str, 'format_percent': float, 'timestamp': datetime}
    """
    n = df.height

    df_bitmap = df.select(
        pl.int_range(0, pl.len(), dtype=pl.Int64).alias("row_id"),
        *(
            pl.col(c).cast(pl.String).str.contains(regex).not_().alias(c)
            for c in columns
        ),
    )

    df_rids = _get_df_rids(df_bitmap, columns, source).with_columns(
        pl.lit(regex).alias("regex")
    )

    df_metric = (
        df_bitmap.select([pl.col(c).sum().alias(c) for c in columns])
        .unpivot(on=columns, variable_name="column_name", value_name="fail_count")
        .select(
            pl.lit(source).alias("source"),
            "column_name",
            (pl.lit(1.0) - pl.col("fail_count") / n).alias("format_percent"),
            pl.lit(timestamp).alias("timestamp"),
        )
    )

    return df_rids, df_metric


def label_set(
    df: DataFrame,
    columns: list[str],
    labels: list[str] | list[float] | list[int],
    source: str,
    timestamp: datetime,
) -> tuple[DataFrame, DataFrame]:
    """
    Return (`df_rids`, `df_metric`) :

    `df_rids` schema :
        {'source': str, 'column_name': str, 'row_id': int}

    `df_metric` schema :
        {'source': str, 'column_name': str, 'labels_percent': float, 'timestamp': datetime}
    """
    n = df.height

    df_bitmap = df.select(
        pl.int_range(0, pl.len(), dtype=pl.Int64).alias("row_id"),
        *(pl.col(c).is_in(labels).not_().alias(c) for c in columns),
    )

    df_rids = _get_df_rids(df_bitmap, columns, source)

    df_metric = (
        df_bitmap.select([pl.col(c).sum().alias(c) for c in columns])
        .unpivot(on=columns, variable_name="column_name", value_name="fail_count")
        .select(
            pl.lit(source).alias("source"),
            "column_name",
            (pl.lit(1.0) - pl.col("fail_count") / n).alias("labels_percent"),
            pl.lit(timestamp).alias("timestamp"),
        )
    )

    return df_rids, df_metric


def interval_validity(
    df: DataFrame, columns: list[tuple[str, str, str]], source: str, timestamp: datetime
) -> tuple[DataFrame, DataFrame]:
    """
    Return (`df_rids`, `df_metric`) :

    `df_rids` schema :
        {'source': str, 'column_name': str, 'row_id': int}
        where 'column_name' is the triplet of columns names separated by a '|'

    `df_metric` schema :
        {
            'source': str, 'column_name_min': str, 'column_target': str,
            'column_name_max': str, 'interval_percent': float, 'timestamp': datetime
        }
    """
    n = df.height
    triple_names = [f"{c0}|{c1}|{c2}" for c0, c1, c2 in columns]

    def _cast_expr(c: str) -> pl.Expr:
        return pl.col(c).cast(pl.String).str.replace_all(r"\s+", "").cast(pl.Float64)

    df_bitmap = df.select(
        pl.int_range(0, pl.len()).cast(pl.Int64).alias("row_id"),
        *(
            (
                ~(
                    (_cast_expr(c0) <= _cast_expr(c1))
                    & (_cast_expr(c1) <= _cast_expr(c2))
                )
            ).alias(tname)
            for (c0, c1, c2), tname in zip(columns, triple_names)
        ),
    )

    df_rids = _get_df_rids(df_bitmap, triple_names, source)

    df_meta = pl.DataFrame(
        {
            "column_name": triple_names,
            "column_name_min": [c0 for c0, _, _ in columns],
            "column_name_target": [c1 for _, c1, _ in columns],
            "column_name_max": [c2 for _, _, c2 in columns],
        }
    )

    df_metric = (
        df_bitmap.select([pl.col(tname).sum().alias(tname) for tname in triple_names])
        .unpivot(variable_name="column_name", value_name="count")
        .join(df_meta, on="column_name", how="left")
        .select(
            pl.lit(source).alias("source"),
            "column_name_min",
            "column_name_target",
            "column_name_max",
            (1.0 - pl.col("count") / n).alias("interval_percent"),
            pl.lit(timestamp).alias("timestamp"),
        )
    )

    return df_rids, df_metric


def foreign_key_validity(
    df1: DataFrame,
    df2: DataFrame,
    columns: list[tuple[str, str]],
    sources: tuple[str, str],
    timestamp: datetime,
) -> DataFrame:
    """
    We check the percent of values for the `columns` pairs, in the `df1` that are included in the `df2`.
    Note that `columns` is a list of pair of columns, the first one refer to a coluln of `df1` (and the second one to `df2`).
    """

    n = df1.height
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
                pl.lit(col1).alias("column_name_1"),
                pl.lit(col2).alias("column_name_2"),
                (pl.len() / n).alias("fk_percent"),
            )
            for col1, col2 in columns
        ]
    )

    return result.with_columns(pl.lit(timestamp).alias("timestamp"))


def fuzzy_foreign_key_validity(
    df1: DataFrame,
    df2: DataFrame,
    columns: list[tuple[str, str]],
    sources: tuple[str, str],
    timestamp: datetime,
) -> DataFrame:
    """
    Same output as `foreign_key_validity`, but a value of `df1` is valid if it
    matches a value of `df2` with `fuzzy_join` (ex: "Cadre" -> "Cadres et professions ...").
    """

    n = df1.height
    src1, src2 = sources

    result = DataFrame(
        [
            {
                "source_1": src1,
                "source_2": src2,
                "column_name_1": col1,
                "column_name_2": col2,
                "fk_percent": fuzzy_join(
                    df1.select(col1), df2.select(col2), col1, col2, how="inner"
                ).height
                / n,
            }
            for col1, col2 in columns
        ]
    )

    return result.with_columns(pl.lit(timestamp).alias("timestamp"))


def expand_foreign_key_validity(
    df1: DataFrame,
    df2: DataFrame,
    column: str,
    extractions: Sequence[tuple[str, str]],
    replace_patterns: Sequence[tuple[str, str]],
    keys: list[str],
    sources: tuple[str, str],
    timestamp: datetime,
) -> DataFrame:
    """
    The function use `expand_column(df1, column, extractions, replace_patterns)` before joining the result to `df2`
     and infer `dtypes` from `df2`.
    !! We assume here that `df1` don't have columns with the names `keys` (if it is they will be overwritten),
    `df2` need to contains the `keys` columns.
    The address `column` of `df1` is split with `expand_column` (ex: N, Nom_Rue, Code_Postal),
    then we check the percent of rows whose `keys` all exist together in `df2`.
    """

    n = df1.height
    src1, src2 = sources

    df1_expanded = expand_column(
        df1,
        column,
        extractions,
        replace_patterns,
        dtypes={key: df2.schema[key] for key in keys},
    )
    matched = df1_expanded.join(
        df2.select(keys).unique(), on=keys, how="semi", nulls_equal=False
    ).height

    return DataFrame(
        {
            "source_1": [src1],
            "source_2": [src2],
            "column_name_1": [column],
            "column_name_2": [", ".join(keys)],
            "fk_percent": [matched / n],
            "timestamp": [timestamp],
        }
    )

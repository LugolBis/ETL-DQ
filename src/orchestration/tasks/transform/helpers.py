import polars as pl
from polars import DataFrame


def fuzzy_join(
    df1: DataFrame, df2: DataFrame, column_1: str, column_2: str
) -> DataFrame:
    id1, id2 = "__id1", "__id2"
    tag, ref = "__tag", "__ref"

    # We apply : lowercase & strip_chars to the `column_1` before split them by '/' to get a list of tags
    left = (
        df1.with_row_index(id1)
        .select(
            id1,
            pl.col(column_1)
            .str.to_lowercase()
            .str.split("/")
            .list.eval(pl.element().str.strip_chars())
            .alias(tag),
        )
        .explode(tag)
        .filter(pl.col(tag).is_not_null() & (pl.col(tag) != ""))
    )

    right = df2.with_row_index(id2).select(
        pl.all(),
        pl.col(column_2).str.to_lowercase().str.strip_chars().alias(ref),
    )

    matches = (
        left.join_where(
            right,
            pl.col(ref).str.contains(pl.col(tag), literal=True),
        )
        .group_by(id1)
        .agg(pl.col(id2).min())
        .join(right, on=id2, how="left")
    )

    result = df1.with_row_index(id1).join(matches, on=id1, how="left").drop(id1)

    print(f"Columns : {result.shape}")

    return result

import re
from collections.abc import Mapping, Sequence
from typing import Literal

import polars as pl
from polars import DataFrame

ADRESSE_EXTRACTIONS = [
    ("N", r"^[^,]+,\s*\d{5},\s*(\d+)"),
    ("Nom_Rue", r"^[^,]+,\s*\d{5},\s*\d+\s+(\S.*?)\s*$"),
    ("Code_Postal", r"^[^,]+,\s*(\d{5}),\s*\d+"),
]


def _tags_of(df_idx: DataFrame, col: str, id_name: str, tag: str) -> DataFrame:
    # lowercase + split('/') + strip for each tag
    return (
        df_idx.select(
            id_name,
            pl.col(col)
            .str.to_lowercase()
            .str.split("/")
            .list.eval(pl.element().str.strip_chars())
            .alias(tag),
        )
        .explode(tag)
        .filter(pl.col(tag).is_not_null() & (pl.col(tag) != ""))
    )


def _refs_of(df_idx: DataFrame, col: str, id_name: str, ref: str) -> DataFrame:
    return df_idx.select(
        id_name,
        pl.col(col).str.to_lowercase().str.strip_chars().alias(ref),
    )


def fuzzy_join(
    df1: DataFrame,
    df2: DataFrame,
    column_1: str,
    column_2: str,
    included: Literal["column_1", "column_2"] = "column_1",
    how: Literal["left", "right"] = "left",
) -> DataFrame:
    """
    Args:
        included : The column whose values (split by '/') must be
            contained in the other column.
            - "column_1": each tag in column_1 must appear in column_2
            - "column_2": each tag in column_2 must appear in column_1
        how :
            - "left": all rows from df1 are retained
            - "right": all rows from df2 are retained
    """
    id1, id2 = "__id1", "__id2"
    tag, ref = "__tag", "__ref"

    df1_idx = df1.with_row_index(id1)
    df2_idx = df2.with_row_index(id2)

    if included == "column_1":
        tags = _tags_of(df1_idx, column_1, id1, tag)
        refs = _refs_of(df2_idx, column_2, id2, ref)
    elif included == "column_2":
        tags = _tags_of(df2_idx, column_2, id2, tag)
        refs = _refs_of(df1_idx, column_1, id1, ref)

    pairs = tags.join_where(
        refs,
        pl.col(ref).str.contains(pl.col(tag), literal=True),
    ).select(id1, id2)

    if how == "left":
        best = pairs.group_by(id1).agg(pl.col(id2).min())
        result = (
            df1_idx.join(best, on=id1, how="left")
            .join(df2_idx, on=id2, how="left")
            .sort(id1)
        )
    elif how == "right":
        best = df2_idx.select(id2).join(
            pairs.group_by(id2).agg(pl.col(id1).min()), on=id2, how="left"
        )
        result = (
            df1_idx.join(best, on=id1, how="right", coalesce=True)
            .join(df2_idx, on=id2, how="left")
            .sort(id2)
        )

    return result.drop(id1, id2)


def expand_column(
    df: DataFrame,
    column: str,
    extractions: Sequence[tuple[str, str]],
    replace_patterns: Sequence[tuple[str, str]] = [],
    normalize: bool = True,
    dtypes: Mapping[str, pl.DataType | type[pl.DataType]] | None = None,
) -> DataFrame:
    """
    Extracts multiple columns from a text column using regex patterns.

    Args:
        extractions: Pairs of (column_name, regex). The extracted value is the
            first capture group of the regex (or the entire match if no group exists).
        replace_patterns: Pairs of (pattern, replacement).
            Each occurence of _pattern_ is replaced by _replacement_.
        normalize: Converts extracted values to uppercase and removes accents.
        dtypes: Optional type casts by column name, e.g. {"N": pl.Int64}.
    """
    aliases = [alias for alias, _ in extractions]
    if len(set(aliases)) != len(aliases):
        raise ValueError(f"Noms de colonnes en double dans `extractions` : {aliases}")

    unknown = set(dtypes or {}) - set(aliases)
    if unknown:
        raise ValueError(
            f"`dtypes` référence des colonnes inconnues : {sorted(unknown)}"
        )

    base = pl.col(column)

    for pattern, replacement in replace_patterns:
        value = replacement
        if re.compile(pattern).groups > 0:
            value = value.replace("$", "$$")
        base = base.str.replace_all(pattern, value)

    dtypes = dict(dtypes or {})
    exprs = []
    for alias, regex in extractions:
        expr = base.str.extract(regex)
        if normalize:
            expr = (
                expr.str.to_uppercase()
                .str.normalize("NFKD")
                .str.replace_all(r"[\u0300-\u036f]", "")
            )
        if alias in dtypes:
            expr = expr.cast(dtypes[alias])
        exprs.append(expr.alias(alias))

    result = df.with_columns(exprs)

    return result

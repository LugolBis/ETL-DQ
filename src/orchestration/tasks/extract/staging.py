from collections.abc import Callable

import polars as pl
from polars import DataFrame

from mappings import SourceRelation
from orchestration.tasks.dtypes import polars_dtype


def needs_staging(relation: SourceRelation) -> bool:
    """True if extracting the relation is more than a plain copy to parquet."""
    return bool(relation.rename_map or relation.casts)


def staging_func(relation: SourceRelation) -> Callable[[DataFrame], DataFrame]:
    """Extraction rules of a source relation, as declared in the mappings.

    Per column: `cleanup` (regex substitutions on text) -> `cast` -> rename.
    Generalises `cast_columns` (one type, one pattern) to per-column specs.
    """

    def inner(df: DataFrame) -> DataFrame:
        expressions = []
        for column in relation.casts:
            expr = pl.col(column.raw_name)

            if df.schema[column.raw_name] == pl.String:
                for pattern, replacement in column.cleanup:
                    expr = expr.str.replace_all(pattern, replacement)

            expressions.append(
                expr.cast(polars_dtype(column.dtype)).alias(column.raw_name)
            )

        if expressions:
            df = df.with_columns(expressions)

        return df.rename(relation.rename_map) if relation.rename_map else df

    return inner

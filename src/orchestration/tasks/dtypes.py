import polars as pl

from mappings import DataType

_POLARS_DTYPES: dict[DataType, type[pl.DataType]] = {
    DataType.BIGINT: pl.Int64,
    DataType.DOUBLE: pl.Float64,
    DataType.VARCHAR: pl.String,
}


def polars_dtype(dtype: DataType) -> type[pl.DataType]:
    """Polars type matching a logical type of the mappings."""
    return _POLARS_DTYPES[dtype]

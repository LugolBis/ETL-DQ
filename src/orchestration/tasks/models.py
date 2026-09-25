from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

import polars as pl

from .enums import FileType

# Readers consts
_ALLOWED_KWARGS: dict[FileType, frozenset[str]] = {
    FileType.CSV: frozenset(
        {
            "has_header",
            "separator",
            "quote_char",
            "encoding",
            "skip_rows",
            "n_rows",
            "try_parse_dates",
            "columns",
            "null_values",
            "infer_schema_length",
        }
    ),
    FileType.PARQUET: frozenset({"n_rows", "columns"}),
    FileType.JSON: frozenset({"infer_schema_length"}),
    FileType.NDJSON: frozenset({"infer_schema_length"}),
    FileType.EXCEL: frozenset(),
    FileType.AVRO: frozenset(),
    FileType.IPC: frozenset({"n_rows", "columns"}),
}

_READERS: dict[FileType, Callable[..., pl.DataFrame]] = {
    FileType.CSV: pl.read_csv,
    FileType.PARQUET: pl.read_parquet,
    FileType.JSON: pl.read_json,
    FileType.NDJSON: pl.read_ndjson,
    FileType.EXCEL: pl.read_excel,
    FileType.AVRO: pl.read_avro,
    FileType.IPC: pl.read_ipc,
}

# Writers consts
_ALLOWED_WRITER_KWARGS: dict[FileType, frozenset[str]] = {
    FileType.CSV: frozenset(
        {
            "include_header",
            "separator",
            "quote_char",
            "null_value",
        }
    ),
    FileType.PARQUET: frozenset(
        {
            "compression",
            "row_group_size",
        }
    ),
    FileType.JSON: frozenset(),
    FileType.NDJSON: frozenset(),
    FileType.EXCEL: frozenset(),
    FileType.AVRO: frozenset({"compression"}),
    FileType.IPC: frozenset({"compression"}),
}
_WRITER_METHODS: dict[FileType, str] = {
    FileType.CSV: "write_csv",
    FileType.PARQUET: "write_parquet",
    FileType.JSON: "write_json",
    FileType.NDJSON: "write_ndjson",
    FileType.EXCEL: "write_excel",
    FileType.AVRO: "write_avro",
    FileType.IPC: "write_ipc",
}


@dataclass(slots=True, eq=False, frozen=True)
class DfReader:
    """
    Class for Polars DataFrame readers such as `pl.read_csv()`, etc.
    """

    path: Path
    file_type: FileType

    # CSV / Excel
    has_header: bool | None = None
    separator: str = ","
    quote_char: str = '"'
    encoding: str = "utf8"
    skip_rows: int = 0
    try_parse_dates: bool = False

    # Shared
    n_rows: int | None = None
    columns: Sequence[str] | None = None
    null_values: Sequence[str] | None = None
    infer_schema_length: int | None = 100

    def _kwargs(self) -> dict:
        data = asdict(self)
        data.pop("path", None)
        data.pop("file_type", None)

        allowed = _ALLOWED_KWARGS[self.file_type]
        return {k: v for k, v in data.items() if k in allowed and v is not None}

    def read(self) -> pl.DataFrame:
        reader = _READERS.get(self.file_type)
        if reader is None:
            raise NotImplementedError(
                f"DfReader.read() isn't supported for {self.file_type}"
            )
        return reader(self.path, **self._kwargs())


@dataclass(slots=True, eq=False, frozen=True)
class DfWriter:
    """
    Class for Polars DataFrame writers such as `df.write_csv()`, etc.
    """

    path: Path
    file_type: FileType

    # CSV / Excel
    include_header: bool | None = None
    separator: str = ","
    quote_char: str = '"'
    null_value: str | None = None

    # Parquet / IPC / Avro
    compression: str | None = None
    row_group_size: int | None = None

    def _kwargs(self) -> dict:
        data = asdict(self)
        data.pop("path", None)
        data.pop("file_type", None)

        allowed = _ALLOWED_WRITER_KWARGS[self.file_type]
        return {k: v for k, v in data.items() if k in allowed and v is not None}

    def write(self, df: pl.DataFrame) -> None:
        method_name = _WRITER_METHODS.get(self.file_type)
        if method_name is None:
            raise NotImplementedError(f"write() non supporté pour {self.file_type}")

        method = getattr(df, method_name)
        method(self.path, **self._kwargs())

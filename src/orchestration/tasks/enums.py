from __future__ import annotations

from enum import Enum


class FileType(Enum):
    CSV = "csv"
    PARQUET = "parquet"
    JSON = "json"
    NDJSON = "ndjson"
    EXCEL = "excel"
    AVRO = "avro"
    IPC = "ipc"

    @classmethod
    def _missing_(cls, value: object) -> FileType | None:
        if not isinstance(value, str):
            return None

        key = value.lower().lstrip(".")

        for member in cls:
            if member.value == key:
                return member

        aliases: dict[str, FileType] = {
            "pq": cls.PARQUET,
            "jsonl": cls.NDJSON,
            "xlsx": cls.EXCEL,
            "xls": cls.EXCEL,
            "feather": cls.IPC,
        }
        return aliases.get(key)

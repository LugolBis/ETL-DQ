from datetime import datetime
from pathlib import Path

import polars as pl
from airflow.sdk import task

from metrics.completude import compute_completude
from orchestration.tasks.enums import FileType
from orchestration.tasks.models import DfReader, DfWriter


@task()
def completude_assessment(
    cfg: dict[str, list[str]], data_dir: Path, df_w: DfWriter, timestamp: str
) -> None:
    timestamp_dt = datetime.fromisoformat(timestamp)

    dfs = [
        compute_completude(
            DfReader(data_dir / f"{name}.parquet", FileType.PARQUET).read(),
            columns,
            name,
            timestamp_dt,
        )
        for name, columns in cfg.items()
    ]

    df_union = pl.concat(dfs, how="vertical")

    df_w.update(df_union)

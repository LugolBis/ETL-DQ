from pathlib import Path

import polars as pl
from airflow.sdk import task

from metrics.completude import compute_completude
from orchestration.tasks.enums import FileType
from orchestration.tasks.models import DfReader, DfWriter


@task()
def completude_assessment(
    cfg: dict[str, list[str]],
    data_dir: Path,
    df_w: DfWriter,
) -> None:
    dfs = [
        compute_completude(
            DfReader(data_dir / f"{name}.parquet", FileType.PARQUET).read(),
            columns,
            name,
        )
        for name, columns in cfg.items()
    ]

    df_union = pl.concat(dfs, how="vertical")

    df_w.write(df_union)

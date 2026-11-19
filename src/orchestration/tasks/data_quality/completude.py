from datetime import datetime
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
    df_wrids: DfWriter,
    df_wmetric: DfWriter,
    timestamp: str,
) -> None:
    timestamp_dt = datetime.fromisoformat(timestamp)
    dfs_metric = []

    for name, columns in cfg.items():
        df_rids, df_metric = compute_completude(
            DfReader(data_dir / f"{name}.parquet", FileType.PARQUET).read(),
            columns,
            name,
            timestamp_dt,
        )

        df_wrids.override(
            delta_write_options={"predicate": f"source = '{name}'"}
        ).write(df_rids)

        dfs_metric.append(df_metric)

    df_wmetric.write(pl.concat(dfs_metric, how="vertical"))

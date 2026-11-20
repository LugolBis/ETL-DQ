from datetime import datetime
from pathlib import Path

import polars as pl
from airflow.sdk import task

from metrics.unicite import compute_unicite
from orchestration.tasks.enums import FileType
from orchestration.tasks.models import DfReader, DfWriter


@task()
def unicite_assessment(
    cfg: list[tuple[list[str], list[str]]],
    data_dir: Path,
    df_wrids: DfWriter,
    df_wmetric: DfWriter,
    timestamp: str,
) -> None:
    timestamp_dt = datetime.fromisoformat(timestamp)
    dfs_metric = []

    for names, columns in cfg:
        source = "+".join(names)

        df_rids, df_metric = compute_unicite(
            pl.concat(
                [
                    DfReader(
                        data_dir / f"{name}.parquet", FileType.PARQUET, columns=columns
                    ).read()
                    for name in names
                ],
            ),
            columns,
            source,
            timestamp_dt,
        )

        df_wrids.override(
            delta_write_options={"predicate": f"source = '{source}'"}
        ).write(df_rids)

        dfs_metric.append(df_metric)

    df_wmetric.update(pl.concat(dfs_metric, how="vertical"))

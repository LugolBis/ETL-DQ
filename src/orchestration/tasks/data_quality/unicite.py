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
    df_w: DfWriter,
    timestamp: str,
) -> None:
    timestamp_dt = datetime.fromisoformat(timestamp)

    dfs = [
        compute_unicite(
            pl.concat(
                [
                    DfReader(
                        data_dir / f"{name}.parquet", FileType.PARQUET, columns=columns
                    ).read()
                    for name in names
                ],
            ),
            columns,
            "+".join(names),
            timestamp_dt,
        )
        for names, columns in cfg
    ]

    df_union = pl.concat(dfs, how="vertical")

    df_w.update(df_union)

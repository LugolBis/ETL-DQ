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
) -> None:
    dfs = [
        compute_unicite(
            pl.concat(
                [
                    DfReader(data_dir / f"{name}.parquet", FileType.PARQUET).read()
                    for name in names
                ],
            ),
            columns,
            "+".join(names),
        )
        for names, columns in cfg
    ]

    df_union = pl.concat(dfs, how="vertical")

    df_w.update(df_union)
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl
from airflow.sdk import task

from metrics.coherence import analyze_grouped_distribution
from orchestration.tasks.enums import FileType
from orchestration.tasks.models import DfReader, DfWriter


@task()
def coherence_assessment(
    cfg: list[tuple[str, float]],
    data_dir: Path,
    df_w: DfWriter,
) -> None:
    dfs = [
        analyze_grouped_distribution(
            DfReader(data_dir / f"{name}.parquet", FileType.PARQUET).read(), seuil
        )
        .filter((pl.col("M1") > 0.50) & ((pl.col("M2") > 5) | (pl.col("M3") > 10)))
        .with_columns(
            pl.lit(datetime.now(ZoneInfo("UTC"))).alias("timestamp"),
            pl.lit(name).alias("source"),
        )
        for name, seuil in cfg
    ]

    df_union = pl.concat(dfs, how="vertical")

    df_w.write(df_union)

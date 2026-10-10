from pathlib import Path

import polars as pl
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import task, task_group

from metrics.conformite import (
    foreign_key_validity,
    interval_validity,
    label_set,
    text_format,
)
from orchestration.tasks.enums import FileType
from orchestration.tasks.models import DfReader, DfWriter

type CONFORMITE_FMT_TYPE = dict[str, list[tuple[list[str], str]]]
type CONFORMITE_LABEL_TYPE = dict[str, tuple[list[str], list[str | int | float]]]
type CONFORMITE_INTERVAL_TYPE = dict[str, list[tuple[str, str, str]]]
type CONFORMITE_FK_TYPE = dict[tuple[str, str], list[tuple[str, str]]]


@task()
def conformite_format_assessment(
    cfg: CONFORMITE_FMT_TYPE, data_dir: Path, df_w: DfWriter
) -> None:
    dfs = [
        text_format(
            DfReader(data_dir / f"{name}.parquet", FileType.PARQUET).read(),
            columns,
            regex,
            name,
        )
        for name, patterns in cfg.items()
        for (columns, regex) in patterns
    ]

    df_union = pl.concat(dfs, how="vertical")

    df_w.update(df_union)


@task()
def conformite_label_assessment(
    cfg: CONFORMITE_LABEL_TYPE, data_dir: Path, df_w: DfWriter
) -> None:
    dfs = [
        label_set(
            DfReader(data_dir / f"{name}.parquet", FileType.PARQUET).read(),
            columns,
            labels,
            name,
        )
        for name, (columns, labels) in cfg.items()
    ]

    df_union = pl.concat(dfs, how="vertical")

    df_w.update(df_union)


@task()
def conformite_interval_assessment(
    cfg: CONFORMITE_INTERVAL_TYPE, data_dir: Path, df_w: DfWriter
) -> None:
    dfs = [
        interval_validity(
            DfReader(data_dir / f"{name}.parquet", FileType.PARQUET).read(),
            columns,
            name,
        )
        for name, columns in cfg.items()
    ]

    df_union = pl.concat(dfs, how="vertical")

    df_w.update(df_union)


@task()
def conformite_foreign_key_assessment(
    cfg: CONFORMITE_FK_TYPE, data_dir: Path, df_w: DfWriter
) -> None:
    dfs = [
        foreign_key_validity(
            DfReader(data_dir / f"{name_1}.parquet", FileType.PARQUET).read(),
            DfReader(data_dir / f"{name_2}.parquet", FileType.PARQUET).read(),
            columns,
            (name_1, name_2),
        )
        for (name_1, name_2), columns in cfg.items()
    ]

    df_union = pl.concat(dfs, how="vertical")

    df_w.update(df_union)


@task_group()
def conformite_assessment(
    cfg: tuple[
        CONFORMITE_FMT_TYPE,
        CONFORMITE_LABEL_TYPE,
        CONFORMITE_INTERVAL_TYPE,
        CONFORMITE_FK_TYPE,
    ],
    data_dir: Path,
    df_ws: tuple[DfWriter, DfWriter, DfWriter, DfWriter],
) -> None:
    """The order of the `cfg` is important, it will be used to map each `cfg` with a `DfWriter` in `df_ws`."""

    start = EmptyOperator(task_id="start_conformite_assessment")
    end = EmptyOperator(task_id="end_conformite_assessment")

    task_fmt = conformite_format_assessment(cfg[0], data_dir, df_ws[0])
    task_label = conformite_label_assessment(cfg[1], data_dir, df_ws[1])
    task_interval = conformite_interval_assessment(cfg[2], data_dir, df_ws[2])
    task_fk = conformite_foreign_key_assessment(cfg[3], data_dir, df_ws[3])

    start >> [task_fmt, task_label, task_interval, task_fk] >> end

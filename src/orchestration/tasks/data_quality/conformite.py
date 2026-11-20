from collections.abc import Callable
from datetime import datetime
from pathlib import Path

import polars as pl
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import task, task_group
from polars import DataFrame

from metrics.conformite import (
    expand_foreign_key_validity,
    foreign_key_validity,
    fuzzy_foreign_key_validity,
    interval_validity,
    label_set,
    text_format,
)
from orchestration.tasks.enums import FileType
from orchestration.tasks.models import DfReader, DfWriter

type CONFORMITE_FMT_TYPE = dict[str, list[tuple[list[str], str]]]
type CONFORMITE_LABEL_TYPE = dict[
    str,
    tuple[list[str], list[float]]
    | tuple[list[str], list[int]]
    | tuple[list[str], list[str]],
]
type CONFORMITE_INTERVAL_TYPE = dict[str, list[tuple[str, str, str]]]
type CONFORMITE_FK_TYPE = dict[tuple[str, str], list[tuple[str, str]]]
# (address column, extractions, replace_patterns, keys)
type CONFORMITE_FK_ADRESSE_TYPE = dict[
    tuple[str, str], tuple[str, list[tuple[str, str]], list[tuple[str, str]], list[str]]
]
type CONFORMITE_FK_ALL_TYPE = tuple[
    CONFORMITE_FK_TYPE, CONFORMITE_FK_TYPE, CONFORMITE_FK_ADRESSE_TYPE
]

type INPUT_DFWRITERS = tuple[DfWriter, DfWriter]


@task()
def conformite_format_assessment(
    cfg: CONFORMITE_FMT_TYPE,
    data_dir: Path,
    df_wrids: DfWriter,
    df_wmetric: DfWriter,
    timestamp: str,
) -> None:
    timestamp_dt = datetime.fromisoformat(timestamp)
    dfs_metric = []

    for name, patterns in cfg.items():
        dfs_rids = []

        for columns, regex in patterns:
            df_rids, df_metric = text_format(
                DfReader(data_dir / f"{name}.parquet", FileType.PARQUET).read(),
                columns,
                regex,
                name,
                timestamp_dt,
            )

            dfs_rids.append(df_rids)
            dfs_metric.append(df_metric)

        df_wrids.override(
            delta_write_options={"predicate": f"source = '{name}'"}
        ).write(pl.concat(dfs_rids, how="vertical"))

    df_wmetric.update(pl.concat(dfs_metric, how="vertical"))


@task()
def conformite_label_assessment(
    cfg: CONFORMITE_LABEL_TYPE,
    data_dir: Path,
    df_wrids: DfWriter,
    df_wmetric: DfWriter,
    timestamp: str,
) -> None:
    timestamp_dt = datetime.fromisoformat(timestamp)
    dfs_metric = []

    for name, (columns, labels) in cfg.items():
        df_rids, df_metric = label_set(
            DfReader(data_dir / f"{name}.parquet", FileType.PARQUET).read(),
            columns,
            labels,
            name,
            timestamp_dt,
        )

        df_wrids.override(
            delta_write_options={"predicate": f"source = '{name}'"}
        ).write(df_rids)

        dfs_metric.append(df_metric)

    df_wmetric.update(pl.concat(dfs_metric, how="vertical"))


@task()
def conformite_interval_assessment(
    cfg: CONFORMITE_INTERVAL_TYPE,
    data_dir: Path,
    df_wrids: DfWriter,
    df_wmetric: DfWriter,
    timestamp: str,
) -> None:
    timestamp_dt = datetime.fromisoformat(timestamp)
    dfs_metric = []

    for name, columns in cfg.items():
        df_rids, df_metric = interval_validity(
            DfReader(data_dir / f"{name}.parquet", FileType.PARQUET).read(),
            columns,
            name,
            timestamp_dt,
        )

        df_wrids.override(
            delta_write_options={"predicate": f"source = '{name}'"}
        ).write(df_rids)

        dfs_metric.append(df_metric)

    df_wmetric.update(pl.concat(dfs_metric, how="vertical"))


@task()
def conformite_foreign_key_assessment(
    cfg: CONFORMITE_FK_ALL_TYPE,
    data_dir: Path,
    df_wrids: DfWriter,
    df_wmetric: DfWriter,
    timestamp: str,
) -> None:
    """
    `cfg` = (exact FK, fuzzy FK, address FK)
    """

    timestamp_dt = datetime.fromisoformat(timestamp)
    map_dfs_rids = {}
    dfs_metric = []

    cfg_exact, cfg_fuzzy, cfg_adresse = cfg
    read_pqt: Callable[[str], DataFrame] = lambda file_name: DfReader(
        data_dir / f"{file_name}.parquet", FileType.PARQUET
    ).read()

    for fn, cfg_fk in (
        (foreign_key_validity, cfg_exact),
        (fuzzy_foreign_key_validity, cfg_fuzzy),
    ):
        for (name_1, name_2), columns in cfg_fk.items():
            df_rids, df_metric = fn(
                read_pqt(name_1),
                read_pqt(name_2),
                columns,
                (name_1, name_2),
                timestamp_dt,
            )

            map_dfs_rids[name_1, name_2] = map_dfs_rids.get(
                (name_1, name_2), []
            ).append(df_rids)

            dfs_metric.append(df_metric)

    for (name_1, name_2), (
        column,
        extractions,
        replace_patterns,
        keys,
    ) in cfg_adresse.items():
        df_rids, df_metric = expand_foreign_key_validity(
            read_pqt(name_1),
            read_pqt(name_2),
            column,
            extractions,
            replace_patterns,
            keys,
            (name_1, name_2),
            timestamp_dt,
        )

        map_dfs_rids[name_1, name_2] = map_dfs_rids.get((name_1, name_2), []).append(
            df_rids
        )

        dfs_metric.append(df_metric)

    for (name_1, name_2), dfs_rids in map_dfs_rids.items():
        df_wmetric.override(
            delta_write_options={
                "predicate": f"source_1 = '{name_1}' and source_2 = '{name_2}'"
            }
        ).write(pl.concat(dfs_rids, how="vertical"))

    df_wmetric.update(pl.concat(dfs_metric, how="vertical"))


@task_group()
def conformite_assessment(
    cfg: tuple[
        CONFORMITE_FMT_TYPE,
        CONFORMITE_LABEL_TYPE,
        CONFORMITE_INTERVAL_TYPE,
        CONFORMITE_FK_ALL_TYPE,
    ],
    data_dir: Path,
    df_ws: tuple[INPUT_DFWRITERS, INPUT_DFWRITERS, INPUT_DFWRITERS, INPUT_DFWRITERS],
    timestamp: str,
) -> None:
    """The order of the `cfg` is important, it will be used to map each `cfg` with a `DfWriter` in `df_ws`."""

    start = EmptyOperator(task_id="start_conformite_assessment")
    end = EmptyOperator(task_id="end_conformite_assessment")

    task_fmt = conformite_format_assessment(cfg[0], data_dir, *df_ws[0], timestamp)
    task_label = conformite_label_assessment(cfg[1], data_dir, *df_ws[1], timestamp)
    task_interval = conformite_interval_assessment(
        cfg[2], data_dir, *df_ws[2], timestamp
    )
    task_fk = conformite_foreign_key_assessment(cfg[3], data_dir, *df_ws[3], timestamp)

    start >> [task_fmt, task_label, task_interval, task_fk] >> end

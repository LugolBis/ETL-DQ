from collections.abc import Callable

import pendulum
import polars as pl
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag
from polars import DataFrame

from mappings import LinkKind
from orchestration.dags.cfg.mapping_config import MAPPINGS, extract_deps
from orchestration.tasks.enums import FileType
from orchestration.tasks.extract.system import ensure_extracted
from orchestration.tasks.load.system import display
from orchestration.tasks.models import DfReader, DfWriter
from orchestration.tasks.transform.core import apply_dfs
from orchestration.tasks.transform.mapping_helpers import aggregation_sql

TARGET = "Consommation_IRIS_Paris"

# GROUP BY keys and AVG(NB_KW_Jour * 365) come from the views of the mappings
SQL_QUERY_STATS = aggregation_sql(MAPPINGS, TARGET)


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["Consommation", "IRIS"],
)
def consommation_iris_paris():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # (extract dag_id, staged files) for every source feeding the target
    sources_deps = extract_deps(TARGET)

    # (Nom_Rue, Code_Postal) of the consumption = (ID_Rue, ID_Ville) of the IRIS
    conso1_iris4 = MAPPINGS.link("conso1_iris4", LinkKind.EQUALS)

    df_readers = [
        DfReader(path, FileType.PARQUET) for _, paths in sources_deps for path in paths
    ]

    # /!\ WARNING : We assume here that invalid 'Code_Postal' aren't mapped into valid city
    #               So the invalid data will be ignored during the join
    fn_transform: Callable[[DataFrame, DataFrame], DataFrame] = lambda df_a, df_b: (
        df_a.with_columns(
            pl.when(pl.col("Code_Postal").is_between(75000, 75999))
            .then(pl.lit("Paris"))
            .otherwise(pl.col("Code_Postal").cast(pl.String))
            .alias("Code_Postal")
        )
        .join(
            df_b,
            left_on=list(conso1_iris4.left.columns),
            right_on=list(conso1_iris4.right.columns),
            how="inner",
        )
        .sql(SQL_QUERY_STATS)
    )

    task_transform = apply_dfs.override(task_id="Join_and_Agg_ops")(
        *df_readers,
        func=fn_transform,
        df_w=DfWriter(MAPPINGS.global_path(TARGET), FileType.PARQUET),
    )

    task_display = display(DfReader(MAPPINGS.global_path(TARGET), FileType.PARQUET), 15)

    # Tasks chaining - HITL
    for source_id, file_paths in sources_deps:
        start >> ensure_extracted(source_id, file_paths) >> task_transform

    task_transform >> task_display >> end


dag_instance = consommation_iris_paris()

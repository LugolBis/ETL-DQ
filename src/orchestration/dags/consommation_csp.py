from collections.abc import Callable
from pathlib import Path

import pendulum
import polars as pl
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag
from polars import DataFrame

from orchestration.tasks.enums import FileType
from orchestration.tasks.extract.system import ensure_extracted
from orchestration.tasks.load.system import display
from orchestration.tasks.models import DfReader, DfWriter
from orchestration.tasks.transform.core import apply_dfs

DATA_DIR = Path("/opt/airflow/.data")

SQL_QUERY_STATS = """
    SELECT 
        CSP,
        Salaire_Moyen,
        AVG(NB_KW_Jour * 365) AS Conso_moyenne_annuelle
    FROM self
    GROUP BY CSP, Salaire_Moyen;
"""


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["Consommation", "CSP"],
)
def consommation_csp():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    sources_deps = (
        (
            1,
            [
                DATA_DIR / "population1.parquet",
                DATA_DIR / "consommation1.parquet",
            ],
        ),
        (
            2,
            [
                DATA_DIR / "population2.parquet",
                DATA_DIR / "consommation2.parquet",
            ],
        ),
        (3, [DATA_DIR / "csp3.parquet"]),
    )

    fn_join_pop_conso: Callable[[DataFrame, DataFrame], DataFrame] = (
        lambda df_pop, df_conso: df_pop.join(
            df_conso, left_on="Adresse", right_on="ID_Adr", how="inner"
        )
    )

    fn_union_sources: Callable[[DataFrame, DataFrame], DataFrame] = (
        lambda df_src1, df_src2: pl.concat([df_src1, df_src2], how="vertical")
    )

    fn_merged_flows: Callable[
        [DataFrame, DataFrame, DataFrame, DataFrame, DataFrame], DataFrame
    ] = lambda df_pop1, df_conso1, df_pop2, df_conso2, df_csp: (
        fn_union_sources(
            fn_join_pop_conso(df_pop1, df_conso1), fn_join_pop_conso(df_pop2, df_conso2)
        )
        .join(df_csp, left_on="CSP", right_on="ID_CSP", how="inner")
        .sql(SQL_QUERY_STATS)
    )

    df_readers = [
        DfReader(path, FileType.PARQUET) for _, paths in sources_deps for path in paths
    ]

    task_transform = apply_dfs.override(task_id="Transform_sources")(
        *df_readers,
        func=fn_merged_flows,
        df_w=DfWriter(DATA_DIR / "Consommation_CSP.parquet", FileType.PARQUET),
    )

    task_display = display(
        DfReader(DATA_DIR / "Consommation_CSP.parquet", FileType.PARQUET), 15
    )

    # Tasks chaining - HITL
    for source_id, file_paths in sources_deps:
        start >> ensure_extracted(source_id, file_paths) >> task_transform

    task_transform >> task_display >> end


dag_instance = consommation_csp()

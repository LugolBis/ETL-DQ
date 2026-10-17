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
from orchestration.tasks.transform.helpers import (
    ADRESSE_EXTRACTIONS,
    expand_column,
    fuzzy_join,
)

DATA_DIR = Path("/opt/airflow/.data")

SQL_QUERY_STATS = """
    SELECT 
        ID_CSP,
        Salaire_Moyen,
        AVG(NB_KW_Jour * 365) AS Conso_moyenne_annuelle
    FROM self
    GROUP BY ID_CSP, Salaire_Moyen;
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
            "extract_source1",
            [
                DATA_DIR / "population1.parquet",
                DATA_DIR / "consommation1.parquet",
            ],
        ),
        (
            "extract_source2",
            [
                DATA_DIR / "population2.parquet",
                DATA_DIR / "consommation2.parquet",
            ],
        ),
        ("extract_source3", [DATA_DIR / "csp3.parquet"]),
    )

    fn_join_pop_conso1: Callable[[DataFrame, DataFrame], DataFrame] = (
        lambda df_pop, df_conso: expand_column(
            df_pop,
            "Adresse",
            ADRESSE_EXTRACTIONS[:-1],
            [(r"^Paris,\s*", "Paris, 75000, "), ("(?i)rue", "")],
            dtypes={"N": pl.Int64},
        ).join(df_conso, on=["N", "Nom_Rue"], how="inner")
    )

    fn_join_pop_conso2: Callable[[DataFrame, DataFrame], DataFrame] = (
        lambda df_pop, df_conso: expand_column(
            df_pop,
            "Adresse",
            ADRESSE_EXTRACTIONS,
            dtypes={"N": pl.Int64},
        ).join(df_conso, on=["N", "Nom_Rue", "Code_Postal"], how="inner")
    )

    fn_join_pop_csp1: Callable[[DataFrame, DataFrame], DataFrame] = (
        lambda df_pop, df_csp: fuzzy_join(df_pop, df_csp, "CSP", "Desc").select(
            ["ID_CSP", "Salaire_Moyen", "NB_KW_Jour"]
        )
    )

    fn_join_pop_csp2: Callable[[DataFrame, DataFrame], DataFrame] = (
        lambda df_pop, df_csp: df_pop.join(
            df_csp, left_on="CSP", right_on="ID_CSP", how="inner"
        ).select(
            pl.col("CSP").alias("ID_CSP"), pl.col("Salaire_Moyen"), pl.col("NB_KW_Jour")
        )
    )

    fn_union_sources: Callable[[DataFrame, DataFrame], DataFrame] = (
        lambda df_src1, df_src2: pl.concat([df_src1, df_src2], how="vertical")
    )

    fn_merged_flows: Callable[
        [DataFrame, DataFrame, DataFrame, DataFrame, DataFrame], DataFrame
    ] = lambda df_pop1, df_conso1, df_pop2, df_conso2, df_csp: fn_union_sources(
        fn_join_pop_csp1(fn_join_pop_conso1(df_pop1, df_conso1), df_csp),
        fn_join_pop_csp2(fn_join_pop_conso2(df_pop2, df_conso2), df_csp),
    ).sql(SQL_QUERY_STATS)

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

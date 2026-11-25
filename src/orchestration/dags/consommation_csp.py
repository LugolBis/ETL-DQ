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
from orchestration.tasks.transform.helpers import expand_column, fuzzy_join
from orchestration.tasks.transform.mapping_helpers import (
    aggregation_sql,
    link_dtypes,
    link_extractions,
)

TARGET = "Consommation_CSP"

# GROUP BY keys and AVG(NB_KW_Jour * 365) come from the views of the mappings
SQL_QUERY_STATS = aggregation_sql(MAPPINGS, TARGET)


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["Consommation", "CSP"],
)
def consommation_csp():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # (extract dag_id, staged files) for every source feeding the target
    sources_deps = extract_deps(TARGET)

    # Joins between the staged sources, as declared in the mappings
    pop1_conso1 = MAPPINGS.link(
        "pop1_conso1", LinkKind.EXTRACTS
    )  # Adresse -> N, Nom_Rue
    csp3_pop1 = MAPPINGS.link("csp3_pop1", LinkKind.CONTAINS)  # Desc contains CSP
    pop2_conso2 = MAPPINGS.link(
        "pop2_conso2", LinkKind.EXTRACTS
    )  # Adresse -> N, Nom_Rue, Code_Postal
    csp3_pop2 = MAPPINGS.link("csp3_pop2", LinkKind.EQUALS)  # ID_CSP = CSP

    # TODO: If it break : improve the 'rue' replacement to support other voie type such as 'boulevard', etc.
    fn_join_pop_conso1: Callable[[DataFrame, DataFrame], DataFrame] = (
        lambda df_pop, df_conso: expand_column(
            df_pop,
            pop1_conso1.left.columns[0],
            link_extractions(pop1_conso1),
            [(r"^Paris,\s*", "Paris, 75000, "), ("(?i)rue", "")],
            dtypes=link_dtypes(MAPPINGS, pop1_conso1),
        ).join(df_conso, on=list(pop1_conso1.right.columns), how="inner")
    )

    fn_join_pop_conso2: Callable[[DataFrame, DataFrame], DataFrame] = (
        lambda df_pop, df_conso: expand_column(
            df_pop,
            pop2_conso2.left.columns[0],
            link_extractions(pop2_conso2),
            dtypes=link_dtypes(MAPPINGS, pop2_conso2),
        ).join(df_conso, on=list(pop2_conso2.right.columns), how="inner")
    )

    # Tags of Source1_Population.CSP (right) must appear in Source3_CSP.Desc (left)
    fn_join_pop_csp1: Callable[[DataFrame, DataFrame], DataFrame] = (
        lambda df_pop, df_csp: fuzzy_join(
            df_pop, df_csp, csp3_pop1.right.columns[0], csp3_pop1.left.columns[0]
        ).select(["ID_CSP", "Salaire_Moyen", "NB_KW_Jour"])
    )

    fn_join_pop_csp2: Callable[[DataFrame, DataFrame], DataFrame] = (
        lambda df_pop, df_csp: df_pop.join(
            df_csp,
            left_on=list(csp3_pop2.right.columns),
            right_on=list(csp3_pop2.left.columns),
            how="inner",
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
        df_w=DfWriter(MAPPINGS.global_path(TARGET), FileType.PARQUET),
    )

    task_display = display(DfReader(MAPPINGS.global_path(TARGET), FileType.PARQUET), 15)

    # Tasks chaining - HITL
    for source_id, file_paths in sources_deps:
        start >> ensure_extracted(source_id, file_paths) >> task_transform

    task_transform >> task_display >> end


dag_instance = consommation_csp()

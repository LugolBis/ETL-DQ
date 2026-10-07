from collections.abc import Callable
from pathlib import Path

import pendulum
import polars as pl
from airflow.sdk import dag
from airflow.sdk.bases.decorator import Task, XComArg
from polars import DataFrame

from orchestration.tasks.enums import FileType
from orchestration.tasks.extract.core import extract
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


def _gen_extracts(
    source: int, names: list[str]
) -> tuple[list[XComArg | Task], list[Path]]:
    tasks = []
    outputs = []

    for name in names:
        output_path = DATA_DIR / f"{name.lower()}{source}.parquet"

        outputs.append(output_path)
        tasks.append(
            extract.override(task_id=f"Extract_Source{source}_{name}")(
                DfReader(
                    DATA_DIR / f"Source{source}_{name}.csv",
                    FileType.CSV,
                    has_header=True,
                ),
                DfWriter(output_path, FileType.PARQUET),
            )
        )

    return tasks, outputs


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["Consommation", "CSP"],
)
def consommation_csp():
    # The order of `names` is important
    tasks_extract_source1, out_paths1 = _gen_extracts(1, ["Population", "Consommation"])
    tasks_extract_source2, out_paths2 = _gen_extracts(2, ["Population", "Consommation"])

    task_extract_source3 = extract.override(task_id="Extract_Source3_CSP")(
        DfReader(DATA_DIR / "Source3_CSP.csv", FileType.CSV, has_header=True),
        DfWriter(DATA_DIR / "csp3.parquet", FileType.PARQUET),
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

    task_transform = apply_dfs.override(task_id="Transform_sources")(
        *[DfReader(path, FileType.PARQUET) for path in out_paths1 + out_paths2],
        DfReader(DATA_DIR / "csp3.parquet", FileType.PARQUET),
        func=fn_merged_flows,
        df_w=DfWriter(DATA_DIR / "Consommation_CSP.parquet", FileType.PARQUET),
    )

    task_display = display(
        DfReader(DATA_DIR / "Consommation_CSP.parquet", FileType.PARQUET), 15
    )

    # Tasks chaining
    tasks_extract_source1 >> task_transform  # ty: ignore[unsupported-operator]
    tasks_extract_source2 >> task_transform  # ty: ignore[unsupported-operator]
    task_extract_source3 >> task_transform >> task_display


dag_instance = consommation_csp()

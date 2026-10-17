from pathlib import Path

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag

from orchestration.tasks.enums import FileType
from orchestration.tasks.extract.system import ensure_extracted
from orchestration.tasks.load.system import display
from orchestration.tasks.models import DfReader, DfWriter
from orchestration.tasks.transform.core import apply_dfs

DATA_DIR = Path("/opt/airflow/.data")

SQL_QUERY_STATS = """
    SELECT 
        ID_Iris,
        AVG(NB_KW_Jour * 365) AS Conso_moyenne_annuelle
    FROM self
    GROUP BY ID_Iris;
"""


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["Consommation", "IRIS"],
)
def consommation_iris_evry():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    sources_deps = (
        ("extract_source2", [DATA_DIR / "consommation2.parquet"]),
        ("extract_source4", [DATA_DIR / "iris4.parquet"]),
    )

    df_readers = [
        DfReader(path, FileType.PARQUET) for _, paths in sources_deps for path in paths
    ]

    task_transform = apply_dfs.override(task_id="Join_and_Agg_ops")(
        *df_readers,
        func=lambda df_a, df_b: df_a.join(
            df_b.rename({"ID_IRIS": "ID_Iris"}),
            left_on=["Nom_Rue", "Code_Postal"],
            right_on=["ID_Rue", "ID_Ville"],
            how="inner",
        ).sql(SQL_QUERY_STATS),
        df_w=DfWriter(DATA_DIR / "Consommation_IRIS_Evry.parquet", FileType.PARQUET),
    )

    task_display = display(
        DfReader(DATA_DIR / "Consommation_IRIS_Evry.parquet", FileType.PARQUET), 15
    )

    # Tasks chaining - HITL
    for source_id, file_paths in sources_deps:
        start >> ensure_extracted(source_id, file_paths) >> task_transform

    task_transform >> task_display >> end


dag_instance = consommation_iris_evry()

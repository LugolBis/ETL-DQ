from pathlib import Path

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag

from orchestration.tasks.enums import FileType
from orchestration.tasks.extract.core import extract
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

    task_extract_conso = extract.override(task_id="Extract_Source2_Consommation")(
        DfReader(DATA_DIR / "Source2_Consommation.csv", FileType.CSV, has_header=True),
        DfWriter(DATA_DIR / "consommation2.parquet", FileType.PARQUET),
    )

    task_extract_iris = extract.override(task_id="Extract_Source4_IRIS")(
        DfReader(DATA_DIR / "Source4_IRIS.csv", FileType.CSV, has_header=True),
        DfWriter(DATA_DIR / "iris4.parquet", FileType.PARQUET),
    )

    task_transform = apply_dfs.override(task_id="Join_and_Agg_ops")(
        DfReader(DATA_DIR / "consommation2.parquet", FileType.PARQUET),
        DfReader(DATA_DIR / "iris4.parquet", FileType.PARQUET),
        func=lambda df_a, df_b: df_a.join(
            df_b,
            left_on=["Nom_Rue", "Code_Postal"],
            right_on=["ID_Rue", "ID_Ville"],
            how="inner",
        ).sql(SQL_QUERY_STATS),
        df_w=DfWriter(DATA_DIR / "Consommation_IRIS_Evry.parquet", FileType.PARQUET),
    )

    task_display = display(
        DfReader(DATA_DIR / "Consommation_IRIS_Evry.parquet", FileType.PARQUET), 15
    )

    (
        start
        >> [task_extract_conso, task_extract_iris]
        >> task_transform
        >> task_display
        >> end
    )


dag_instance = consommation_iris_evry()

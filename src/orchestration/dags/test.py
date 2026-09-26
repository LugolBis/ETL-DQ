from pathlib import Path

import pendulum
from airflow.sdk import dag

from orchestration.tasks.enums import FileType
from orchestration.tasks.extract.core import extract
from orchestration.tasks.load.system import display
from orchestration.tasks.models import DfReader, DfWriter
from orchestration.tasks.transform.core import apply_dfs


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "airflow-v3"],
)
def airflow_test():
    task_personnes = extract(
        DfReader(
            Path("/opt/airflow/.data/Personnes.txt"), FileType.CSV, has_header=True
        ),
        DfWriter(Path("/opt/airflow/.data/personnes.parquet"), FileType.PARQUET),
    )
    task_salaires = extract(
        DfReader(
            Path("/opt/airflow/.data/Salaires.txt"), FileType.CSV, has_header=True
        ),
        DfWriter(Path("/opt/airflow/.data/salaires.parquet"), FileType.PARQUET),
    )

    task_join = apply_dfs(
        DfReader(Path("/opt/airflow/.data/personnes.parquet"), FileType.PARQUET),
        DfReader(Path("/opt/airflow/.data/salaires.parquet"), FileType.PARQUET),
        lambda df_a, df_b: df_a.join(df_b, on="id", how="inner"),
        DfWriter(Path("/opt/airflow/.data/merged.parquet"), FileType.PARQUET),
    )

    task_display = display(
        DfReader(Path("/opt/airflow/.data/merged.parquet"), FileType.PARQUET), 15
    )

    # Task chaining
    [task_personnes, task_salaires] >> task_join >> task_display


dag_instance = airflow_test()

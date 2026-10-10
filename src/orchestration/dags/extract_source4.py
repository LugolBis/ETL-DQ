from pathlib import Path

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag

from orchestration.tasks.enums import FileType
from orchestration.tasks.extract.core import extract
from orchestration.tasks.models import DfReader, DfWriter

DATA_DIR = Path("/opt/airflow/.data")


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["Source4", "IRIS"],
)
def extract_source4():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    task_extract_iris = extract.override(task_id="Extract_IRIS")(
        DfReader(DATA_DIR / "Source4_IRIS.csv", FileType.CSV, has_header=True),
        DfWriter(DATA_DIR / "iris4.parquet", FileType.PARQUET),
    )

    start >> task_extract_iris >> end


dag_instance = extract_source4()

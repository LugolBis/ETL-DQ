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
    tags=["Source2", "Consommation", "Population"],
)
def extract_source2():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    task_extract_conso = extract.override(task_id="Extract_Consommation")(
        DfReader(DATA_DIR / "Source2_Consommation.csv", FileType.CSV, has_header=True),
        DfWriter(DATA_DIR / "consommation1.parquet", FileType.PARQUET),
    )

    task_extract_pop = extract.override(task_id="Extract_Population")(
        DfReader(DATA_DIR / "Source2_Population.csv", FileType.CSV, has_header=True),
        DfWriter(DATA_DIR / "population1.parquet", FileType.PARQUET),
    )

    start >> [task_extract_conso, task_extract_pop] >> end


dag_instance = extract_source2()

from pathlib import Path

import pendulum
import polars as pl
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag

from orchestration.tasks.enums import FileType
from orchestration.tasks.models import DfReader, DfWriter
from orchestration.tasks.transform.core import apply_df
from orchestration.tasks.transform.helpers import cast_columns

DATA_DIR = Path("/opt/airflow/.data")


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["Source3", "CSP"],
)
def extract_source3():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    columns = ["Salaire_Min", "Salaire_Max"]
    target_type = pl.Int64

    task_extract_csp = apply_df.override(task_id="Extract_CSP")(
        DfReader(DATA_DIR / "Source3_CSP.csv", FileType.CSV, has_header=True),
        func=cast_columns(columns, target_type, (" ", "")),
        df_w=DfWriter(DATA_DIR / "csp3.parquet", FileType.PARQUET),
    )

    start >> task_extract_csp >> end


dag_instance = extract_source3()

from pathlib import Path

import pendulum
import polars as pl
from airflow.sdk import dag

from orchestration.tasks.enums import FileType
from orchestration.tasks.extract.core import extract
from orchestration.tasks.load.system import display
from orchestration.tasks.models import DfReader, DfWriter
from orchestration.tasks.transform.core import apply_df

DATA_DIR = Path("/opt/airflow/.data")

def filtrage_salaires_eleves(df_salaires: pl.DataFrame) -> pl.DataFrame:
    return df_salaires.filter(pl.col("salary") >= 50000).sort(
        "salary", 
        descending=True
    )

@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example","airflow-v3","sql"]
)

def salaires_eleves():
    task_extract = extract(
        DfReader(DATA_DIR / "Salaires.txt", FileType.CSV, has_header=True),
        DfWriter(DATA_DIR / "salaires_brut.parquet", FileType.PARQUET)
    )

    task_filter = apply_df(
        DfReader(DATA_DIR / "salaires_brut.parquet", FileType.PARQUET),
        filtrage_salaires_eleves,
        DfWriter(DATA_DIR / "salaires_eleves.parquet", FileType.PARQUET, include_header=True)
    )

    task_display = display(
        DfReader(DATA_DIR / "salaires_eleves.parquet", FileType.PARQUET), 15
    )

    task_extract >> task_filter >> task_display

dag_instance = salaires_eleves()
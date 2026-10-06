from pathlib import Path

import pendulum
from airflow.sdk import dag

from orchestration.tasks.enums import FileType
from orchestration.tasks.extract.core import extract
from orchestration.tasks.load.system import display
from orchestration.tasks.models import DfReader, DfWriter
from orchestration.tasks.transform.core import query

DATA_DIR = Path("/opt/airflow/.data")

SQL_QUERY_STATS = """
    SELECT 
        COUNT(*) AS nb_personnes,
        MIN(age) AS age_min,
        MAX(age) AS age_max,
        AVG(age) AS age_moyen
    FROM self
"""

@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "airflow-v3", "sql"]
)

def statistiques_ages():
    task_extract = extract(
        DfReader(DATA_DIR / "Personnes.txt", FileType.CSV, has_header=True),
        DfWriter(DATA_DIR / "personnes_brut.parquet", FileType.PARQUET)
    )

    task_query = query(
        DfReader(DATA_DIR / "personnes_brut.parquet", FileType.PARQUET), 
        SQL_QUERY_STATS,
        DfWriter(DATA_DIR / "personnes_stats.parquet", FileType.PARQUET)
    )

    task_display = display(
        DfReader(DATA_DIR / "personnes_stats.parquet", FileType.PARQUET), 15
    )

    task_extract >> task_query >> task_display

dag_instance = statistiques_ages()
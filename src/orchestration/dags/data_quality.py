from pathlib import Path

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag

from orchestration.dags.cfg.completude import COMPLETUDE_CFG
from orchestration.tasks.data_quality.completude import completude_assessment
from orchestration.tasks.enums import FileType
from orchestration.tasks.models import DfWriter

DATA_DIR = Path("/opt/airflow/.data")
DQ_DIR = DATA_DIR / "data_quality"


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["Data Quality", "Qualité de données", "DQ metrics", "DQ assessment"],
)
def data_quality_assessment():
    # For now we suppose here that sources are already extracted

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    task_completude = completude_assessment(
        COMPLETUDE_CFG,
        DATA_DIR,
        DfWriter(DQ_DIR / "completude.parquet", FileType.PARQUET),
    )

    start >> task_completude >> end


dag_instance = data_quality_assessment()

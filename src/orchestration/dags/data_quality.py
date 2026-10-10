from pathlib import Path

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag

from orchestration.dags.cfg.coherence import COHRENCE_CFG
from orchestration.dags.cfg.completude import COMPLETUDE_CFG
from orchestration.dags.cfg.conformite import (
    CONFORMITE_FK_CFG,
    CONFORMITE_FMT_CFG,
    CONFORMITE_INTERVAL_CFG,
    CONFORMITE_LABEL_CFG,
)
from orchestration.dags.cfg.unicite import UNICITE_CFG
from orchestration.tasks.data_quality.coherence import coherence_assessment
from orchestration.tasks.data_quality.completude import completude_assessment
from orchestration.tasks.data_quality.conformite import (
    conformite_assessment,
)
from orchestration.tasks.data_quality.unicite import unicite_assessment
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

    task_coherence = coherence_assessment(
        COHRENCE_CFG, DATA_DIR, DfWriter(DQ_DIR / "coherence.parquet", FileType.PARQUET)
    )

    task_grp_conformite = conformite_assessment(
        (
            CONFORMITE_FMT_CFG,
            CONFORMITE_LABEL_CFG,
            CONFORMITE_INTERVAL_CFG,
            CONFORMITE_FK_CFG,
        ),
        DATA_DIR,
        (
            DfWriter(DQ_DIR / "conformite_fmt.parquet", FileType.PARQUET),
            DfWriter(DQ_DIR / "conformite_label.parquet", FileType.PARQUET),
            DfWriter(DQ_DIR / "conformite_interval.parquet", FileType.PARQUET),
            DfWriter(DQ_DIR / "conformite_fk.parquet", FileType.PARQUET),
        ),
    )

    task_unicite = unicite_assessment(
        UNICITE_CFG, 
        DATA_DIR, DfWriter(DQ_DIR / "unicite.parquet", FileType.PARQUET),
    )

    start >> [task_completude, task_coherence, task_grp_conformite, task_unicite] >> end

dag_instance = data_quality_assessment()

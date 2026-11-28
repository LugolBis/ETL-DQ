from pathlib import Path

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag

from orchestration.dags.cfg.remediation.unicite import UNICITE_RAW_CFG
from orchestration.tasks.enums import FileType
from orchestration.tasks.load.helpers import SEPARATOR
from orchestration.tasks.models import DfReader, DfWriter
from orchestration.tasks.remediation.unicite import (
    UNICITE_REM_INPUT,
    unicite_remediation,
)

DATA_DIR = Path("/opt/airflow/.data")
DQ_DIR = DATA_DIR / "data_quality"
DQ_REM_DIR = DQ_DIR / "remediation"


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=[
        "Data Quality",
        "Qualité de données",
        "DQ remediation",
        "DQ improvement",
    ],
)
def data_quality_remediation():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    cfg_unicite: UNICITE_REM_INPUT = [
        (
            [
                DfReader(DATA_DIR / f"{source}.parquet", FileType.PARQUET)
                for source in sources
            ],
            DfReader(DQ_DIR / f"{source_err}.delta", FileType.DELTA),
            DfWriter(
                DQ_REM_DIR / f"{SEPARATOR.join(sources)}.parquet", FileType.PARQUET
            ),
            "+".join(sources),
            dedup_cfg,
        )
        for (sources, source_err, dedup_cfg) in UNICITE_RAW_CFG
    ]

    task_unicite = unicite_remediation(cfg_unicite, DATA_DIR)

    # Task chaining
    start >> task_unicite >> end


dag_instance = data_quality_remediation()

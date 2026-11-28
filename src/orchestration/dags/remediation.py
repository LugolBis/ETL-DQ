from pathlib import Path

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag

from orchestration.dags.cfg.remediation.completude import COMPLETUDE_REM_CFG
from orchestration.dags.cfg.remediation.unicite import UNICITE_RAW_CFG
from orchestration.tasks.enums import FileType
from orchestration.tasks.load.helpers import SEPARATOR
from orchestration.tasks.models import DfReader, DfWriter
from orchestration.tasks.remediation.completude import completude_remediation
from orchestration.tasks.remediation.heterogeneite import (
    heterogeneite_remediation,
)
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
                DfReader(
                    DATA_DIR / f"{source}.parquet",
                    FileType.PARQUET,
                )
                for source in sources
            ],
            DfReader(
                DQ_DIR / f"{source_err}.delta",
                FileType.DELTA,
            ),
            DfWriter(
                DATA_DIR / f"{SEPARATOR.join(sources)}.parquet",
                FileType.PARQUET,
            ),
            "+".join(sources),
            dedup_cfg,
            funcs,
        )
        for (sources, source_err, dedup_cfg, funcs) in UNICITE_RAW_CFG
    ]

    task_unicite = unicite_remediation(cfg_unicite, DATA_DIR)

    task_completude = completude_remediation(COMPLETUDE_REM_CFG, DATA_DIR)

    task_heterogeneite = heterogeneite_remediation(
        cfg_dispersion=[
            (
                DfReader(
                    DATA_DIR / "consommation1.parquet",
                    FileType.PARQUET,
                ),
                DfReader(
                    DATA_DIR / "consommation2.parquet",
                    FileType.PARQUET,
                ),
                DfWriter(
                    DATA_DIR / f"consommation1{SEPARATOR}consommation2.parquet",
                    FileType.PARQUET,
                ),
                "NB_KW_Jour",
                [],
                0.05,
            ),
        ],
        cfg_ratio=[
            (
                DfReader(
                    DATA_DIR / "csp3.parquet",
                    FileType.PARQUET,
                ),
                DfWriter(
                    DATA_DIR / "csp3.parquet",
                    FileType.PARQUET,
                ),
                "Salaire_Max",
                [1.0],
                0.5,
            ),
        ],
        autoriser_ecriture=True,
    )

    # Task Chaining
    start >> task_heterogeneite >> task_unicite >> task_completude >> end


dag_instance = data_quality_remediation()

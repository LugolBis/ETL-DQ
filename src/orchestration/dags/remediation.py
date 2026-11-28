
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag

from orchestration.dags.cfg.remediation.unicite import UNICITE_RAW_CFG
from orchestration.tasks.enums import FileType
from orchestration.tasks.models import DfReader, DfWriter
from orchestration.tasks.remediation.unicite import (
    UNICITE_REM_INPUT,
    unicite_remediation,
)

# Import de la remediation de l'heterogeneite
from orchestration.tasks.remediation.heterogeneite import (
    heterogeneite_remediation,
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

    # ========================================================
    # 1. REMEDIATION DE L'UNICITE
    # Code existant de l'equipe, conserve
    # ========================================================

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
                DQ_REM_DIR / f"{'_'.join(sources)}_rem.delta",
                FileType.DELTA,
            ),
            "+".join(sources),
            dedup_cfg,
        )
        for (sources, source_err, dedup_cfg) in UNICITE_RAW_CFG
    ]

    task_unicite = unicite_remediation(cfg_unicite)

    # ========================================================
    # 2. REMEDIATION DE L'HETEROGENEITE
    # Nouvelle contribution
    # ========================================================

    task_heterogeneite = heterogeneite_remediation(
        cfg_dispersion=[
            (
                # Source 1 : consommation Paris
                DfReader(
                    DATA_DIR / "consommation1.parquet",
                    FileType.PARQUET,
                ),

                # Source 2 : consommation Evry
                DfReader(
                    DATA_DIR / "consommation2.parquet",
                    FileType.PARQUET,
                ),

                # Destination eventuelle des donnees corrigees
                DfWriter(
                    DQ_REM_DIR / "consommation1_heterogeneite_rem.parquet",
                    FileType.PARQUET,
                ),

                # Colonne concernee
                "NB_KW_Jour",

                # Facteurs connus :
                # aucun facteur valide pour l'instant
                [],

                # Seuil de proximite provisoire
                0.05,
            ),
        ],

        # Partie salaires csp3 :
        # pas encore activee
        cfg_ratio=[],

        # Mode simulation : aucun fichier n'est ecrit
        autoriser_ecriture=False,
    )

    # ========================================================
    # 3. ENCHAINEMENT DES TACHES
    # ========================================================

    # Remediation de l'unicite
    start >> task_unicite >> end

    # Remediation de l'heterogeneite
    start >> task_heterogeneite >> end


dag_instance = data_quality_remediation()

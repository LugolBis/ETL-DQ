
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

from orchestration.tasks.remediation.unicite import (
    UNICITE_REM_INPUT,
    unicite_remediation,
)

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
    # Derniere version du code de l'equipe
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

    # ========================================================
    # REMEDIATION DE LA COMPLETUDE (MICE)
    # En dernier, cf. orchestration de la documentation
    # ========================================================

    task_completude = completude_remediation(COMPLETUDE_REM_CFG, DATA_DIR)

    # ========================================================
    # 2. REMEDIATION DE L'HETEROGENEITE
    # Dispersion + ratio de position
    # ========================================================

    task_heterogeneite = heterogeneite_remediation(

        # ====================================================
        # PARTIE A : DIFFERENCE DE DISPERSION
        # ====================================================

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

                # Colonne a examiner
                "NB_KW_Jour",

                # Aucun facteur de conversion valide pour l'instant
                [],

                # Seuil de proximite provisoire
                0.05,
            ),
        ],

        # ====================================================
        # PARTIE B : RATIO DE POSITION
        # ====================================================

        cfg_ratio=[
            (
                # Source des salaires
                DfReader(
                    DATA_DIR / "csp3.parquet",
                    FileType.PARQUET,
                ),

                # Destination eventuelle des donnees corrigees
                DfWriter(
                    DQ_REM_DIR / "csp3_heterogeneite_rem.parquet",
                    FileType.PARQUET,
                ),

                # Colonne a examiner
                "Salaire_Max",

                # Facteur neutre pour tester le passage dans Airflow
                # Multiplier ou diviser par 1 ne change pas les valeurs
                [1.0],

                # Seuil : au moins 50 % des lignes atypiques
                # doivent etre ameliorables
                0.5,
            ),
        ],

        # ====================================================
        # SECURITE : MODE SIMULATION
        # ====================================================

        # Aucune ecriture de fichiers n'est autorisee
        autoriser_ecriture=False,
    )

    # ========================================================
    # 3. ENCHAINEMENT DES TACHES
    # ========================================================

    start >> task_unicite >> task_completude >> end
    start >> task_heterogeneite >> end


dag_instance = data_quality_remediation()

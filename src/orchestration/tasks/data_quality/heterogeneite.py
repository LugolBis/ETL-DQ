
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
from airflow.sdk import task

from metrics.heterogeneite import (
    calculer_cv,
    calculer_difference_dispersion,
    calculer_ratio_position,
)
from orchestration.tasks.enums import FileType
from orchestration.tasks.models import DfReader, DfWriter


@task()
def heterogeneite_assessment(
    data_dir: Path,
    df_w: DfWriter,
) -> None:
    # 1. Lire les deux sources de consommation
    df1 = DfReader(
        data_dir / "consommation1.parquet",
        FileType.PARQUET,
    ).read()

    df2 = DfReader(
        data_dir / "consommation2.parquet",
        FileType.PARQUET,
    ).read()

    # 2. Calculer les coefficients de variation
    cv1 = calculer_cv(df1)
    cv2 = calculer_cv(df2)

    # 3. Calculer la difference de dispersion
    difference = calculer_difference_dispersion(cv1, cv2)

    # 4. Lire les donnees CSP et calculer le ratio de position
    df_csp = DfReader(
        data_dir / "csp3.parquet",
        FileType.PARQUET,
    ).read()

    rp = calculer_ratio_position(df_csp)

    # 5. Afficher les metriques dans les logs Airflow
    print("========== METRIQUES D'HETEROGENEITE ==========")
    print(f"CV consommation 1 : {cv1:.6f}")
    print(f"CV consommation 2 : {cv2:.6f}")
    print(f"Difference de dispersion : {difference:.6f}")
    print(f"Ratio de position : {rp:.6f}")
    print("===============================================")

    # 6. Enregistrer les resultats
    timestamp = datetime.now(timezone.utc)

    resultats = pl.DataFrame({
        "metric": [
            "CV_consommation1",
            "CV_consommation2",
            "difference_dispersion",
            "ratio_position",
        ],
        "value": [cv1, cv2, difference, rp],
        "timestamp": [timestamp] * 4,
    })

    df_w.update(resultats)

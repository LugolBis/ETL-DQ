from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import task, task_group

from metrics.heterogeneite import (
    calculer_cv,
    calculer_difference_dispersion,
    calculer_ratio_position,
)
from orchestration.tasks.enums import FileType
from orchestration.tasks.models import DfReader, DfWriter

# Types des configurations
type HETEROGENEITE_CV_TYPE = dict[str, list[str]]

type HETEROGENEITE_DISPERSION_TYPE = list[tuple[str, str, str, str]]

type HETEROGENEITE_RATIO_TYPE = dict[str, list[tuple[str, str, str, float, float]]]


def lire_source(data_dir: Path, source: str) -> pl.DataFrame:
    """Lire un fichier Parquet a partir de son nom de source."""
    return DfReader(
        data_dir / f"{source}.parquet",
        FileType.PARQUET,
    ).read()


def creer_resultat(
    source: str,
    colonne: str,
    metrique: str,
    valeur: float,
) -> dict:
    """Creer une ligne de resultat avec le schema commun."""
    return {
        "source": source,
        "column_name": colonne,
        "metric": metrique,
        "value": valeur,
        "timestamp": datetime.now(ZoneInfo("UTC")),
    }


# 1. Difference de dispersion
@task()
def heterogeneite_dispersion_assessment(
    cfg: HETEROGENEITE_DISPERSION_TYPE,
    data_dir: Path,
    df_w: DfWriter,
) -> None:
    resultats = []
    timestamp = datetime.now(ZoneInfo("UTC"))

    for source1, colonne1, source2, colonne2 in cfg:
        df1 = lire_source(data_dir, source1)
        df2 = lire_source(data_dir, source2)

        cv1 = calculer_cv(df1, colonne1)
        cv2 = calculer_cv(df2, colonne2)

        difference = calculer_difference_dispersion(
            cv1,
            cv2,
        )

        print(f"Difference de dispersion {source1} vs {source2} : {difference:.6f}")

        resultats.append(
            {
                "source_1": source1,
                "source_2": source2,
                "column_name_1": colonne1,
                "column_name_2": colonne2,
                "difference_dispersion": difference,
                "timestamp": timestamp,
            }
        )

    df_w.update(pl.DataFrame(resultats))


# 2. Ratio de position
@task()
def heterogeneite_ratio_assessment(
    cfg: HETEROGENEITE_RATIO_TYPE,
    data_dir: Path,
    df_w: DfWriter,
) -> None:
    resultats = []
    timestamp = datetime.now(ZoneInfo("UTC"))

    for source, configurations in cfg.items():
        df = lire_source(data_dir, source)

        for (
            colonne_min,
            colonne_moyen,
            colonne_max,
            seuil_min,
            seuil_max,
        ) in configurations:
            ratio = calculer_ratio_position(
                df,
                colonne_min=colonne_min,
                colonne_moyen=colonne_moyen,
                colonne_max=colonne_max,
                seuil_min=seuil_min,
                seuil_max=seuil_max,
            )

            print(f"Ratio de position {source} : {ratio:.6f}")

            resultats.append(
                {
                    "source": source,
                    "column_name_min": colonne_min,
                    "column_name_mean": colonne_moyen,
                    "column_name_max": colonne_max,
                    "ratio_position": ratio,
                    "timestamp": timestamp,
                }
            )

    df_w.update(pl.DataFrame(resultats))


# 3. Groupe des taches Airflow
@task_group()
def heterogeneite_assessment(
    cfg: tuple[
        HETEROGENEITE_CV_TYPE,
        HETEROGENEITE_DISPERSION_TYPE,
        HETEROGENEITE_RATIO_TYPE,
    ],
    data_dir: Path,
    df_ws: tuple[DfWriter, DfWriter, DfWriter],
) -> None:
    """
    Regrouper les trois metriques d'heterogeneite.

    cfg[0] : configuration du CV
    cfg[1] : configuration de la dispersion
    cfg[2] : configuration du ratio de position

    df_ws[0] : fichier de sortie du CV
    df_ws[1] : fichier de sortie de la dispersion
    df_ws[2] : fichier de sortie du ratio
    """

    start = EmptyOperator(task_id="start_heterogeneite_assessment")

    end = EmptyOperator(task_id="end_heterogeneite_assessment")

    task_dispersion = heterogeneite_dispersion_assessment(
        cfg[1],
        data_dir,
        df_ws[1],
    )

    task_ratio = heterogeneite_ratio_assessment(
        cfg[2],
        data_dir,
        df_ws[2],
    )

    start >> [task_dispersion, task_ratio] >> end

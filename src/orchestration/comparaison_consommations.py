
from pathlib import Path
import math

import polars as pl

from orchestration.tasks.enums import FileType
from orchestration.tasks.models import DfReader


def calculer_statistiques(df: pl.DataFrame) -> pl.DataFrame:
    return df.select(
        pl.col("NB_KW_Jour").min().alias("minimum"),
        pl.col("NB_KW_Jour").max().alias("maximum"),
        pl.col("NB_KW_Jour").mean().alias("moyenne"),
        pl.col("NB_KW_Jour").median().alias("mediane"),
        pl.col("NB_KW_Jour").std().alias("ecart_type"),
    )


def compter_valeurs_atypiques(df: pl.DataFrame) -> int:
    return df.filter(
        pl.col("NB_KW_Jour") > 100
    ).height


def calculer_cv(df: pl.DataFrame) -> float:
    stats = calculer_statistiques(df).row(0, named=True)

    moyenne = stats["moyenne"]
    ecart_type = stats["ecart_type"]

    if moyenne is None or ecart_type is None or moyenne == 0:
        raise ValueError(
            "Impossible de calculer le coefficient de variation"
        )

    return ecart_type / abs(moyenne)


def calculer_difference_dispersion(
    cv_paris: float,
    cv_evry: float,
) -> float:
    if cv_paris <= 0 or cv_evry <= 0:
        raise ValueError(
            "Les coefficients de variation doivent être positifs"
        )

    return abs(math.log(cv_evry / cv_paris))


# 5. Nouvelle métrique : complétude des données
def calculer_completude(df: pl.DataFrame) -> float:
    nombre_cellules = df.height * df.width

    if nombre_cellules == 0:
        raise ValueError(
            "Impossible de calculer la complétude d'un tableau vide"
        )

    valeurs_manquantes = sum(df.null_count().row(0))

    completude = (
        (nombre_cellules - valeurs_manquantes)
        / nombre_cellules
    ) * 100

    return completude


def comparer_consommations() -> None:
    dossier = Path("/opt/airflow/.data")

    paris = DfReader(
        dossier / "Consommation_Paris.csv",
        FileType.CSV,
        has_header=True,
    ).read()

    evry = DfReader(
        dossier / "Consommation_Evry.csv",
        FileType.CSV,
        has_header=True,
    ).read()

    # 1. Statistiques descriptives
    stats_paris = calculer_statistiques(paris).row(0, named=True)
    stats_evry = calculer_statistiques(evry).row(0, named=True)

    print(f"STATISTIQUES PARIS : {stats_paris}", flush=True)
    print(f"STATISTIQUES EVRY : {stats_evry}", flush=True)

    # 2. Valeurs atypiques
    print(
        "VALEURS ATYPIQUES PARIS :",
        compter_valeurs_atypiques(paris),
        flush=True,
    )

    print(
        "VALEURS ATYPIQUES EVRY :",
        compter_valeurs_atypiques(evry),
        flush=True,
    )

    # 3. Coefficients de variation
    cv_paris = calculer_cv(paris)
    cv_evry = calculer_cv(evry)

    print(f"CV PARIS : {cv_paris:.4f}", flush=True)
    print(f"CV EVRY : {cv_evry:.4f}", flush=True)

    # 4. Différence de dispersion entre les sources
    d_cv = calculer_difference_dispersion(cv_paris, cv_evry)
    seuil = math.log(5)

    print(f"DIFFERENCE DE DISPERSION : {d_cv:.4f}", flush=True)
    print(f"SEUIL : {seuil:.4f}", flush=True)

    if d_cv <= seuil:
        print("HETEROGENEITE : seuil respecte", flush=True)
    else:
        print("HETEROGENEITE : seuil depasse", flush=True)

    # 5. Complétude des données
    completude_paris = calculer_completude(paris)
    completude_evry = calculer_completude(evry)

    print(
        f"COMPLETUDE PARIS : {completude_paris:.2f} %",
        flush=True,
    )
    print(
        f"COMPLETUDE EVRY : {completude_evry:.2f} %",
        flush=True,
    )


if __name__ == "__main__":
    comparer_consommations()

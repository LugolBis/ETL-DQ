
from pathlib import Path

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

    stats_paris = calculer_statistiques(paris).row(0, named=True)
    stats_evry = calculer_statistiques(evry).row(0, named=True)

    print(f"STATISTIQUES PARIS : {stats_paris}", flush=True)
    print(f"STATISTIQUES EVRY : {stats_evry}", flush=True)

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


if __name__ == "__main__":
    comparer_consommations()

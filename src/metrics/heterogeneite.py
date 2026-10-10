
import math

import polars as pl


def calculer_cv(
    df: pl.DataFrame,
    colonne: str = "NB_KW_Jour",
) -> float:
    if colonne not in df.columns:
        raise ValueError(f"Colonne manquante : {colonne}")

    moyenne = df[colonne].mean()
    ecart_type = df[colonne].std()

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
            "Les coefficients de variation doivent etre positifs"
        )

    return abs(math.log(cv_evry / cv_paris))


def calculer_ratio_position(
    df: pl.DataFrame,
    colonne_min: str = "Salaire_Min",
    colonne_moyen: str = "Salaire_Moyen",
    colonne_max: str = "Salaire_Max",
    seuil_min: float = 0.1,
    seuil_max: float = 0.9,
) -> float:
    if not 0 <= seuil_min < seuil_max <= 1:
        raise ValueError(
            "Les seuils doivent verifier 0 <= seuil_min < seuil_max <= 1"
        )

    colonnes = [colonne_min, colonne_moyen, colonne_max]

    if df.is_empty():
        raise ValueError("Le tableau est vide")

    if any(col not in df.columns for col in colonnes):
        raise ValueError("Colonnes de salaire manquantes")

    min_s = pl.col(colonne_min)
    moy_s = pl.col(colonne_moyen)
    max_s = pl.col(colonne_max)

    valides = (
        min_s.is_not_null()
        & moy_s.is_not_null()
        & max_s.is_not_null()
        & (min_s <= moy_s)
        & (moy_s <= max_s)
        & (min_s < max_s)
    )

    if not df.select(valides.all()).item():
        raise ValueError("Certaines lignes ont des salaires invalides")

    ratios = df.select(
        ((moy_s - min_s) / (max_s - min_s)).alias("ratio")
    )

    return ratios.select(
        (
            (pl.col("ratio") < seuil_min)
            | (pl.col("ratio") > seuil_max)
        ).mean()
    ).item()

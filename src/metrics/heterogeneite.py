
import math

import polars as pl


def calculer_cv(df: pl.DataFrame) -> float:
    moyenne = df["NB_KW_Jour"].mean()
    ecart_type = df["NB_KW_Jour"].std()

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


def calculer_ratio_position(df: pl.DataFrame) -> float:
    colonnes = ["Salaire_Min", "Salaire_Moyen", "Salaire_Max"]

    if df.is_empty():
        raise ValueError("Le tableau est vide")

    if any(col not in df.columns for col in colonnes):
        raise ValueError("Colonnes de salaire manquantes")

    min_s = pl.col("Salaire_Min")
    moy_s = pl.col("Salaire_Moyen")
    max_s = pl.col("Salaire_Max")

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
            (pl.col("ratio") < 0.1)
            | (pl.col("ratio") > 0.9)
        ).mean()
    ).item()

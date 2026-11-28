
import numpy as np
import polars as pl
from sklearn.linear_model import TheilSenRegressor


# ============================================================
# PARTIE 1 : AMELIORATION DE LA DISPERSION
# ============================================================


# 1. Calcul des quantiles
def calculer_quantiles(df: pl.DataFrame, colonne: str) -> np.ndarray:
    """Calculer les quantiles de 1 % a 99 % d'une colonne."""

    if colonne not in df.columns:
        raise ValueError(f"Colonne absente : {colonne}")

    valeurs = (
        df.get_column(colonne)
        .cast(pl.Float64)
        .drop_nulls()
        .to_numpy()
    )

    valeurs = valeurs[np.isfinite(valeurs)]

    if len(valeurs) < 2:
        raise ValueError("Pas assez de valeurs numeriques valides")

    probabilites = np.arange(1, 100) / 100

    return np.quantile(valeurs, probabilites)


# 2. Estimation de la regression Theil-Sen
def estimer_theil_sen(
    df_source1: pl.DataFrame,
    df_source2: pl.DataFrame,
    colonne: str,
) -> tuple[float, float, float]:
    """Estimer la relation entre les quantiles de deux sources."""

    q1 = calculer_quantiles(df_source1, colonne)
    q2 = calculer_quantiles(df_source2, colonne)

    if np.ptp(q1) == 0:
        raise ValueError("Les quantiles de la source 1 sont constants")

    # Preparer les donnees pour scikit-learn
    X = q1.reshape(-1, 1)
    y = q2

    # Creer et entrainer le modele Theil-Sen
    modele = TheilSenRegressor(random_state=42)
    modele.fit(X, y)

    # Recuperer les coefficients et le score
    a = float(modele.coef_[0])
    b = float(modele.intercept_)
    r2 = float(modele.score(X, y))

    return a, b, r2


# 3. Choix de la transformation
def choisir_transformation(
    a: float,
    b: float,
    r2: float,
    facteurs_connus: list[float],
    seuil_proximite: float,
    seuil_r2: float = 0.95,
) -> tuple[float, float, str]:
    """Choisir la transformation selon les regles de la documentation."""

    if not 0 <= seuil_proximite <= 1:
        raise ValueError("Le seuil de proximite doit etre entre 0 et 1")

    if not 0 <= seuil_r2 <= 1:
        raise ValueError("Le seuil R2 doit etre entre 0 et 1")

    if not all(np.isfinite(v) for v in (a, b, r2)):
        raise ValueError("Les resultats de la regression sont invalides")

    if any(not np.isfinite(k) or k <= 0 for k in facteurs_connus):
        raise ValueError("Les facteurs connus doivent etre positifs")

    # Cas 1 : chercher un facteur connu proche de a
    if facteurs_connus:
        k = min(
            facteurs_connus,
            key=lambda facteur: abs(a - facteur) / facteur,
        )

        if abs(a - k) / k <= seuil_proximite:
            return k, 0.0, "facteur_connu"

    # Cas 2 : utiliser la regression Theil-Sen
    if r2 >= seuil_r2:
        return a, b, "theil_sen"

    # Cas 3 : aucune correction
    return 1.0, 0.0, "aucune_correction"


# 4. Application de la transformation
def appliquer_transformation(
    df: pl.DataFrame,
    colonne: str,
    a: float,
    b: float,
) -> pl.DataFrame:
    """Appliquer une transformation lineaire a une colonne."""

    if colonne not in df.columns:
        raise ValueError(f"Colonne absente : {colonne}")

    if not np.isfinite(a) or not np.isfinite(b):
        raise ValueError("Coefficients de transformation invalides")

    # Creer un nouveau DataFrame
    df_corrige = df.with_columns(
        (
            pl.col(colonne).cast(pl.Float64) * a + b
        ).alias(colonne)
    )

    return df_corrige


# 5. Processus complet d'amelioration de la dispersion
def ameliorer_dispersion(
    df_source1: pl.DataFrame,
    df_source2: pl.DataFrame,
    colonne: str,
    facteurs_connus: list[float],
    seuil_proximite: float,
    seuil_r2: float = 0.95,
) -> tuple[pl.DataFrame, str]:
    """Ameliorer l'echelle de la source 1 par rapport a la source 2."""

    # Etape 1 : estimer la regression
    a, b, r2 = estimer_theil_sen(
        df_source1,
        df_source2,
        colonne,
    )

    # Etape 2 : choisir la transformation
    facteur, decalage, methode = choisir_transformation(
        a,
        b,
        r2,
        facteurs_connus,
        seuil_proximite,
        seuil_r2,
    )

    # Etape 3 : ne rien changer si aucune correction n'est retenue
    if methode == "aucune_correction":
        return df_source1.clone(), methode

    # Etape 4 : appliquer la transformation
    df_corrige = appliquer_transformation(
        df_source1,
        colonne,
        facteur,
        decalage,
    )

    return df_corrige, methode


# ============================================================
# PARTIE 2 : AMELIORATION DU RATIO DE POSITION
# ============================================================


# 6. Detection des ratios de position atypiques
def reperer_ratios_atypiques(
    df: pl.DataFrame,
    colonne_min: str = "Salaire_Min",
    colonne_moyen: str = "Salaire_Moyen",
    colonne_max: str = "Salaire_Max",
    seuil_min: float = 0.1,
    seuil_max: float = 0.9,
) -> pl.DataFrame:
    """Retourner les row_id et ratios des lignes salariales atypiques."""

    colonnes = [colonne_min, colonne_moyen, colonne_max]

    if any(col not in df.columns for col in colonnes):
        raise ValueError("Colonnes de salaire manquantes")

    if not 0 <= seuil_min < seuil_max <= 1:
        raise ValueError("Seuils de ratio invalides")

    # Ajouter les identifiants avant de filtrer les lignes
    donnees = df.with_row_index("row_id")

    # Convertir les salaires en nombres
    donnees = donnees.with_columns(
        [
            pl.col(col)
            .cast(pl.String)
            .str.replace_all(r"\s+", "")
            .cast(pl.Float64, strict=False)
            .alias(col)
            for col in colonnes
        ]
    )

    # Garder les lignes dont les trois salaires sont presents
    donnees = donnees.drop_nulls(subset=colonnes)

    # Exclure les valeurs infinies ou non numeriques
    donnees = donnees.filter(
        pl.all_horizontal(
            [pl.col(col).is_finite() for col in colonnes]
        )
    )

    # Ne garder que les intervalles valides
    donnees = donnees.filter(
        (pl.col(colonne_min) <= pl.col(colonne_moyen))
        & (pl.col(colonne_moyen) <= pl.col(colonne_max))
        & (pl.col(colonne_min) < pl.col(colonne_max))
    )

    # Calculer le ratio de position
    donnees = donnees.with_columns(
        (
            (pl.col(colonne_moyen) - pl.col(colonne_min))
            / (pl.col(colonne_max) - pl.col(colonne_min))
        ).alias("ratio")
    )

    # Conserver uniquement les ratios atypiques
    return donnees.filter(
        (pl.col("ratio") < seuil_min)
        | (pl.col("ratio") > seuil_max)
    ).select("row_id", "ratio")


# 7. Tester une conversion sur une ligne de salaires
def tester_conversion_rp(
    salaire_min: float,
    salaire_moyen: float,
    salaire_max: float,
    colonne_a_corriger: str,
    facteur: float,
    operation: str,
    seuil_min: float = 0.1,
    seuil_max: float = 0.9,
) -> bool:
    """Verifier si une conversion rend le ratio de position normal."""

    if facteur <= 0 or not np.isfinite(facteur):
        raise ValueError("Facteur de conversion invalide")

    if operation not in ("multiplier", "diviser"):
        raise ValueError("Operation inconnue")

    if not 0 <= seuil_min < seuil_max <= 1:
        raise ValueError("Seuils invalides")

    valeurs = {
        "Salaire_Min": salaire_min,
        "Salaire_Moyen": salaire_moyen,
        "Salaire_Max": salaire_max,
    }

    if colonne_a_corriger not in valeurs:
        raise ValueError("Colonne de salaire inconnue")

    if not all(np.isfinite(v) for v in valeurs.values()):
        return False

    # Appliquer temporairement la conversion
    if operation == "multiplier":
        valeurs[colonne_a_corriger] *= facteur
    else:
        valeurs[colonne_a_corriger] /= facteur

    minimum = valeurs["Salaire_Min"]
    moyen = valeurs["Salaire_Moyen"]
    maximum = valeurs["Salaire_Max"]

    # Verifier l'ordre des salaires
    if not minimum <= moyen <= maximum or minimum == maximum:
        return False

    # Calculer le nouveau ratio
    ratio = (moyen - minimum) / (maximum - minimum)

    return seuil_min <= ratio <= seuil_max


# 8. Choisir un facteur commun pour les ratios atypiques
def choisir_facteur_rp(
    df: pl.DataFrame,
    colonne_a_corriger: str,
    facteurs: list[float],
    seuil_reparation: float = 0.5,
    seuil_min: float = 0.1,
    seuil_max: float = 0.9,
) -> tuple[float | None, str | None, int, int]:
    """Choisir la conversion ameliorant le plus de lignes atypiques."""

    if not 0 < seuil_reparation <= 1:
        raise ValueError("Seuil de reparation invalide")

    if colonne_a_corriger not in (
        "Salaire_Min",
        "Salaire_Moyen",
        "Salaire_Max",
    ):
        raise ValueError("Colonne de salaire inconnue")

    if not facteurs or any(
        not np.isfinite(f) or f <= 0 for f in facteurs
    ):
        raise ValueError("Facteurs de conversion invalides")

    # Etape 1 : identifier les lignes atypiques
    atypiques = reperer_ratios_atypiques(
        df,
        seuil_min=seuil_min,
        seuil_max=seuil_max,
    )

    total = atypiques.height

    if total == 0:
        return None, None, 0, 0

    # Etape 2 : recuperer les salaires de ces lignes
    donnees = (
        df.with_row_index("row_id")
        .filter(pl.col("row_id").is_in(atypiques["row_id"]))
        .select(["Salaire_Min", "Salaire_Moyen", "Salaire_Max"])
        .with_columns(
            [
                pl.col(col)
                .cast(pl.String)
                .str.replace_all(r"\s+", "")
                .cast(pl.Float64, strict=False)
                .alias(col)
                for col in (
                    "Salaire_Min",
                    "Salaire_Moyen",
                    "Salaire_Max",
                )
            ]
        )
    )

    # Garder les lignes dans une liste pour tester
    # plusieurs facteurs successivement
    lignes = list(donnees.iter_rows())

    # Etape 3 : essayer tous les facteurs et operations
    meilleur_facteur = None
    meilleure_operation = None
    meilleur_nombre = 0

    for facteur in facteurs:
        for operation in ("multiplier", "diviser"):

            nombre_corrigees = sum(
                tester_conversion_rp(
                    salaire_min,
                    salaire_moyen,
                    salaire_max,
                    colonne_a_corriger,
                    facteur,
                    operation,
                    seuil_min,
                    seuil_max,
                )
                for salaire_min, salaire_moyen, salaire_max in lignes
            )

            if nombre_corrigees > meilleur_nombre:
                meilleur_nombre = nombre_corrigees
                meilleur_facteur = facteur
                meilleure_operation = operation

    # Etape 4 : verifier la regle des 50 %
    if meilleur_nombre / total >= seuil_reparation:
        return (
            meilleur_facteur,
            meilleure_operation,
            meilleur_nombre,
            total,
        )

    # Aucun facteur suffisamment efficace
    return None, None, meilleur_nombre, total


# 9. Appliquer le facteur aux lignes ameliorables
def appliquer_facteur_rp(
    df: pl.DataFrame,
    colonne_a_corriger: str,
    facteur: float,
    operation: str,
    seuil_min: float = 0.1,
    seuil_max: float = 0.9,
) -> pl.DataFrame:
    """Corriger uniquement les lignes dont le ratio devient normal."""

    colonnes = ["Salaire_Min", "Salaire_Moyen", "Salaire_Max"]

    if any(col not in df.columns for col in colonnes):
        raise ValueError("Colonnes de salaire manquantes")

    if colonne_a_corriger not in colonnes:
        raise ValueError("Colonne de salaire inconnue")

    if not np.isfinite(facteur) or facteur <= 0:
        raise ValueError("Facteur invalide")

    if operation not in ("multiplier", "diviser"):
        raise ValueError("Operation inconnue")

    if not 0 <= seuil_min < seuil_max <= 1:
        raise ValueError("Seuils invalides")

    # Etape 1 : reperer les lignes atypiques
    atypiques = reperer_ratios_atypiques(
        df,
        seuil_min=seuil_min,
        seuil_max=seuil_max,
    )

    ids_atypiques = set(atypiques["row_id"].to_list())

    # Etape 2 : preparer les salaires pour tester les conversions
    donnees = df.with_row_index("__rid__").with_columns(
        [
            pl.col(col)
            .cast(pl.String)
            .str.replace_all(r"\s+", "")
            .cast(pl.Float64, strict=False)
            .alias(col)
            for col in colonnes
        ]
    )

    ids_a_corriger = []

    for rid, minimum, moyen, maximum in donnees.select(
        "__rid__", *colonnes
    ).iter_rows():

        if rid not in ids_atypiques:
            continue

        # Ne retenir que les lignes reellement ameliorables
        if tester_conversion_rp(
            minimum,
            moyen,
            maximum,
            colonne_a_corriger,
            facteur,
            operation,
            seuil_min,
            seuil_max,
        ):
            ids_a_corriger.append(rid)

    # Etape 3 : ne rien changer si aucune correction n'est possible
    if not ids_a_corriger:
        return df.clone()

    # Etape 4 : preparer la nouvelle valeur
    valeur = (
        pl.col(colonne_a_corriger)
        .cast(pl.String)
        .str.replace_all(r"\s+", "")
        .cast(pl.Float64, strict=False)
    )

    if operation == "multiplier":
        nouvelle_valeur = valeur * facteur
    else:
        nouvelle_valeur = valeur / facteur

    # Etape 5 : modifier uniquement les lignes concernees
    return (
        df.with_row_index("__rid__")
        .with_columns(
            pl.when(pl.col("__rid__").is_in(ids_a_corriger))
            .then(nouvelle_valeur)
            .otherwise(valeur)
            .alias(colonne_a_corriger)
        )
        .drop("__rid__")
    )


# ============================================================
# 10. PROCESSUS COMPLET D'AMELIORATION DU RATIO DE POSITION
# ============================================================

def ameliorer_ratio_position(
    df: pl.DataFrame,
    colonne_a_corriger: str,
    facteurs: list[float],
    seuil_reparation: float = 0.5,
    seuil_min: float = 0.1,
    seuil_max: float = 0.9,
) -> tuple[pl.DataFrame, str, int, int]:
    """Choisir puis appliquer la meilleure conversion RP."""

    # Etape 1 : choisir le meilleur facteur
    facteur, operation, nombre, total = choisir_facteur_rp(
        df,
        colonne_a_corriger,
        facteurs,
        seuil_reparation,
        seuil_min,
        seuil_max,
    )

    # Etape 2 : aucune correction si le seuil n'est pas atteint
    if facteur is None:
        return df.clone(), "aucune_correction", nombre, total

    # Etape 3 : appliquer la conversion retenue
    df_corrige = appliquer_facteur_rp(
        df,
        colonne_a_corriger,
        facteur,
        operation,
        seuil_min,
        seuil_max,
    )

    # Etape 4 : retourner les resultats
    methode = f"{operation}_par_{facteur}"

    return df_corrige, methode, nombre, total

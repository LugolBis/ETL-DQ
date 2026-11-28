import polars as pl
from polars import DataFrame
from sklearn.experimental import enable_iterative_imputer  # noqa: F401
from sklearn.impute import IterativeImputer


def _to_float(c: str) -> pl.Expr:
    # "16 800" -> 16800.0
    return pl.col(c).cast(pl.String).str.replace_all(r"\s+", "").cast(pl.Float64)


def mice_imputation(df: DataFrame, columns: list[str]) -> DataFrame:
    """
    Remplit les cases vides des colonnes numériques de `columns` avec MICE.
    Chaque valeur manquante est estimée à partir des autres colonnes (type régression linéaire).
    Les valeurs déjà connues ne sont pas modifiées.
    """
    x = df.select(_to_float(c) for c in columns).to_numpy()
    x_imp = IterativeImputer(max_iter=10, random_state=0).fit_transform(x)
    df_imp = pl.DataFrame(x_imp, schema=columns, orient="row")

    filled = []
    for c in columns:
        dtype = df.schema[c]

        if not dtype.is_numeric():
            continue

        values = df_imp[c].round() if dtype.is_integer() else df_imp[c]
        filled.append(pl.col(c).fill_null(values.cast(dtype)))

    return df.with_columns(filled)

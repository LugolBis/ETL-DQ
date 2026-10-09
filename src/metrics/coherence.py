from collections.abc import Callable
from functools import lru_cache
from typing import Any

import Levenshtein
import numpy as np
import polars as pl
from numpy import ndarray
from polars import DataFrame, Series
from sklearn.manifold import TSNE


def filter_rel(df: DataFrame, seuil_completude: float = 0.8) -> DataFrame:
    assert 0.0 <= seuil_completude <= 1.0, (
        f"Error: `seuil_completude` should be in [0; 1], fund {seuil_completude}"
    )

    n = df.height
    pk_cols = df.select(pl.all().n_unique())
    null_cols = df.null_count()

    def is_representative(col: str) -> bool:
        distinct_vals = pk_cols[col][0]
        null_count = null_cols[col][0]

        return (
            (distinct_vals != n)
            and ((distinct_vals - int(null_count > 0)) >= 2)
            and ((n - null_count) / n >= seuil_completude)
        )

    target_cols = [col for col in df.columns if is_representative(col)]

    return df.select(target_cols)


# Similarity & Distance


def compute_dissimilarity_matrix(labels: Series | list[str]) -> ndarray:
    N = len(labels)
    D = np.zeros((N, N))  # Init a matrix N x N filled with zeros

    for idx_row in range(N - 1):
        for idx_col in range(idx_row + 1, N):
            # We compute the distance using Levenshtein edit distance
            distance = Levenshtein.distance(labels[idx_row], labels[idx_col])
            word_len = max(len(labels[idx_row]), len(labels[idx_col]))
            distance_normalized = distance / word_len if word_len > 0 else 0.0

            D[idx_row][idx_col] = distance_normalized
            D[idx_col][idx_row] = distance_normalized  # symetric matrix
    return D


def multimodal_distance(a: Any, b: Any) -> float:
    if isinstance(a, np.ndarray) and isinstance(b, np.ndarray):
        return np.linalg.norm(a - b)
    return abs(a - b)


# Encoding


def encode_proj_rel(df: DataFrame) -> DataFrame:
    str_cols = [name for name, dtype in df.schema.items() if dtype == pl.Utf8]

    for col_name in str_cols:
        labels = (
            df.select(pl.col(col_name).unique(maintain_order=True))
            .to_series()
            .drop_nulls()
        )
        n = labels.len()

        D = compute_dissimilarity_matrix(labels)

        tsne = TSNE(
            n_components=2,
            perplexity=int(n * 0.5),
            metric="precomputed",
            init="random",
            random_state=42,
        )
        embeddings = tsne.fit_transform(D)

        mapping = {
            lab: [float(x), float(y)]
            for lab, (x, y) in zip(labels.to_list(), embeddings)
        }

        # /!\ WARNING : The DataFrame isn't anymore in 1NF
        df = df.with_columns(
            pl.col(col_name)
            .replace_strict(mapping, default=None, return_dtype=pl.Array(pl.Float64, 2))
            .alias(col_name)
        )

    return df


def encode_num_rel(df: DataFrame) -> DataFrame:
    exprs = [
        pl.col(name).cast(pl.Int8).alias(name)  # bool -> 0/1
        if dtype == pl.Boolean
        else pl.col(name).cast(pl.Int32).alias(name)  # date -> entier
        if dtype == pl.Date
        else pl.col(name)
        for name, dtype in df.schema.items()
    ]
    return df.with_columns(exprs)


def _global_stats(df_encoded: DataFrame, x: str):
    """Return : tuple[ndarray | float, float]"""
    values = df_encoded[x].drop_nulls().to_numpy()
    return values.mean(axis=0), values.var(axis=0, ddof=0).sum()


def _grouped_stats(df_encoded: DataFrame, x: str, y: str):
    dtype = df_encoded.schema[x]

    if isinstance(dtype, pl.Array):
        g = df_encoded.group_by(y).agg(pl.col(x).drop_nulls())
        N, mu, var = [], [], []
        for v in g[x]:
            m = np.array(v)
            if m.shape[0] == 0:
                continue
            N_g = m.shape[0]
            mu_g = m.mean(axis=0)
            var_g = m.var(axis=0, ddof=0).sum()

            N.append(N_g)
            mu.append(mu_g)
            var.append(var_g)
        return np.array(N), np.array(mu), np.array(var)

    agg = df_encoded.group_by(y).agg(
        [
            pl.col(x).count().alias("N"),
            pl.col(x).mean().alias("mu"),
            pl.col(x).var(ddof=0).alias("var"),
        ]
    )

    return agg["N"].to_numpy(), agg["mu"].to_numpy(), agg["var"].to_numpy()


def analyze_grouped_distribution(
    df: DataFrame,
    seuil_completude: float = 0.8,
    encoder: Callable[[DataFrame], DataFrame] = encode_proj_rel,
) -> DataFrame:
    df_filtered = filter_rel(df, seuil_completude)
    df_encoded = encoder(encode_num_rel(df_filtered))

    N = df_encoded.height
    epsilon = 10e-12
    columns = df_encoded.columns

    global_cache = {x: _global_stats(df_encoded, x) for x in columns}
    xs_out, ys_out, m1s, m2s, m3s = [], [], [], [], []

    for x in columns:
        global_mu, global_var = global_cache[x]
        for y in columns:
            if y == x:
                continue
            Ng, mus, vars_g = _grouped_stats(df_encoded, x, y)

            var_intra = float(np.sum((Ng / N) * vars_g))
            max_group_var = float(np.max(vars_g))

            if isinstance(global_mu, np.ndarray):
                dists = np.linalg.norm(mus - global_mu, axis=1)
            else:
                dists = np.abs(mus - global_mu)
            M3 = float(np.max(np.sqrt(Ng) * dists)) / (np.sqrt(global_var) + epsilon)

            M1 = (global_var - var_intra) / (global_var + epsilon)
            M2 = max_group_var / (var_intra + epsilon)

            xs_out.append(x)
            ys_out.append(y)
            m1s.append(M1)
            m2s.append(M2)
            m3s.append(M3)

    return DataFrame(
        {
            "x": pl.Series(xs_out, dtype=pl.String),
            "y": pl.Series(ys_out, dtype=pl.String),
            "M1": pl.Series(m1s, dtype=pl.Float64),
            "M2": pl.Series(m2s, dtype=pl.Float64),
            "M3": pl.Series(m3s, dtype=pl.Float64),
        }
    )


@lru_cache(maxsize=1)
def _load_model(model_name):
    from sentence_transformers import SentenceTransformer

    return SentenceTransformer(model_name)


def encode_proj_rel_slm(df, model_name="intfloat/multilingual-e5-small"):
    model = _load_model(model_name)
    str_cols = [name for name, dtype in df.schema.items() if dtype == pl.Utf8]
    for col_name in str_cols:
        labels = (
            df.select(pl.col(col_name).unique(maintain_order=True))
            .to_series()
            .drop_nulls()
        )
        if labels.len() == 0:
            continue
        prefixed = [f"query: {x}" for x in labels.to_list()]
        embeddings = model.encode(prefixed, normalize_embeddings=True)
        mapping = dict(zip(labels.to_list(), embeddings))
        df = df.with_columns(
            pl.col(col_name)
            .replace_strict(
                mapping,
                default=None,
                return_dtype=pl.Array(pl.Float64, embeddings.shape[1]),
            )
            .alias(col_name)
        )

    return df

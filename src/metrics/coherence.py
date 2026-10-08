from typing import Any

import Levenshtein
import numpy as np
import polars as pl
from numpy import ndarray
from polars import DataFrame, Series
from sklearn.manifold import TSNE


def filter_rel(df: DataFrame) -> DataFrame:
    n = df.height
    pk_cols = df.select(pl.all().n_unique())
    target_cols = [col for col in df.columns if pk_cols[col][0] != n]

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

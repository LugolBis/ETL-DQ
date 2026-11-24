"""
**Deduplication of a Polars DataFrame through the detection of approximate functional dependencies (AFDs, TANE algorithm), guided by an error registry.**

**Inputs**
df      : the relation R to be deduplicated.
errors  : DataFrame {"row_id": Int64, "source": Utf8, "column_name": Utf8}.
          ``column_name`` = 1..N columns (separated by ', ') tested as a key
          on R, which do not constitute a key and contain duplicates.

**What the error registry changes compared to TANE**
Let C = {K1, K2, ...} be the sets of columns extracted from ``column_name``.

1. **Lattice subtree pruning (anti-monotonicity).**
   A node X (= LHS \\cup {A}) is generated only if there exists K \\in C such that X ⊆ K.
   If X \\subsetneq K for every K, none of its supersets can be contained in K either:
   the entire subtree is pruned. Therefore, only dependencies
   K' \\ {A} -> A are tested within faulty keys, i.e., quasi-collisions:
   rows that are identical on K \\ {A} but differ on A (e.g., a typo in one
   of the key columns).

2. **Candidate filtering.**
   For an AFD X -> A (0 < err ≤ ε), a group of rows with "same X, different A" is retained
   only if it contains at least one row flagged for a key K \\supseteq X \\cup {A}
   (``only_flagged=True``). The goal is to find quasi-duplicates *around* the duplicates
   already identified.

3. **Exact duplicates** (flagged rows grouped by their values on K) are
   added to the candidates.

Next: merge overlapping groups (connected components), then apply a survival rule
to retain a single row per cluster.

**Null values:** a row containing a null in the LHS is never grouped (identity is unknown); a null in the RHS is not considered a conflict. g3 is therefore computed on non-null rows for the relevant columns.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass

import polars as pl
from polars import DataFrame

log = logging.getLogger(__name__)

ROW = "__row_id__"  # internal identifier
SEP = ", "  # separator used in the "column_name"
REQUIRED_ERROR_COLS = {"row_id", "source", "column_name"}

AttrSet = frozenset  # frozenset[str]


def _is_in(ids: Iterable[int]) -> pl.Expr:
    return pl.col(ROW).is_in(pl.Series(sorted(ids), dtype=pl.Int64).implode())


@dataclass(frozen=True, slots=True)
class DFA:
    """
    Aproximative Functional Dependency (DFA) lhs -> rhs, with an error g3.
    """

    lhs: frozenset
    rhs: str
    err: float

    def __str__(self) -> str:
        return f"{{{', '.join(sorted(self.lhs))}}} -> {self.rhs}  (g3={self.err:.4f})"


@dataclass
class DedupResult:
    deduped: DataFrame  # R without duplicate
    clusters: DataFrame  # row_id, survivor_id, is_survivor (clusters of size ≥ 2)
    dfas: list[DFA]  # DFA kept (with violation or not)


# 1. Preparation inputs
def _prepare_relation(df: DataFrame, row_id_col: str | None) -> DataFrame:
    """
    Add the column ROW (Int64) : column ``row_id_col`` or position 0-based.
    """

    if row_id_col is None:
        return df.with_row_index(ROW).with_columns(pl.col(ROW).cast(pl.Int64))

    if row_id_col not in df.columns:
        raise ValueError(f"Column for row identifier missing from df : {row_id_col!r}")

    return df.with_columns(pl.col(row_id_col).cast(pl.Int64).alias(ROW))


def parse_errors(
    errors: DataFrame, columns: Iterable[str], source: str | None = None
) -> dict[frozenset, set[int]]:
    """
    Return {K: set of row_id reported for K}, K is a frozenset of columns.
    """

    missing = REQUIRED_ERROR_COLS - set(errors.columns)
    if missing:
        raise ValueError(f"Columns missing in errors : {sorted(missing)}")

    if source is not None:
        errors = errors.filter(pl.col("source") == source)

    known = set(columns)
    nonkeys: dict[frozenset, set[int]] = {}
    grouped = errors.group_by("column_name").agg(
        pl.col("row_id").cast(pl.Int64).unique().alias("rows")
    )

    for name, rows in grouped.iter_rows():
        cols = frozenset(c.strip() for c in name.split(SEP.strip()) if c.strip())
        unknown = cols - known

        if unknown:
            raise ValueError(
                f"Columns unknown in errors.column_name={name!r} : {sorted(unknown)}"
            )
        nonkeys.setdefault(cols, set()).update(rows)

    return nonkeys


# 2. g3 and pruned TANE
def g3(work: DataFrame, lhs: Iterable[str], rhs: str) -> float:
    """
    Minimal proportion of tuples to delete for lhs -> rhs be exact.
    g3 = 1 - Sum_groups(effectively of the majority value of rhs) / |R|
    """

    lhs = sorted(lhs)
    sub = work.drop_nulls([*lhs, rhs])
    n = sub.height
    if n == 0:
        return 0.0

    counts = sub.group_by([*lhs, rhs]).agg(pl.len().alias("__n__"))
    if lhs:
        counts = counts.group_by(lhs).agg(pl.col("__n__").max())

    kept = counts["__n__"].max() if not lhs else counts["__n__"].sum()
    return (n - kept) / n  # ty: ignore[unsupported-operator]


def next_level(
    level: Sequence[frozenset], admissible: Callable[[frozenset], bool]
) -> list[frozenset]:
    """
    Genration supposed of the next level : we merge the sets who share the same prefix, then we keep Y if :
      (a) Y is eligible (included in a faulty key)  -> prune by the errors
      (b) all of the subset of size |Y| - 1 are in the level -> prune by TANE
    """

    present = set(level)
    blocks: dict[tuple, list[str]] = defaultdict(list)

    for X in level:
        s = tuple(sorted(X))
        blocks[s[:-1]].append(s[-1])

    out = []
    for prefix, tails in blocks.items():
        tails.sort()

        for i, a in enumerate(tails):
            for b in tails[i + 1 :]:
                Y = frozenset((*prefix, a, b))
                if admissible(Y) and all(Y - {c} in present for c in Y):
                    out.append(Y)

    return out


def tane(
    work: DataFrame,
    epsilon: float,
    nonkeys: Iterable[frozenset],
    max_lhs: int = 3,
) -> list[DFA]:
    """
    TANE, whose trellis is restricted to the set of all subsets of the faulty keys.
    """

    nonkeys = list(nonkeys)
    attrs = set().union(*nonkeys) if nonkeys else set()

    def admissible(Y: frozenset) -> bool:
        return any(Y <= K for K in nonkeys)

    C: dict[frozenset, set[str]] = {frozenset(): set(attrs)}  # C+(X)
    level = [frozenset([a]) for a in sorted(attrs)]
    dfas: list[DFA] = []
    n_tests = 0

    while level and len(level[0]) <= max_lhs + 1:
        for X in level:
            C[X] = set.intersection(*(C[X - {A}] for A in X))

            for A in sorted(X & C[X]):
                err = g3(work, X - {A}, A)
                n_tests += 1

                if err <= epsilon:  # X\{A} -> A est une DFA valide
                    dfas.append(DFA(X - {A}, A, err))
                    C[X].discard(A)  # minimalité
                    if err == 0:
                        C[X] -= attrs - X  # élagage supplémentaire pour les DF exactes

        level = [X for X in level if C[X]]
        level = next_level(level, admissible)

    log.info("TANE : %d tests g3, %d DFA kept", n_tests, len(dfas))
    return dfas


# 3. Candidats : exact duplicates + groups who violate a DFA
def exact_duplicate_groups(
    work: DataFrame, nonkeys: dict[frozenset, set[int]]
) -> list[list[int]]:
    """
    Groups of equal lines on K, reconstructed from the flagged lines.
    We start again from the K *values* of the flagged rows (semi-join): this is
    robust regardless of whether the register lists all rows in a group or only the excess ones.
    """

    groups: list[list[int]] = []
    for K, rows in nonkeys.items():
        cols = sorted(K)
        values = work.filter(_is_in(rows)).select(cols).drop_nulls().unique()
        members = work.join(values, on=cols, how="semi")
        g = members.group_by(cols).agg(pl.col(ROW)).filter(pl.col(ROW).list.len() > 1)
        groups.extend(g[ROW].to_list())

    return groups


def violating_groups(
    work: DataFrame, lhs: Iterable[str], rhs: str, seeds: set[int] | None = None
) -> list[list[int]]:
    """Group the rows of same lhs where rhs take more than one value (null ignored)"""
    lhs = sorted(lhs)
    aggs = [
        pl.col(ROW),
        pl.col(rhs).drop_nulls().n_unique().alias("__nd__"),
    ]

    if seeds is not None:
        aggs.append(_is_in(seeds).any().alias("__seed__"))

    g = work.drop_nulls(lhs).group_by(lhs).agg(aggs).filter(pl.col("__nd__") > 1)

    if seeds is not None:
        g = g.filter(pl.col("__seed__"))

    return g[ROW].to_list()


def detect_duplicates_dfa(
    work: DataFrame,
    nonkeys: dict[frozenset, set[int]],
    epsilon: float,
    max_lhs: int = 3,
    only_flagged: bool = True,
) -> tuple[list[list[int]], list[DFA]]:
    """
    Adapted version of DetectDuplicatesDFA : return (candidates groupes, DFA).
    """

    candidates = exact_duplicate_groups(work, nonkeys)
    dfas = tane(work, epsilon, nonkeys, max_lhs)

    for dfa in dfas:
        # err == 0 : no violation; empty LHS: all tuples would be candidates
        if dfa.err == 0 or not dfa.lhs:
            continue

        seeds = None
        if only_flagged:
            attrs = dfa.lhs | {dfa.rhs}
            seeds = set().union(*(rows for K, rows in nonkeys.items() if attrs <= K))

        candidates.extend(violating_groups(work, dfa.lhs, dfa.rhs, seeds))

    return candidates, dfas


# 4. Merge candidates + follow-up rule
def merge_groups(groups: Iterable[Sequence[int]]) -> dict[int, int]:
    """
    Union-find: merges groups that share a row. Returns {row_id: root}.
    """

    parent: dict[int, int] = {}

    def _find(x: int) -> int:
        root = x
        while parent[root] != root:
            root = parent[root]
        while parent[x] != root:  # compression of the path
            nxt = parent[x]
            parent[x] = root
            x = nxt
        return root

    for group in groups:
        if not group:
            continue

        first = group[0]
        parent.setdefault(first, first)

        for rid in group[1:]:
            parent.setdefault(rid, rid)
            rid_a, rid_b = _find(first), _find(rid)

            if rid_a != rid_b:
                parent[rid_b] = rid_a

    return {rid: _find(rid) for rid in parent}


def apply_survival_rule(
    work: DataFrame,
    cluster_of: dict[int, int],
    order_by: str | None = None,
    order_descending: bool = True,
) -> DataFrame:
    """
    Selects one survivor per cluster. Order of preference:
      1. ``order_by`` (if provided; defaults to the highest value, e.g., the most recent date)
      2. Most complete row (fewest nulls)
      3. Smallest ``row_id`` (for determinism)
    Returns: ``row_id``, ``survivor_id``, ``is_survivor``.
    """
    members = DataFrame(
        {ROW: list(cluster_of.keys()), "__cluster__": list(cluster_of.values())},
        schema={ROW: pl.Int64, "__cluster__": pl.Int64},
    )
    data_cols = [c for c in work.columns if c != ROW]

    by, desc = [], []
    if order_by is not None:
        by.append(order_by)
        desc.append(order_descending)

    by += ["__nulls__", ROW]
    desc += [False, False]

    ranked = (
        work.join(members, on=ROW, how="inner")
        .with_columns(
            pl.sum_horizontal(
                *[pl.col(c).is_null().cast(pl.Int32) for c in data_cols]
            ).alias("__nulls__")
        )
        .sort(by, descending=desc, nulls_last=True)
        .with_columns(pl.col(ROW).first().over("__cluster__").alias("survivor_id"))
        .with_columns((pl.col(ROW) == pl.col("survivor_id")).alias("is_survivor"))
    )

    return ranked.select(
        pl.col(ROW).alias("row_id"), "survivor_id", "is_survivor"
    ).sort("survivor_id", "row_id")


# 5. Entry point
def deduplicate(
    df: DataFrame,
    errors: DataFrame,
    source: str,
    epsilon: float = 0.05,
    max_lhs: int = 3,
    row_id_col: str | None = None,
    only_flagged: bool = True,
    order_by: str | None = None,
    order_descending: bool = True,
) -> DedupResult:
    """
    Delete `df` duplicates using the register `errors`.

    Parameters :
        epsilon       : treshold g3 of a valid DFA.
        max_lhs       : maximum size of the LHS explored (the lattice goes up to max_lhs + 1).
        row_id_col    : column in df corresponding to errors.row_id; None = position (0-indexed).
        source        : keep only the errors from this source (None = all).
        only_flagged  : True  -> A DFA group is a candidate only if it contains a marked row. ;
                        False -> all groups that violate a DFA from the pruned lattice are candidates.
        order_by      : column that separates the survivors (ex. "updated_at").
    """

    work = _prepare_relation(df, row_id_col)
    nonkeys = parse_errors(errors, df.columns, source)

    empty_clusters = DataFrame(
        schema={"row_id": pl.Int64, "survivor_id": pl.Int64, "is_survivor": pl.Boolean}
    )
    if not nonkeys:
        return DedupResult(df, empty_clusters, [])

    candidates, dfas = detect_duplicates_dfa(
        work, nonkeys, epsilon, max_lhs, only_flagged
    )
    cluster_of = merge_groups(candidates)

    if not cluster_of:
        return DedupResult(df, empty_clusters, dfas)

    clusters = apply_survival_rule(work, cluster_of, order_by, order_descending)
    removed = clusters.filter(~pl.col("is_survivor"))["row_id"]
    deduped = work.filter(~_is_in(removed.to_list())).drop(ROW)

    log.info(
        "%d deleted lines in %d clusters",
        removed.len(),
        clusters["survivor_id"].n_unique(),
    )
    return DedupResult(deduped, clusters, dfas)

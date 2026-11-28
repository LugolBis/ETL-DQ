import os
from collections import defaultdict
from pathlib import Path

import polars as pl
from polars import DataFrame

from orchestration.tasks.enums import FileType
from orchestration.tasks.models import DfReader

SEPARATOR = "__union__"

type LAZY_SOURCES = list[DfReader]
type READED_SOURCES = list[tuple[DataFrame, str]]


def _find_match_ranges(
    srcs_names: list[str], candidates: list[tuple[list[str], str]]
) -> list[tuple[int, int, int]]:
    """
    For each list `union_names` at index `x` in `srcs_union`, return the ranges (`x`, `start`, `end`)
    such that `srcs_names`[`start`:`end`] == `union_names` (with the order strictly preserved).
    """
    matches = []

    for index, (union_names, _) in enumerate(candidates):
        size = len(union_names)

        if size == 0:
            continue

        for index_srcn in range(len(srcs_names) - size + 1):
            if srcs_names[index_srcn : index_srcn + size] == union_names:
                matches.append((index, index_srcn, index_srcn + size))

    return matches


def _rewrite_reading(
    srcs_names: list[str],
    matches: list[tuple[int, int, int]],
    prefer_longest: bool = True,
) -> list[tuple[int, bool]]:
    """
    Apply the replacement of `matches` index on `srcs_names` using the values of `srcs_union` while avoiding overlaps.
    By default at equal position we take the longest match.
    """
    by_start: dict[int, list[tuple[int, int, int]]] = defaultdict(list)

    for idx_u, idx_start, idx_end in matches:
        by_start[idx_start].append((idx_u, idx_start, idx_end))

    merged = []
    index = 0
    while index < len(srcs_names):
        if index in by_start:
            candidates = by_start[index]

            if prefer_longest:
                idx_u, _, idx_end = max(candidates, key=lambda t: t[2] - t[1])
            else:
                idx_u, _, idx_end = candidates[0]

            merged.append((idx_u, True))
            index = idx_end
        else:
            merged.append((index, False))
            index += 1

    return merged


def _resolve_union_dyn(srcs_names: list[str], data_dir: Path) -> list[DfReader | int]:
    candidates = []

    for fname in os.listdir(data_dir):
        if SEPARATOR in fname:
            (raw_name, extension) = fname.split(".")
            srcs_union = raw_name.split(SEPARATOR)
            candidates.append((srcs_union, extension))

    matches = _find_match_ranges(srcs_names, candidates)
    mask = _rewrite_reading(srcs_names, matches)

    readers = []
    for index, is_union in mask:
        if is_union:
            union_names, extension = candidates[index]
            filename = SEPARATOR.join(union_names) + f".{extension}"
            readers.append(DfReader(data_dir / filename, FileType(extension)))
        else:
            readers.append(index)

    return readers


def union_dyn(sources: READED_SOURCES, data_dir: Path) -> DataFrame:
    srcs_names = [name for (_, name) in sources]
    readers = _resolve_union_dyn(srcs_names, data_dir)

    dfs = [
        reader.read() if isinstance(reader, DfReader) else sources[reader][0]
        for reader in readers
    ]

    return pl.concat(dfs, how="vertical")


def lazy_union_dyn(sources: LAZY_SOURCES, data_dir: Path) -> list[DfReader]:
    srcs_names = [df_r.path.stem for df_r in sources]
    readers = _resolve_union_dyn(srcs_names, data_dir)

    df_rs = [
        reader if isinstance(reader, DfReader) else sources[reader]
        for reader in readers
    ]

    return df_rs

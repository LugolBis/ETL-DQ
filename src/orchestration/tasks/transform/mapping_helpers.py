"""Helpers that turn the mappings into what the transformations need."""

import polars as pl

from mappings import DataType, LinkKind, MappingError, MappingSet, SourceLink
from orchestration.tasks.dtypes import polars_dtype
from orchestration.tasks.transform.helpers import ADRESSE_EXTRACTIONS


def aggregation_sql(mappings: MappingSet, global_relation: str) -> str:
    """Polars SQL computing a global relation from the joined sources.

    The computed attributes (e.g. avg(NB_KW_Jour * 365)) come from the views;
    every other attribute of the global relation is a GROUP BY key.
    """
    relation = mappings.global_relation(global_relation)
    measures = mappings.measures(global_relation)
    if not measures:
        raise MappingError(
            f"{global_relation} : aucune mesure calculée dans les mappings"
        )

    columns: list[str] = []
    keys: list[str] = []
    for name in relation.attribute_names:
        measure = measures.get(name)
        if measure is None:
            columns.append(name)
            keys.append(name)
        elif measure.aggregate is None:
            raise MappingError(f"{global_relation}.{name} : un agrégat est requis")
        else:
            columns.append(
                f"{measure.aggregate.value.upper()}({measure.expression}) AS {name}"
            )

    sql = f"SELECT {', '.join(columns)} FROM self"
    return f"{sql} GROUP BY {', '.join(keys)}" if keys else sql


def link_extractions(link: SourceLink) -> list[tuple[str, str]]:
    """Address-extraction patterns for the right-hand columns of an `extracts` link."""
    if link.kind is not LinkKind.EXTRACTS:
        raise MappingError(f"Lien {link.name!r} : un lien `extracts` est attendu")
    unknown = [c for c in link.right.columns if c not in dict(ADRESSE_EXTRACTIONS)]
    if unknown:
        raise MappingError(
            f"Lien {link.name!r} : pas d'extraction d'adresse pour {unknown}"
        )
    wanted = set(link.right.columns)
    return [(name, regex) for name, regex in ADRESSE_EXTRACTIONS if name in wanted]


def link_dtypes(mappings: MappingSet, link: SourceLink) -> dict[str, type[pl.DataType]]:
    """Types to cast the extracted columns to (text columns need no cast)."""
    relation = mappings.source_relation(link.right.relation)
    return {
        name: polars_dtype(relation.column(name).dtype)
        for name in link.right.columns
        if relation.column(name).dtype is not DataType.VARCHAR
    }

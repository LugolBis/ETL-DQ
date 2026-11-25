"""Local-As-View (LAV) mappings between sources, staged files and the global schema.

Standard library only: this package knows nothing about Airflow, Polars or the
DAGs. Pipelines *read* a `MappingSet` and decide what to do with it.

Model
-----
* Global schema : `GlobalRelation` (materialised as `<name>.parquet`).
* Source side   : `SourceRelation` = one raw file, its staging rules (`Column`:
                  cleanup / cast / rename) and its staged file
                  `<name>_stage.parquet`.
* LAV mapping   : each source relation is a *view over the global schema*:
                      Source(x1..xn)  ⊆  q1 ∪ ... ∪ qk
                  where each qi is a conjunctive query (`Atom`s + `Condition`s)
                  over global relations. Attributes left out of an atom are
                  existential (`_`); source columns used in no atom are payload.
* Source links  : `SourceLink` records how source relations join each other.

Instantiate from JSON or from Python constants (same model, same validation):

    ms = MappingSet.from_json("config/mappings.json", data_dir="/opt/airflow/.data")
    # or: from mappings.definitions import MAPPINGS as ms

    ms.staged_path("Source4_IRIS")             # .../Source4_IRIS_stage.parquet
    ms.source_relation("Source4_IRIS").rename_map   # {"ID_IRIS": "ID_Iris"}
    ms.dependencies("Consommation_CSP")        # {"source1": (...), "source3": (...)}
    print(ms.describe())                       # Datalog-like view of every mapping
"""

from .base import (
    Aggregate,
    Containment,
    DataType,
    FileFormat,
    LinkKind,
    MappingError,
    Operator,
)
from .mapping_set import MappingSet
from .model import (
    STAGE_SUFFIX,
    Atom,
    Attribute,
    Col,
    Column,
    Condition,
    ConjunctiveQuery,
    Const,
    Derived,
    GlobalRelation,
    LavMapping,
    LinkEnd,
    RawFile,
    Replacement,
    SourceLink,
    SourceRelation,
)

__all__ = [
    "STAGE_SUFFIX",
    "Aggregate",
    "Atom",
    "Attribute",
    "Col",
    "Column",
    "Condition",
    "ConjunctiveQuery",
    "Const",
    "Containment",
    "DataType",
    "Derived",
    "FileFormat",
    "GlobalRelation",
    "LavMapping",
    "LinkEnd",
    "LinkKind",
    "MappingError",
    "MappingSet",
    "Operator",
    "RawFile",
    "Replacement",
    "SourceLink",
    "SourceRelation",
]

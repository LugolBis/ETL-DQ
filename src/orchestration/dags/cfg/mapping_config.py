"""Entry point of the orchestration layer to the LAV mappings.

The mappings are loaded once from `config/mappings.json` (override the location
with the MAPPINGS_CONFIG environment variable). DAGs read paths, dependencies and
extraction rules from here instead of hard-coding them.
"""

import os
from pathlib import Path

from mappings import MappingSet

DEFAULT_CONFIG = Path(__file__).resolve().parent / "mappings.json"
CONFIG_PATH = Path(os.environ.get("MAPPINGS_CONFIG", DEFAULT_CONFIG))

MAPPINGS = MappingSet.from_json(CONFIG_PATH)


def extract_dag_id(source_id: str) -> str:
    """dag_id of the extraction DAG of a data provider: 'source1' -> 'extract_source1'."""
    return f"extract_{source_id}"


def extract_deps(global_relation: str) -> tuple[tuple[str, list[Path]], ...]:
    """`sources_deps` of a transformation DAG: ((extract dag_id, [staged files]), ...).

    Follows the declaration order of the source relations in the mappings, which is
    the order of the positional arguments expected by the transformation functions.
    """
    return tuple(
        (extract_dag_id(source_id), list(paths))
        for source_id, paths in MAPPINGS.dependencies(global_relation).items()
    )

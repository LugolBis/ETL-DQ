"""One extraction DAG per data provider, generated from the mappings.

Replaces extract_source1.py, extract_source2.py, extract_source3.py and
extract_source4.py: every raw file declared in `config/mappings.json` is read
with its declared options and written as `<name>_stage.parquet`, applying its
staging rules (cleanup / cast / rename) when it has any.
"""

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag

from mappings import SourceRelation
from orchestration.dags.cfg.mapping_config import MAPPINGS, extract_dag_id
from orchestration.tasks.enums import FileType
from orchestration.tasks.extract.core import extract
from orchestration.tasks.extract.staging import needs_staging, staging_func
from orchestration.tasks.models import DfReader, DfWriter
from orchestration.tasks.transform.core import apply_df


def _label(relation: SourceRelation) -> str:
    """'Source3_CSP' -> 'CSP' (used for task ids and tags)."""
    return relation.name.split("_", 1)[-1]


def _extract_task(relation: SourceRelation):
    # FileFormat and FileType share their member names (CSV, PARQUET)
    reader = DfReader(
        MAPPINGS.raw_path(relation.name),
        FileType[relation.raw.format.name],
        **relation.raw.options,
    )
    writer = DfWriter(MAPPINGS.staged_path(relation.name), FileType.PARQUET)
    task_id = f"Extract_{_label(relation)}"

    if not needs_staging(relation):
        return extract.override(task_id=task_id)(reader, writer)

    return apply_df.override(task_id=task_id)(
        reader, func=staging_func(relation), df_w=writer
    )


def build_extract_dag(source_id: str):
    relations = MAPPINGS.relations_of_source(source_id)

    @dag(
        dag_id=extract_dag_id(source_id),
        schedule=None,
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        catchup=False,
        tags=[source_id.capitalize(), *(_label(r) for r in relations)],
    )
    def extract_source():
        start = EmptyOperator(task_id="start")
        end = EmptyOperator(task_id="end")

        start >> [_extract_task(relation) for relation in relations] >> end

    return extract_source()


# Airflow discovers DAGs among the module globals
for _source_id in MAPPINGS.source_ids():
    globals()[extract_dag_id(_source_id)] = build_extract_dag(_source_id)

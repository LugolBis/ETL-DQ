from airflow.sdk import task

from orchestration.tasks.models import DfReader


@task()
def display(df_r: DfReader, limit: int) -> None:
    df_r.read().show(limit)

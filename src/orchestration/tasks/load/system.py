from datetime import datetime
from zoneinfo import ZoneInfo

from airflow.sdk import task

from orchestration.tasks.models import DfReader


@task()
def display(df_r: DfReader, limit: int) -> None:
    df_r.read().show(limit)


@task()
def get_timestamp() -> str:
    return datetime.now(ZoneInfo("UTC")).isoformat()

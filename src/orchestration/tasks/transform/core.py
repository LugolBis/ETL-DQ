from collections.abc import Callable

from airflow.sdk import task
from polars import DataFrame

from orchestration.tasks.models import DfReader, DfWriter


@task()
def apply_dfs(
    df_ra: DfReader,
    df_rb: DfReader,
    func: Callable[[DataFrame, DataFrame], DataFrame],
    df_w: DfWriter,
) -> None:
    df_w.write(func(df_ra.read(), df_rb.read()))


@task()
def apply_df(
    df_r: DfReader, func: Callable[[DataFrame], DataFrame], df_w: DfWriter
) -> None:
    df_w.write(func(df_r.read()))


@task()
def query(df_r: DfReader, sql_query: str, df_w: DfWriter) -> None:
    df_w.write(df_r.read().sql(sql_query))

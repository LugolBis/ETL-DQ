import os

from airflow.sdk import task

from orchestration.tasks.models import DfReader, DfWriter


@task()
def extract(df_reader: DfReader, df_writer: DfWriter) -> None:
    """
    Extract a DataFrame using `df_reader` and write it using `df_writer`.
    """

    if not os.path.exists(df_reader.path):
        raise ValueError(f"The following path doesn't exist : {df_reader.path}")

    os.makedirs(df_writer.path.parent, exist_ok=True)
    df_writer.write(df_reader.read())

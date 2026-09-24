import pendulum
from airflow.sdk import dag

from orchestration.tasks.test import extract, load, transform


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "airflow-v3"],
)
def airflow_test():
    """### Airflow 3.3 DAG Example

    A simple data pipeline using the TaskFlow API in Airflow 3.
    """

    # Define task dependencies using the TaskFlow pattern
    raw_data = extract()
    processed_data = transform(raw_data)  # ty: ignore[invalid-argument-type]
    load(processed_data)  # ty: ignore[invalid-argument-type]


dag_instance = airflow_test()

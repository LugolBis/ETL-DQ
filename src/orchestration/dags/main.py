import pendulum
from airflow.decorators import dag, task


@task()
def extract():
    """Extract data step."""
    data = {"count": 42, "items": ["apple", "banana", "cherry"]}
    print("Extracted data successfully.")
    return data


@task()
def transform(raw_data: dict):
    """Transform data step."""
    count = raw_data["count"]
    doubled_count = count * 2
    print(f"Transformed count from {count} to {doubled_count}.")
    return {"count": doubled_count, "items": raw_data["items"]}


@task()
def load(transformed_data: dict):
    """Load data step."""

    assert transformed_data["count"] == 84
    assert transformed_data["items"] == ["apple", "banana", "cherry"]

    print(f"Loaded final count: {transformed_data['count']}")
    print(f"Loaded items: {transformed_data['items']}")


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
    processed_data = transform(raw_data)
    load(processed_data)


dag_instance = airflow_test()

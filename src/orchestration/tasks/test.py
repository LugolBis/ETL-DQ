from airflow.sdk import task


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


import pendulum
from airflow.sdk import dag, task

from orchestration.comparaison_consommations import comparer_consommations


@dag(
    dag_id="controle_qualite",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["qualite", "polars"],
)
def controle_qualite():

    @task
    def comparer_paris_evry():
        comparer_consommations()

    comparer_paris_evry()


dag_instance = controle_qualite()

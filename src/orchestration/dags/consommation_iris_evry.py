import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag

from mappings import LinkKind
from orchestration.dags.cfg.mapping_config import MAPPINGS, extract_deps
from orchestration.tasks.enums import FileType
from orchestration.tasks.extract.system import ensure_extracted
from orchestration.tasks.load.system import display
from orchestration.tasks.models import DfReader, DfWriter
from orchestration.tasks.transform.core import apply_dfs
from orchestration.tasks.transform.helpers import fuzzy_join
from orchestration.tasks.transform.mapping_helpers import aggregation_sql

TARGET = "Consommation_IRIS_Evry"

# GROUP BY keys and AVG(NB_KW_Jour * 365) come from the views of the mappings
SQL_QUERY_STATS = aggregation_sql(MAPPINGS, TARGET)


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["Consommation", "IRIS"],
)
def consommation_iris_evry():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # (extract dag_id, staged files) for every source feeding the target
    sources_deps = extract_deps(TARGET)

    # Nom_Rue of the consumption (left) contains the tags of ID_Rue of the IRIS (right)
    conso2_iris4 = MAPPINGS.link("conso2_iris4", LinkKind.CONTAINS)

    df_readers = [
        DfReader(path, FileType.PARQUET) for _, paths in sources_deps for path in paths
    ]

    task_transform = apply_dfs.override(task_id="Join_and_Agg_ops")(
        *df_readers,
        func=lambda df_a, df_b: fuzzy_join(
            df_a,
            df_b,
            conso2_iris4.left.columns[0],
            conso2_iris4.right.columns[0],
            "column_2",
            "inner",
        ).sql(SQL_QUERY_STATS),
        df_w=DfWriter(MAPPINGS.global_path(TARGET), FileType.PARQUET),
    )

    task_display = display(DfReader(MAPPINGS.global_path(TARGET), FileType.PARQUET), 15)

    # Tasks chaining - HITL
    for source_id, file_paths in sources_deps:
        start >> ensure_extracted(source_id, file_paths) >> task_transform

    task_transform >> task_display >> end


dag_instance = consommation_iris_evry()

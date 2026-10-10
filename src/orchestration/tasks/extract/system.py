from __future__ import annotations

from pathlib import Path

from airflow.exceptions import AirflowSkipException
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.hitl import HITLBranchOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import TaskGroup, get_current_context, task

OPT_EXTRACT = "Extract"
OPT_SKIP = "Don't extract"


def ensure_extracted(id: int, file_paths: list[Path]) -> TaskGroup:
    extract_dag_id = f"extract_source{id}"

    with TaskGroup(group_id=f"ensure_extracted_{id}") as group:
        # task_id complet de check_file (préfixe du groupe inclus),
        # nécessaire pour le templating du body HITL
        check_task_id = f"{group.group_id}.check_file"

        # Path when all the files already exist
        file_present = EmptyOperator(task_id="file_present")

        # Path when the human accept to extract the source
        trigger_extract = TriggerDagRunOperator(
            task_id="trigger_extract",
            trigger_dag_id=extract_dag_id,
            conf={"id": id},
            wait_for_completion=True,
            deferrable=True,
            poke_interval=30,
            allowed_states=["success"],
            failed_states=["failed"],
        )

        # Path when the human refuse -> we skip the downstream tasks
        @task(task_id="refuse_extract")
        def refuse_extract():
            raise AirflowSkipException(
                f"Extraction of source {id} refused : downstream tasks will be skipped."
            )

        refuse = refuse_extract()

        # HITL : the human choose the branch
        # The bopdy is templated to dynamicly list the missing files from the XCom
        missing_tpl = (
            "{{ ti.xcom_pull(task_ids='" + check_task_id + "', key='missing_files')"
            " | join('\\n- ') }}"
        )
        ask = HITLBranchOperator(
            task_id="ask_extraction",
            subject=f"Missing files for source {id}",
            body=(
                "The following files don't exist :\n\n"
                f"- {missing_tpl}\n\n"
                f"Trigger the DAG `{extract_dag_id}` ?"
            ),
            options=[OPT_EXTRACT, OPT_SKIP],
            options_mapping={
                OPT_EXTRACT: trigger_extract.task_id,
                OPT_SKIP: refuse.operator.task_id,  # ty: ignore[unresolved-attribute]
            },
        )

        # Jonction point between branches
        done = EmptyOperator(
            task_id="extraction_done",
            trigger_rule="none_failed_min_one_success",
        )

        # Initial branch : do all the files exist ?
        @task.branch(task_id="check_file")
        def check_file() -> str:
            missing = [str(p) for p in file_paths if not p.exists()]
            if not missing:
                return file_present.task_id

            get_current_context()["ti"].xcom_push(key="missing_files", value=missing)
            return ask.task_id

        check = check_file()

        check >> [file_present, ask]
        ask >> [trigger_extract, refuse]
        [file_present, trigger_extract, refuse] >> done

    return group

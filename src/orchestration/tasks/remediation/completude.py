from pathlib import Path

from airflow.sdk import task

from orchestration.tasks.enums import FileType
from orchestration.tasks.models import DfReader, DfWriter
from remediation.completude import mice_imputation


@task()
def completude_remediation(cfg: dict[str, list[str]], data_dir: Path) -> None:
    for name, columns in cfg.items():
        df = DfReader(data_dir / f"{name}.parquet", FileType.PARQUET).read()

        DfWriter(data_dir / f"{name}.parquet", FileType.PARQUET).write(
            mice_imputation(df, columns)
        )

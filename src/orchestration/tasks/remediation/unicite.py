from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import polars as pl
from airflow.sdk import task

from orchestration.tasks.load.helpers import lazy_union_dyn
from orchestration.tasks.models import DfReader, DfWriter
from remediation.unicite import deduplicate


@dataclass(frozen=True, eq=False)
class DeduplicationCfg:
    epsilon: float = 0.05
    max_lhs: int = 3
    row_id_col: str | None = None
    only_flagged: bool = True
    order_by: str | None = None
    order_descending: bool = True


type UNICITE_REM_INPUT = Sequence[
    tuple[Sequence[DfReader], DfReader, DfWriter, str, DeduplicationCfg]
]


@task()
def unicite_remediation(cfg: UNICITE_REM_INPUT, data_dir: Path) -> None:
    for df_rs, df_rerr, df_w, source, dedup_cfg in cfg:
        df_r_union = lazy_union_dyn(list(df_rs), data_dir)

        dedup_res = deduplicate(
            pl.concat((df_r.read() for df_r in df_r_union), how="vertical"),
            df_rerr.read(),
            source,
            **vars(dedup_cfg),
        )

        print(
            f"\n============================\nDFA kept for the source '{source}' :\n{'\t\n'.join(str(dfa) for dfa in dedup_res.dfas)}"
            "\n============================\n\n"
        )

        if len(dedup_res.dfas) > 0:
            df_w.write(dedup_res.deduped)

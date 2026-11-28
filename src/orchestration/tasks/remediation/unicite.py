from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path

from airflow.sdk import task
from polars import DataFrame

from orchestration.tasks.load.helpers import union_dyn
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
    tuple[
        Sequence[DfReader],
        DfReader,
        DfWriter,
        str,
        DeduplicationCfg,
        list[Callable[[DataFrame], DataFrame] | None],
    ]
]


@task()
def unicite_remediation(cfg: UNICITE_REM_INPUT, data_dir: Path) -> None:
    for df_rs, df_rerr, df_w, source, dedup_cfg, funcs in cfg:
        sources = []
        for index, fn in enumerate(funcs):
            df_r = df_rs[index]
            src_name = df_r.path.stem

            if fn:
                sources.append((fn(df_r.read()), src_name))
            else:
                sources.append((df_r.read(), src_name))

        dedup_res = deduplicate(
            union_dyn(sources, data_dir),
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

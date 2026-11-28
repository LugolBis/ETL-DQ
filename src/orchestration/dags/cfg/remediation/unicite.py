from collections.abc import Callable

import polars as pl
from polars import DataFrame

from orchestration.tasks.remediation.unicite import DeduplicationCfg

UNICITE_RAW_CFG_TYPE = list[
    tuple[
        list[str], str, DeduplicationCfg, list[None | Callable[[DataFrame], DataFrame]]
    ]
]

UNICITE_RAW_CFG: UNICITE_RAW_CFG_TYPE = [
    (["population1"], "unicite_rids", DeduplicationCfg(), [None]),
    (["population2"], "unicite_rids", DeduplicationCfg(), [None]),
    (["consommation1"], "unicite_rids", DeduplicationCfg(), [None]),
    (["consommation2"], "unicite_rids", DeduplicationCfg(), [None]),
    (["csp3"], "unicite_rids", DeduplicationCfg(), [None]),
    (
        ["population1", "population2"],
        "unicite_rids",
        DeduplicationCfg(),
        [
            lambda df: df.cast({"ID_Personne": pl.Utf8}),
            lambda df: df.cast({"CSP": pl.Utf8}),
        ],
    ),
]

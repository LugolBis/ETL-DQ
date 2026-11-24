from orchestration.tasks.remediation.unicite import DeduplicationCfg

UNICITE_RAW_CFG = [
    (["population1"], "unicite_rids", DeduplicationCfg()),
    (["population2"], "unicite_rids", DeduplicationCfg()),
    (["consommation1"], "unicite_rids", DeduplicationCfg()),
    (["consommation2"], "unicite_rids", DeduplicationCfg()),
    (["csp3"], "unicite_rids", DeduplicationCfg()),
    (["population1", "population2"], "unicite_rids", DeduplicationCfg()),
]

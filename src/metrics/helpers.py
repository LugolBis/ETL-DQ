def unique_alias(alias: str, existing: set[str]) -> str:
    while alias in existing:
        alias = f"_{alias}"

    return alias

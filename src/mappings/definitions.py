"""Project mappings declared as Python constants.

Depends on the `mappings` package only. `config/mappings.json` is generated from
`MAPPINGS` (`MAPPINGS.to_json(...)`); a test keeps both in sync.

Assumptions to double-check against the real files:
  * `columns` only lists what the DAGs use; other raw columns pass through staging.
  * Column types are inferred from how the DAGs use them (join keys, comparisons).
  * `extract_source2` was not available: its raw files are assumed to be
    `Source2_Population.csv` / `Source2_Consommation.csv`, CSV with a header.
"""

from pathlib import Path

from mappings import (
    Aggregate,
    Atom,
    Attribute,
    Column,
    Condition,
    ConjunctiveQuery,
    DataType,
    Derived,
    FileFormat,
    GlobalRelation,
    LavMapping,
    LinkEnd,
    LinkKind,
    MappingSet,
    Operator,
    RawFile,
    Replacement,
    SourceLink,
    SourceRelation,
)

DATA_DIR = Path("/opt/airflow/.data")

BIGINT, DOUBLE, VARCHAR = DataType.BIGINT, DataType.DOUBLE, DataType.VARCHAR


def _csv(name: str) -> RawFile:
    return RawFile(f"{name}.csv", FileFormat.CSV, {"has_header": True})


# Global schema
CONSOMMATION_CSP = GlobalRelation(
    "Consommation_CSP",
    (
        Attribute("ID_CSP", BIGINT, "Identifiant de la catégorie socioprofessionnelle"),
        Attribute("Salaire_Moyen", BIGINT, "Salaire moyen associé à la catégorie"),
        Attribute("Conso_moyenne_annuelle", DOUBLE, "Consommation annuelle moyenne"),
    ),
    "Données de consommation énergétique agrégées par catégorie "
    "socioprofessionnelle (CSP) et niveau de salaire.",
)

CONSOMMATION_IRIS_PARIS = GlobalRelation(
    "Consommation_IRIS_Paris",
    (
        Attribute("ID_Iris", VARCHAR, "Identifiant de la zone géographique IRIS"),
        Attribute("Conso_moyenne_annuelle", DOUBLE, "Consommation annuelle moyenne"),
    ),
    "Données de consommation énergétique agrégées par zone géographique IRIS à Paris.",
)

CONSOMMATION_IRIS_EVRY = GlobalRelation(
    "Consommation_IRIS_Evry",
    (
        Attribute("ID_Iris", VARCHAR, "Identifiant de la zone géographique IRIS"),
        Attribute("Conso_moyenne_annuelle", DOUBLE, "Consommation annuelle moyenne"),
    ),
    "Données de consommation énergétique agrégées par zone géographique IRIS à Évry.",
)

# Source relations
SOURCE1_POPULATION = SourceRelation(
    name="Source1_Population",
    source="source1",
    raw=_csv("Source1_Population"),
    columns=(
        Column(
            "Adresse", VARCHAR, description="Adresse complète, parsée en N / Nom_Rue"
        ),
        Column("CSP", VARCHAR, description="Libellé de la CSP (tags séparés par '/')"),
    ),
)

SOURCE1_CONSOMMATION = SourceRelation(
    name="Source1_Consommation",
    source="source1",
    raw=_csv("Source1_Consommation"),
    columns=(
        Column("N", BIGINT),
        Column("Nom_Rue", VARCHAR),
        Column("Code_Postal", BIGINT),
        Column("NB_KW_Jour", DOUBLE),
    ),
)

SOURCE2_POPULATION = SourceRelation(
    name="Source2_Population",
    source="source2",
    raw=_csv("Source2_Population"),
    columns=(
        Column(
            "Adresse",
            VARCHAR,
            description="Adresse complète, parsée en N / Nom_Rue / Code_Postal",
        ),
        Column("CSP", BIGINT, description="Identifiant de la CSP"),
    ),
)

SOURCE2_CONSOMMATION = SourceRelation(
    name="Source2_Consommation",
    source="source2",
    raw=_csv("Source2_Consommation"),
    columns=(
        Column("N", BIGINT),
        Column("Nom_Rue", VARCHAR),
        Column("Code_Postal", VARCHAR),
        Column("NB_KW_Jour", DOUBLE),
    ),
)

SOURCE3_CSP = SourceRelation(
    name="Source3_CSP",
    source="source3",
    raw=_csv("Source3_CSP"),
    columns=(
        Column("ID_CSP", BIGINT),
        Column("Desc", VARCHAR, description="Libellé de la CSP"),
        Column("Salaire_Min", BIGINT, cast=True, cleanup=(Replacement(" ", ""),)),
        Column("Salaire_Max", BIGINT, cast=True, cleanup=(Replacement(" ", ""),)),
        Column("Salaire_Moyen", BIGINT),
    ),
)

SOURCE4_IRIS = SourceRelation(
    name="Source4_IRIS",
    source="source4",
    raw=_csv("Source4_IRIS"),
    columns=(
        Column("ID_Iris", VARCHAR, source="ID_IRIS"),
        Column("ID_Rue", VARCHAR),
        Column("ID_Ville", VARCHAR),
    ),
)


# LAV mappings: source relation ⊆ union of conjunctive queries over the global schema
ANNUAL_KW = Derived(("NB_KW_Jour",), "NB_KW_Jour * 365", Aggregate.AVG)

MAPPINGS_LAV = (
    LavMapping(
        "Source1_Population",
        (ConjunctiveQuery((Atom("Consommation_CSP"),)),),
    ),
    LavMapping(
        "Source1_Consommation",
        (
            ConjunctiveQuery(
                (Atom("Consommation_CSP", {"Conso_moyenne_annuelle": ANNUAL_KW}),)
            ),
            ConjunctiveQuery(
                (
                    Atom(
                        "Consommation_IRIS_Paris", {"Conso_moyenne_annuelle": ANNUAL_KW}
                    ),
                )
            ),
        ),
    ),
    LavMapping(
        "Source2_Population",
        (ConjunctiveQuery((Atom("Consommation_CSP", {"ID_CSP": "CSP"}),)),),
    ),
    LavMapping(
        "Source2_Consommation",
        (
            ConjunctiveQuery(
                (Atom("Consommation_CSP", {"Conso_moyenne_annuelle": ANNUAL_KW}),)
            ),
            ConjunctiveQuery(
                (Atom("Consommation_IRIS_Evry", {"Conso_moyenne_annuelle": ANNUAL_KW}),)
            ),
        ),
    ),
    LavMapping(
        "Source3_CSP",
        (
            ConjunctiveQuery(
                (
                    Atom(
                        "Consommation_CSP",
                        {"ID_CSP": "ID_CSP", "Salaire_Moyen": "Salaire_Moyen"},
                    ),
                )
            ),
        ),
    ),
    LavMapping(
        "Source4_IRIS",
        (
            ConjunctiveQuery(
                (Atom("Consommation_IRIS_Paris", {"ID_Iris": "ID_Iris"}),),
                (Condition("ID_Ville", Operator.EQ, "Paris"),),
            ),
            ConjunctiveQuery((Atom("Consommation_IRIS_Evry", {"ID_Iris": "ID_Iris"}),)),
        ),
        description="Côté Évry, la jointure actuelle ne porte que sur le nom de rue : "
        "aucune condition sur ID_Ville n'est exprimée.",
    ),
)


# Links between source relations (joins used by the transformations)
LINKS = (
    SourceLink(
        "pop1_conso1",
        LinkEnd("Source1_Population", ("Adresse",)),
        LinkEnd("Source1_Consommation", ("N", "Nom_Rue")),
        LinkKind.EXTRACTS,
        "N et Nom_Rue sont extraits de l'adresse (le code postal n'intervient pas).",
    ),
    SourceLink(
        "csp3_pop1",
        LinkEnd("Source3_CSP", ("Desc",)),
        LinkEnd("Source1_Population", ("CSP",)),
        LinkKind.CONTAINS,
        "Chaque tag de CSP (séparés par '/') doit figurer dans Desc, sans tenir compte de la casse.",
    ),
    SourceLink(
        "pop2_conso2",
        LinkEnd("Source2_Population", ("Adresse",)),
        LinkEnd("Source2_Consommation", ("N", "Nom_Rue", "Code_Postal")),
        LinkKind.EXTRACTS,
    ),
    SourceLink(
        "csp3_pop2",
        LinkEnd("Source3_CSP", ("ID_CSP",)),
        LinkEnd("Source2_Population", ("CSP",)),
        LinkKind.EQUALS,
    ),
    SourceLink(
        "conso1_iris4",
        LinkEnd("Source1_Consommation", ("Nom_Rue", "Code_Postal")),
        LinkEnd("Source4_IRIS", ("ID_Rue", "ID_Ville")),
        LinkKind.EQUALS,
        "Un Code_Postal entre 75000 et 75999 est ramené à 'Paris' avant la comparaison.",
    ),
    SourceLink(
        "conso2_iris4",
        LinkEnd("Source2_Consommation", ("Nom_Rue",)),
        LinkEnd("Source4_IRIS", ("ID_Rue",)),
        LinkKind.CONTAINS,
        "Chaque tag de ID_Rue (séparés par '/') doit figurer dans Nom_Rue, sans tenir compte de la casse.",
    ),
)

MAPPINGS = MappingSet(
    global_relations=(
        CONSOMMATION_CSP,
        CONSOMMATION_IRIS_PARIS,
        CONSOMMATION_IRIS_EVRY,
    ),
    source_relations=(
        SOURCE1_POPULATION,
        SOURCE1_CONSOMMATION,
        SOURCE2_POPULATION,
        SOURCE2_CONSOMMATION,
        SOURCE3_CSP,
        SOURCE4_IRIS,
    ),
    mappings=MAPPINGS_LAV,
    links=LINKS,
    data_dir=DATA_DIR,
)

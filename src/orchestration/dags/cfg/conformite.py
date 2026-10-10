from orchestration.tasks.data_quality.conformite import (
    CONFORMITE_FK_TYPE,
    CONFORMITE_FMT_TYPE,
    CONFORMITE_INTERVAL_TYPE,
    CONFORMITE_LABEL_TYPE,
)

ADRESSE_REGEX = r"^\d{1,4}\s+(?:[A-Za-zÀ-ÿ'’\-\.]+\s+){1,5}(?:rue|avenue|boulevard|chemin|route|impasse|place|allée|quai|square|passage|voie|hameau|lieu-dit|résidence|lotissement|zone|ZAC|ZA|ZI|villa|cité|rond-point|carrefour|esplanade|promenade|sentier|traverse|vallon|clos|domaine|mas|bastide|château|moulin|ferme|grange|hôtel)[a-zÀ-ÿ\-\.']*\s+\d{5}\s+[A-Za-zÀ-ÿ\-\.']+$"

CSP_CATEGORIES = [
    "employés",
    "agriculteurs exploitants",
    "cadres et professions intellectuelles supérieures",
    "professions intermédiaires",
    "retraités",
    "artisans, commerçants et chefs d’entreprise",
    "autres personnes sans activité professionnelle",
    "ouvriers",
]

CODES_POSTAUX = [
    91000,
    75006,
    75011,
    75020,
    75005,
    75008,
    75002,
    75001,
    75017,
    75012,
]

CONFORMITE_FK_CFG: CONFORMITE_FK_TYPE = {
    ("consommation1", "iris4"): [("Code_Postal", "ID_Ville")],
    ("consommation2", "iris4"): [("Code_Postal", "ID_Ville")],
    ("population1", "csp3"): [("CSP", "ID_CSP")],
    ("population2", "csp3"): [("CSP", "ID_CSP")],
}

# TODO : Readapt the REGEX to match the real data format
CONFORMITE_FMT_CFG: CONFORMITE_FMT_TYPE = {
    "population1": [(["Adresse"], ADRESSE_REGEX)],
    "population2": [(["Adresse"], ADRESSE_REGEX)],
    "consommation1": [(["ID_Adr"], "_"), (["Nom_Rue"], "_")],
    "consommation2": [(["ID_Adr"], "_"), (["Nom_Rue"], "_")],
    "iris4": [(["ID_Rue"], "_")],
}

# TODO : Readapt the Labels to match the real data format
CONFORMITE_LABEL_CFG: CONFORMITE_LABEL_TYPE = {
    "population1": (["CSP"], CSP_CATEGORIES),
    "population2": (["CSP"], CSP_CATEGORIES),
    "csp3": (["ID_CSP"], CSP_CATEGORIES),
    "consommation1": (["Code_Postal"], CODES_POSTAUX),
    "consommation2": (["Code_Postal"], CODES_POSTAUX),
    "iris4": (["ID_Ville"], CODES_POSTAUX),
}

CONFORMITE_INTERVAL_CFG: CONFORMITE_INTERVAL_TYPE = {
    "csp3": [("Salaire_Min", "Salaire_Moyen", "Salaire_Max")]
}

from orchestration.tasks.data_quality.conformite import (
    CONFORMITE_FK_ADRESSE_TYPE,
    CONFORMITE_FK_TYPE,
    CONFORMITE_FMT_TYPE,
    CONFORMITE_INTERVAL_TYPE,
    CONFORMITE_LABEL_TYPE,
)
from orchestration.tasks.transform.helpers import ADRESSE_EXTRACTIONS

# "Paris, 20 rue Arthur Groussier"
ADRESSE_PARIS_REGEX = r"^Paris, \d+ rue .+\S$"

# "Evry-Courcouronnes, 91000, 19 Boulevard Aguado" (the number can be "2bis")
ADRESSE_EVRY_REGEX = r"^Evry-Courcouronnes, \d{5}, \d+(bis|ter)? .+\S$"

CODE_POSTAL_REGEX = r"^\d{5}$"

# Street names are written in uppercase : "ARTHUR GROUSSIER"
NOM_RUE_REGEX = r"^[A-Z ]+$"

# Labels of the column 'Desc' of Source3_CSP (Paris uses the labels)
CSP_CATEGORIES = [
    "Agriculteurs exploitants / algricultrices exploitantes",
    "Artisans / artisanes, commerçants / commerçantes et chefs / cheffes d'entreprise",
    "Cadres et professions intellectuelles supérieures",
    "Professions intermédiaires",
    "Employés / employées",
    "Ouvriers / ouvrières",
]

# Values of the column 'ID_CSP' of Source3_CSP (Evry uses the codes)
CSP_IDS = [1, 2, 3, 4, 5, 6]

CODES_POSTAUX_PARIS = list(range(75001, 75021))

# 'Code_Postal' of Evry is read as String because of invalid values ('91000a')
CODES_POSTAUX_EVRY = ["91000"]

VILLES = ["Paris", "Evry-Courcouronnes"]

CONFORMITE_FK_CFG: CONFORMITE_FK_TYPE = {
    ("consommation1", "iris4"): [("Nom_Rue", "ID_Rue")],
    ("consommation2", "iris4"): [("Nom_Rue", "ID_Rue")],
    ("population2", "csp3"): [("CSP", "ID_CSP")],
}

# Paris writes the CSP in full text ("Cadre") : matched with `fuzzy_join` on 'Desc'
CONFORMITE_FK_FUZZY_CFG: CONFORMITE_FK_TYPE = {
    ("population1", "csp3"): [("CSP", "Desc")],
}

# The address is split with `expand_column` (same parameters as the DAG consommation_csp)
CONFORMITE_FK_ADRESSE_CFG: CONFORMITE_FK_ADRESSE_TYPE = {
    # "Paris, 20 rue Arthur Groussier" -> "Paris, 75000, 20 Arthur Groussier"
    # 75000 isn't the real postal code : the join is only on N + Nom_Rue
    ("population1", "consommation1"): (
        "Adresse",
        ADRESSE_EXTRACTIONS[:-1],
        [(r"^Paris,\s*", "Paris, 75000, "), ("(?i)rue", "")],
        ["N", "Nom_Rue"],
    ),
    ("population2", "consommation2"): (
        "Adresse",
        ADRESSE_EXTRACTIONS,
        [],
        ["N", "Nom_Rue", "Code_Postal"],
    ),
}

CONFORMITE_FMT_CFG: CONFORMITE_FMT_TYPE = {
    "population1": [(["Adresse"], ADRESSE_PARIS_REGEX)],
    "population2": [(["Adresse"], ADRESSE_EVRY_REGEX)],
    "consommation1": [
        (["ID_Adr"], r"^\d+[A-Za-z]+$"),  # "20AG", "8Bh"
        (["Nom_Rue"], NOM_RUE_REGEX),
        (["Code_Postal"], CODE_POSTAL_REGEX),
    ],
    "consommation2": [
        (["ID_Adr"], r"^\d{6}$"),  # "231901"
        (["Nom_Rue"], NOM_RUE_REGEX),
        (["Code_Postal"], CODE_POSTAL_REGEX),
    ],
    "iris4": [(["ID_Rue"], NOM_RUE_REGEX)],
}

CONFORMITE_LABEL_CFG: CONFORMITE_LABEL_TYPE = {
    "population1": (["CSP"], CSP_CATEGORIES),
    "population2": (["CSP"], CSP_IDS),
    "csp3": (["ID_CSP"], CSP_IDS),
    "consommation1": (["Code_Postal"], CODES_POSTAUX_PARIS),
    "consommation2": (["Code_Postal"], CODES_POSTAUX_EVRY),
    "iris4": (["ID_Ville"], VILLES),
}

CONFORMITE_INTERVAL_CFG: CONFORMITE_INTERVAL_TYPE = {
    "csp3": [("Salaire_Min", "Salaire_Moyen", "Salaire_Max")]
}

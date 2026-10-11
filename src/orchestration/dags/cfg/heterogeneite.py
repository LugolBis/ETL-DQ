
# Configuration des metriques d'heterogeneite
#
# Les sources correspondent aux fichiers Parquet presents
# dans le repertoire de donnees utilise par Airflow.


# 1. Coefficient de variation (CV)
#
# Structure :
# "nom_source": ["colonne1", "colonne2", ...]
#
# Le CV sera calcule pour chaque colonne indiquee.

HETEROGENEITE_CV_CFG = {
    "consommation1": ["NB_KW_Jour"],
    "consommation2": ["NB_KW_Jour"],
}


# 2. Difference de dispersion
#
# Structure :
# (
#     "source1",
#     "colonne1",
#     "source2",
#     "colonne2",
# )
#
# La metrique compare les coefficients de variation
# des deux sources selectionnees.

HETEROGENEITE_DISPERSION_CFG = [
    (
        "consommation1",
        "NB_KW_Jour",
        "consommation2",
        "NB_KW_Jour",
    ),
]


# 3. Ratio de position
#
# Structure :
# "nom_source": [
#     (
#         "colonne_min",
#         "colonne_moyen",
#         "colonne_max",
#         seuil_min,
#         seuil_max,
#     )
# ]
#
# Les seuils sont configurables.

HETEROGENEITE_RATIO_CFG = {
    "csp3": [
        (
            "Salaire_Min",
            "Salaire_Moyen",
            "Salaire_Max",
            0.1,
            0.9,
        ),
    ],
}

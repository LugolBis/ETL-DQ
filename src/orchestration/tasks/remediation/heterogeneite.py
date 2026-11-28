
from collections.abc import Sequence

from airflow.sdk import task

from orchestration.tasks.models import DfReader, DfWriter
from remediation.heterogeneite import (
    ameliorer_dispersion,
    ameliorer_ratio_position,
)


# Configuration des corrections de dispersion
type DISPERSION_REM_INPUT = Sequence[
    tuple[
        DfReader,        # Lecteur source 1
        DfReader,        # Lecteur source 2
        DfWriter,        # Fichier de sortie
        str,             # Colonne a corriger
        list[float],     # Facteurs connus
        float,           # Seuil de proximite
    ]
]


# Configuration des corrections du ratio de position
type RATIO_REM_INPUT = Sequence[
    tuple[
        DfReader,        # Lecteur des salaires
        DfWriter,        # Fichier de sortie
        str,             # Colonne a corriger
        list[float],     # Facteurs a tester
        float,           # Seuil de reparation
    ]
]


@task()
def heterogeneite_remediation(
    cfg_dispersion: DISPERSION_REM_INPUT,
    cfg_ratio: RATIO_REM_INPUT,
    autoriser_ecriture: bool = False,
) -> None:
    """
    Executer la remediation de l'heterogeneite.

    Par defaut, les corrections sont simulees :
    aucun fichier n'est ecrit.
    """

    # ========================================================
    # PARTIE 1 : DIFFERENCE DE DISPERSION
    # ========================================================

    for (
        lecteur_source1,
        lecteur_source2,
        ecrivain,
        colonne,
        facteurs_connus,
        seuil_proximite,
    ) in cfg_dispersion:

        # Lire les deux sources
        df_source1 = lecteur_source1.read()
        df_source2 = lecteur_source2.read()

        # Appeler notre algorithme
        df_corrige, methode = ameliorer_dispersion(
            df_source1,
            df_source2,
            colonne,
            facteurs_connus,
            seuil_proximite,
        )

        print(f"Dispersion - colonne : {colonne}")
        print(f"Methode proposee : {methode}")

        # Ecrire uniquement si l'ecriture est autorisee
        if autoriser_ecriture and methode != "aucune_correction":
            ecrivain.write(df_corrige)
            print("Fichier corrige enregistre.")
        else:
            print("Aucune ecriture effectuee.")

    # ========================================================
    # PARTIE 2 : RATIO DE POSITION
    # ========================================================

    for (
        lecteur_salaires,
        ecrivain,
        colonne,
        facteurs,
        seuil_reparation,
    ) in cfg_ratio:

        # Lire les salaires
        df_salaires = lecteur_salaires.read()

        # Appeler notre algorithme
        df_corrige, methode, nombre, total = ameliorer_ratio_position(
            df_salaires,
            colonne,
            facteurs,
            seuil_reparation,
        )

        print(f"Ratio de position - colonne : {colonne}")
        print(f"Methode proposee : {methode}")
        print(f"Lignes ameliorables : {nombre} / {total}")

        # Ecrire uniquement si l'ecriture est autorisee
        if autoriser_ecriture and methode != "aucune_correction":
            ecrivain.write(df_corrige)
            print("Fichier corrige enregistre.")
        else:
            print("Aucune ecriture effectuee.")

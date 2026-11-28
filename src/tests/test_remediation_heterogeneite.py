
import polars as pl
import pytest

from remediation.heterogeneite import (
    calculer_quantiles,
    estimer_theil_sen,
    choisir_transformation,
    appliquer_transformation,
    ameliorer_dispersion,
    reperer_ratios_atypiques,
    tester_conversion_rp as verifier_conversion_rp,
    choisir_facteur_rp,
    appliquer_facteur_rp,
    ameliorer_ratio_position,
)


class TestRemediationHeterogeneite:

    # ========================================================
    # PARTIE 1 : TESTS DE LA DISPERSION
    # ========================================================

    # 1. Tester le calcul des quantiles
    def test_calculer_quantiles(self):
        df = pl.DataFrame({
            "NB_KW_Jour": [10, 20, 30, 40, 50]
        })

        quantiles = calculer_quantiles(df, "NB_KW_Jour")

        assert len(quantiles) == 99
        assert quantiles[49] == pytest.approx(30.0)

    # 2. Tester la regression Theil-Sen
    def test_estimer_theil_sen(self):
        paris = pl.DataFrame({
            "NB_KW_Jour": [10, 20, 30, 40, 50]
        })

        evry = pl.DataFrame({
            "NB_KW_Jour": [100, 200, 300, 400, 500]
        })

        a, b, r2 = estimer_theil_sen(
            paris,
            evry,
            "NB_KW_Jour",
        )

        assert a == pytest.approx(10.0, rel=0.01)
        assert b == pytest.approx(0.0, abs=1.0)
        assert r2 > 0.99

    # 3. Tester le choix d'un facteur connu
    def test_choisir_facteur_connu(self):
        facteur, decalage, methode = choisir_transformation(
            a=10.0,
            b=2.0,
            r2=1.0,
            facteurs_connus=[10.0],
            seuil_proximite=0.05,
        )

        assert facteur == 10.0
        assert decalage == 0.0
        assert methode == "facteur_connu"

    # 4. Tester le refus d'une transformation
    def test_refuser_transformation(self):
        facteur, decalage, methode = choisir_transformation(
            a=2.0,
            b=3.0,
            r2=0.2,
            facteurs_connus=[],
            seuil_proximite=0.05,
        )

        assert facteur == 1.0
        assert decalage == 0.0
        assert methode == "aucune_correction"

    # 5. Tester l'application d'une transformation
    def test_appliquer_transformation(self):
        df = pl.DataFrame({
            "NB_KW_Jour": [10, 20, 30]
        })

        corrige = appliquer_transformation(
            df,
            "NB_KW_Jour",
            10,
            0,
        )

        assert corrige["NB_KW_Jour"].to_list() == [
            100.0,
            200.0,
            300.0,
        ]

        # Verifier que les donnees originales sont inchangees
        assert df["NB_KW_Jour"].to_list() == [
            10,
            20,
            30,
        ]

    # 6. Tester le processus complet de dispersion
    def test_ameliorer_dispersion(self):
        paris = pl.DataFrame({
            "NB_KW_Jour": [10, 20, 30, 40, 50]
        })

        evry = pl.DataFrame({
            "NB_KW_Jour": [100, 200, 300, 400, 500]
        })

        corrige, methode = ameliorer_dispersion(
            paris,
            evry,
            "NB_KW_Jour",
            facteurs_connus=[10.0],
            seuil_proximite=0.05,
        )

        assert methode == "facteur_connu"

        assert corrige["NB_KW_Jour"].to_list() == [
            100.0,
            200.0,
            300.0,
            400.0,
            500.0,
        ]

    # ========================================================
    # PARTIE 2 : TESTS DU RATIO DE POSITION
    # ========================================================

    # 7. Tester la detection des ratios atypiques
    def test_reperer_ratios_atypiques(self):
        df = pl.DataFrame({
            "Salaire_Min": [1500, 1500, 1500],
            "Salaire_Moyen": [1600, 2500, None],
            "Salaire_Max": [24000, 3500, 3500],
        })

        atypiques = reperer_ratios_atypiques(df)

        assert atypiques.height == 1
        assert atypiques["row_id"].to_list() == [0]

    # 8. Tester une conversion du ratio de position
    def test_tester_conversion_rp(self):
        resultat = verifier_conversion_rp(
            1500,
            1600,
            24000,
            "Salaire_Max",
            12,
            "diviser",
        )

        assert resultat is True

    # 9. Tester le choix d'un facteur commun
    def test_choisir_facteur_rp(self):
        df = pl.DataFrame({
            "Salaire_Min": [1500, 1500, 1500],
            "Salaire_Moyen": [1600, 1700, 2500],
            "Salaire_Max": [24000, 24000, 3500],
        })

        resultat = choisir_facteur_rp(
            df,
            "Salaire_Max",
            [12, 10, 30],
            0.5,
        )

        assert resultat == (12, "diviser", 2, 2)

    # 10. Tester l'application d'un facteur
    def test_appliquer_facteur_rp(self):
        df = pl.DataFrame({
            "Salaire_Min": [1500, 1500, 1500],
            "Salaire_Moyen": [1600, 1700, 2500],
            "Salaire_Max": [24000, 24000, 3500],
        })

        corrige = appliquer_facteur_rp(
            df,
            "Salaire_Max",
            12,
            "diviser",
        )

        assert corrige["Salaire_Max"].to_list() == [
            2000.0,
            2000.0,
            3500.0,
        ]

        # Verifier que les donnees originales sont inchangees
        assert df["Salaire_Max"].to_list() == [
            24000,
            24000,
            3500,
        ]

    # 11. Tester le processus complet du ratio de position
    def test_ameliorer_ratio_position(self):
        df = pl.DataFrame({
            "Salaire_Min": [1500, 1500, 1500],
            "Salaire_Moyen": [1600, 1700, 2500],
            "Salaire_Max": [24000, 24000, 3500],
        })

        corrige, methode, nombre, total = (
            ameliorer_ratio_position(
                df,
                "Salaire_Max",
                [12, 10, 30],
            )
        )

        assert methode == "diviser_par_12"
        assert nombre == 2
        assert total == 2

        assert corrige["Salaire_Max"].to_list() == [
            2000.0,
            2000.0,
            3500.0,
        ]

    # 12. Tester le refus d'une correction insuffisante
    def test_refuser_correction_rp(self):
        df = pl.DataFrame({
            "Salaire_Min": [1500, 1500, 1500],
            "Salaire_Moyen": [1600, 1550, 1700],
            "Salaire_Max": [24000, 30000, 24000],
        })

        corrige, methode, nombre, total = (
            ameliorer_ratio_position(
                df,
                "Salaire_Max",
                [12],
                seuil_reparation=0.75,
            )
        )

        assert methode == "aucune_correction"
        assert nombre == 2
        assert total == 3
        assert corrige.equals(df)

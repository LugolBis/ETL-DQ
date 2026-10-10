
import math

import polars as pl
import pytest

from metrics.heterogeneite import (
    calculer_cv,
    calculer_difference_dispersion,
    calculer_ratio_position,
)


class TestHeterogeneite:

    def test_calculer_cv(self):
        df = pl.DataFrame({
            "NB_KW_Jour": [10.0, 20.0, 30.0]
        })
        assert calculer_cv(df) == pytest.approx(0.5)

    def test_difference_dispersion_identique(self):
        assert calculer_difference_dispersion(0.5, 0.5) == pytest.approx(0.0)

    def test_difference_dispersion(self):
        assert calculer_difference_dispersion(0.5, 1.0) == pytest.approx(
            math.log(2)
        )

    def test_ratio_position_valide(self):
        df = pl.DataFrame({
            "Salaire_Min": [1000.0, 2000.0],
            "Salaire_Moyen": [1500.0, 2500.0],
            "Salaire_Max": [2000.0, 3000.0],
        })
        assert calculer_ratio_position(df) == pytest.approx(0.0)

    def test_ratio_position_atypique(self):
        df = pl.DataFrame({
            "Salaire_Min": [1000.0, 1000.0],
            "Salaire_Moyen": [1050.0, 1500.0],
            "Salaire_Max": [2000.0, 2000.0],
        })
        assert calculer_ratio_position(df) == pytest.approx(0.5)

    def test_ratio_position_salaires_invalides(self):
        df = pl.DataFrame({
            "Salaire_Min": [2000.0],
            "Salaire_Moyen": [1500.0],
            "Salaire_Max": [3000.0],
        })
        with pytest.raises(ValueError):
            calculer_ratio_position(df)

    def test_ratio_position_tableau_vide(self):
        df = pl.DataFrame({
            "Salaire_Min": [],
            "Salaire_Moyen": [],
            "Salaire_Max": [],
        })
        with pytest.raises(ValueError):
            calculer_ratio_position(df)

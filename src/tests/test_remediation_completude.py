import polars as pl

from remediation.completude import mice_imputation


class TestRemediationCompletude:
    def test_mice_imputation(self):
        df = pl.DataFrame(
            {
                "Salaire_Moyen": [24780, 39860, 24450, None],
                "Salaire_Min": ["16 800", "27 900", "17 100", "16 830"],
                "Salaire_Max": ["36 000", "59 800", "36 700", "36 060"],
            }
        )

        df_res = mice_imputation(df, ["Salaire_Moyen", "Salaire_Min", "Salaire_Max"])

        # La case vide est remplie, avec une valeur entre le min et le max
        assert df_res["Salaire_Moyen"].null_count() == 0
        assert 16830 <= df_res["Salaire_Moyen"][3] <= 36060

        # Les valeurs déjà connues et les types ne changent pas
        assert df_res["Salaire_Moyen"].to_list()[:3] == [24780, 39860, 24450]
        assert df_res["Salaire_Min"].to_list() == df["Salaire_Min"].to_list()
        assert df_res.schema == df.schema

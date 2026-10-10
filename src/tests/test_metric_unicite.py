from metrics.unicite import compute_unicite
import polars as pl

class TestUnicite:
    def test_no_duplicates(self):
        df = pl.DataFrame({
            "Prénom": ["Loïc", "Cyrus", "Mohamed Anis", "Ahmad"],
            "Nom": ["DESMARÈS", "NOUKPOZOUNKOU", "YAICI", "HATOUM"]
        })
        columns = ["Prénom", "Nom"]
        result = compute_unicite(df, columns, "test_source")

        assert result["unicite_percent"].to_list() == [1.0]
        assert result["column_name"].to_list() == ["Prénom, Nom"]

    def test_with_duplicates_single_column(self):
        df = pl.DataFrame({
            "Prénom": ["Loïc", "Cyrus", "Mohamed Anis", "Ahmad", "Ahmad"],
            "Nom": ["DESMARÈS", "NOUKPOZOUNKOU", "YAICI", "HATOUM", "BLABLA"]
        })
        columns = ["Prénom"]
        result = compute_unicite(df, columns, "test_source")

        assert result["column_name"].to_list() == ["Prénom"]
        assert result["unicite_percent"].to_list() == [0.6]

    def test_with_duplicates_multiple_columns(self):
        df = pl.DataFrame({
            "Prénom": ["Loïc", "Cyrus", "Mohamed Anis", "Ahmad", "Ahmad"],
            "Nom": ["DESMARÈS", "NOUKPOZOUNKOU", "YAICI", "HATOUM", "HATOUM"]
        })
        columns = ["Prénom", "Nom"]
        result = compute_unicite(df, columns, "test_source")

        assert result["column_name"].to_list() == ["Prénom, Nom"]
        assert result["unicite_percent"].to_list() == [0.6]

    def test_empty_dataframe(self):
        df = pl.DataFrame({
            "Prénom": [],
            "Nom": []
        })
        columns = ["Prénom", "Nom"]
        result = compute_unicite(df, columns, "test_source")

        assert result["column_name"].to_list() == ["Prénom, Nom"]
        assert result["unicite_percent"].to_list() == [None]

    def test_duplicate_across_sources(self):
        df_1 = pl.DataFrame({
            "Nom": ["Michel", "Dupont"],
            "Prénom": ["Alice", "Marie"],
            "Adresse": ["PAR019", "PAR001"],
        })
        df_2 = pl.DataFrame({
            "Nom": ["Michel", "Martin"],
            "Prénom": ["Alice", "Paul"],
            "Adresse": ["EVR003", "EVR001"],
        })

        df_final = pl.concat([df_1, df_2])

        result_nom_prenom = compute_unicite(df_final, ["Nom", "Prénom"], "test_source")
        result_nom_prenom_adresse = compute_unicite(df_final, ["Nom", "Prénom", "Adresse"], "test_source")

        assert result_nom_prenom["column_name"].to_list() == ["Nom, Prénom"]
        assert result_nom_prenom["unicite_percent"].to_list() == [0.5]

        assert result_nom_prenom_adresse["column_name"].to_list() == ["Nom, Prénom, Adresse"]
        assert result_nom_prenom_adresse["unicite_percent"].to_list() == [1.0]

    def test_duplicates_on_subset_of_columns(self):
        df = pl.DataFrame({
            "N": [41, 41, 15],
            "Nom_Rue": ["Rue de Rivoli", "Rue de Rivoli", "Rue de la Paix"],
            "Code_Postal": [75001, 75001, 75002],
            "NB_KW_Jour": [6.3, 9.8, 6.9],
        })
        result = compute_unicite(df, ["N", "Nom_Rue", "Code_Postal"], "consommation")

        assert result["column_name"].to_list() == ["N, Nom_Rue, Code_Postal"]
        assert result["unicite_percent"].to_list() == [1 / 3]
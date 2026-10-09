from datetime import UTC

import polars as pl
import pytest
from polars import DataFrame

from metrics.conformite import text_format

EMAIL_REGEX = r"^[\w.+-]+@[\w-]+\.[\w.-]+$"
CODE_POSTAL_FR_REGEX = r"^(0[1-9]|[1-8]\d|9[0-5])\d{3}$"
TEL_FR_REGEX = r"^(?:\+33|0)[1-9]\d{8}$"
DATE_ISO_REGEX = (
    r"^[0-9]{4}-"
    r"(?:"
    r"(?:0[13578]|1[02])-(?:0[1-9]|[12][0-9]|3[01])"
    r"|"
    r"(?:0[469]|11)-(?:0[1-9]|[12][0-9]|30)"
    r"|"
    r"02-(?:0[1-9]|1[0-9]|2[0-8])"
    r")"
    r"$"
)
SIRET_REGEX = r"^\d{14}$"


@pytest.fixture
def df() -> DataFrame:
    return DataFrame(
        {
            "email": [
                "alice.dupont@example.com",  # OK
                "bob.martin@example.fr",  # OK
                "charlie+news@sub.example.org",  # OK
                "invalid-email",  # KO
                "daniel@example",  # KO (pas de TLD)
                "eve@Example.COM",  # OK
                "frank @example.com",  # KO (espace)
                None,  # KO
                "grace@example.com",  # OK
                "henri@@example.com",  # KO
            ],
            "code_postal": [
                "75001",  # OK (Paris)
                "13001",  # OK (Marseille)
                "69001",  # OK (Lyon)
                "01000",  # OK (Bourg-en-Bresse)
                "96000",  # KO (96xxx n'existe pas)
                "2A004",  # KO (Corse, format non numérique)
                "00000",  # KO
                "31000",  # OK (Toulouse)
                None,  # KO
                "4400",  # KO (4 chiffres)
            ],
            "telephone": [
                "+33612345678",  # OK
                "0612345678",  # OK
                "+33712345678",  # OK
                "0712345678",  # OK
                "0123456789",  # OK
                "+3301234567",  # KO (trop court)
                "061234567",  # KO (9 chiffres)
                "06 12 34 56 78",  # KO (espaces)
                None,  # KO
                "+3361234567",  # KO
            ],
            "date_naissance": [
                "1985-06-15",  # OK
                "1990-12-31",  # OK
                "2000-01-01",  # OK
                "1975-02-30",  # KO (30 février)
                "1988-13-01",  # KO (mois 13)
                "1999-00-10",  # KO (mois 00)
                "2001-11-07",  # OK
                "15/06/1985",  # KO (mauvais format)
                None,  # KO
                "1980-04-22",  # OK
            ],
            "siret": [
                "73282932000074",  # OK
                "40483304800017",  # OK
                "55210055400013",  # OK
                "1234567890123",  # KO (13 chiffres)
                "123456789012345",  # KO (15 chiffres)
                "ABCDEFGHIJKLMN",  # KO
                None,  # KO
                "00000000000000",  # OK (14 chiffres)
                "12345 67890 123",  # KO (espaces)
                "98765432109876",  # OK
            ],
        }
    )


class TestConformiteTextFormat:
    def test_text_format_schema_and_shape(self, df: DataFrame) -> None:
        result = text_format(
            df,
            ["email", "code_postal", "telephone", "date_naissance", "siret"],
            EMAIL_REGEX,
            "annuaire_clients",
        )

        assert set(result.columns) == {
            "source",
            "column_name",
            "format_percent",
            "timestamp",
        }
        print(result)
        assert result.height == 5
        assert result["source"].to_list() == ["annuaire_clients"] * 5
        assert result["column_name"].to_list() == [
            "email",
            "code_postal",
            "telephone",
            "date_naissance",
            "siret",
        ]

    def test_text_format_email(self, df: DataFrame) -> None:
        result = text_format(df, ["email"], EMAIL_REGEX, "annuaire_clients")

        # 5 emails conformes sur 10 : alice, bob, charlie, eve, grace
        assert result["format_percent"].to_list() == pytest.approx([0.5])

    def test_text_format_code_postal(self, df: DataFrame) -> None:
        result = text_format(
            df, ["code_postal"], CODE_POSTAL_FR_REGEX, "annuaire_clients"
        )

        # 5 codes postaux conformes : 75001, 13001, 69001, 01000, 31000
        assert result["format_percent"].to_list() == pytest.approx([0.5])

    def test_text_format_telephone(self, df: DataFrame) -> None:
        result = text_format(df, ["telephone"], TEL_FR_REGEX, "annuaire_clients")

        # 5 téléphones conformes : +336..., 0612345678, +337..., 0712345678, 0123456789
        assert result["format_percent"].to_list() == pytest.approx([0.5])

    def test_text_format_date_naissance(self, df: DataFrame) -> None:
        result = text_format(df, ["date_naissance"], DATE_ISO_REGEX, "annuaire_clients")

        # 5 dates conformes : 1985-06-15, 1990-12-31, 2000-01-01, 2001-11-07, 1980-04-22
        assert result["format_percent"].to_list() == pytest.approx([0.5])

    def test_text_format_siret(self, df: DataFrame) -> None:
        result = text_format(df, ["siret"], SIRET_REGEX, "annuaire_clients")

        # 5 SIRET conformes : 73282932000074, 40483304800017,
        #                     55210055400013, 00000000000000, 98765432109876
        assert result["format_percent"].to_list() == pytest.approx([0.5])

    def test_text_format_toutes_colonnes(self, df: DataFrame) -> None:
        result = text_format(
            df,
            ["email", "code_postal", "telephone", "date_naissance", "siret"],
            EMAIL_REGEX,
            "annuaire_clients",
        )

        per_column = dict(
            zip(
                result["column_name"].to_list(),
                result["format_percent"].to_list(),
            )
        )

        # Chaque colonne a exactement 5 valeurs conformes sur 10, mais avec
        # des regex différentes appliquées à la même colonne on change le résultat.
        # Ici, on n'applique qu'EMAIL_REGEX → seule la colonne email doit valoir 0.5.
        assert per_column["email"] == pytest.approx(0.5)
        # Les autres colonnes ne contiennent aucun email → 0.0
        for col in ["code_postal", "telephone", "date_naissance", "siret"]:
            assert per_column[col] == pytest.approx(0.0)

    def test_text_format_timestamp_utc(self, df: DataFrame) -> None:
        result = text_format(df, ["email"], EMAIL_REGEX, "annuaire_clients")

        timestamps = result["timestamp"].to_list()
        assert len(set(timestamps)) == 1
        assert timestamps[0].tzinfo is not None
        assert timestamps[0].utcoffset() == UTC.utcoffset(None)

    def test_text_format_nulls_counted_as_non_matching(self, df: DataFrame) -> None:
        """Un None sur une colonne donnée doit compter comme non conforme."""
        result = text_format(df, ["date_naissance"], DATE_ISO_REGEX, "annuaire_clients")

        n = df.height
        non_null_ok = (
            df["date_naissance"].drop_nulls().str.contains(DATE_ISO_REGEX).sum()  # ty: ignore[invalid-argument-type]
        )
        expected = non_null_ok / n
        assert result["format_percent"].to_list() == pytest.approx([expected])

    def test_text_format_colonne_vide(self) -> None:
        df = DataFrame({"email": []}, schema={"email": pl.String})

        result = text_format(df, ["email"], EMAIL_REGEX, "annuaire_clients")

        assert result.height == 1
        assert result["column_name"].to_list() == ["email"]

    def test_text_format_aucune_colonne(self, df: DataFrame) -> None:
        result = text_format(df, [], EMAIL_REGEX, "annuaire_clients")

        assert result.height == 0
        assert set(result.columns) == {
            "source",
            "column_name",
            "format_percent",
            "timestamp",
        }

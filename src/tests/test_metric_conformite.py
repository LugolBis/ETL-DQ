from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import polars as pl
import pytest
from polars import DataFrame

from metrics.conformite import (
    foreign_key_validity,
    interval_validity,
    label_set,
    text_format,
)

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


class TestConformiteLabels:
    def test_basic_percentages(self):
        df = pl.DataFrame(
            {
                "a": ["x", "y", "x", "z"],
                "b": ["y", "y", "w", "x"],
            }
        )
        result = label_set(df, columns=["a", "b"], labels=["x", "y"], source="src")

        assert result["column_name"].to_list() == ["a", "b"]
        assert result["labels_percent"].to_list() == [0.75, 0.75]

    def test_all_values_match(self):
        df = pl.DataFrame({"a": ["x", "x", "x"]})
        result = label_set(df, ["a"], ["x"], "s")
        assert result["labels_percent"].to_list() == [1.0]

    def test_no_value_matches(self):
        df = pl.DataFrame({"a": ["x", "y", "z"]})
        result = label_set(df, ["a"], ["w"], "s")
        assert result["labels_percent"].to_list() == [0.0]

    def test_single_row(self):
        df = pl.DataFrame({"a": ["x"]})
        result = label_set(df, ["a"], ["x"], "s")
        assert result["labels_percent"].to_list() == [1.0]

    def test_multiple_labels(self):
        df = pl.DataFrame({"a": ["cat", "dog", "bird", "fish"]})
        result = label_set(df, ["a"], ["cat", "dog", "bird"], "s")
        assert result["labels_percent"].to_list() == [0.75]

    def test_nulls_are_not_counted_as_labels(self):
        df = pl.DataFrame({"a": ["x", None, "y", None]})
        result = label_set(df, ["a"], ["x", "y"], "s")
        # 2 matchs sur 4 lignes
        assert result["labels_percent"].to_list() == [0.5]

    def test_all_nulls_gives_zero(self):
        df = pl.DataFrame({"a": pl.Series([None, None], dtype=pl.String)})
        result = label_set(df, ["a"], ["x"], "s")
        assert result["labels_percent"].to_list() == [0.0]

    def test_output_columns(self):
        df = pl.DataFrame({"a": ["x"]})
        result = label_set(df, ["a"], ["x"], "s")
        assert result.columns == [
            "source",
            "column_name",
            "labels_percent",
            "timestamp",
        ]

    def test_output_height_matches_columns(self):
        df = pl.DataFrame({"a": ["x"], "b": ["y"], "c": ["z"]})
        result = label_set(df, ["a", "b", "c"], ["x"], "s")
        assert result.height == 3

    def test_column_order_is_preserved(self):
        df = pl.DataFrame({"a": ["x"], "b": ["x"], "c": ["x"]})
        result = label_set(df, ["c", "a"], ["x"], "s")
        assert result["column_name"].to_list() == ["c", "a"]

    def test_source_is_broadcast_on_every_row(self):
        df = pl.DataFrame({"a": ["x"], "b": ["y"], "c": ["z"]})
        result = label_set(df, ["a", "b", "c"], ["x"], "my-source")
        assert result["source"].to_list() == ["my-source"] * 3

    def test_empty_source_string(self):
        df = pl.DataFrame({"a": ["x"]})
        result = label_set(df, ["a"], ["x"], "")
        assert result["source"].to_list() == [""]

    def test_labels_percent_dtype_is_float64(self):
        df = pl.DataFrame({"a": ["x", "y"]})
        result = label_set(df, ["a"], ["x"], "s")
        assert result["labels_percent"].dtype == pl.Float64

    def test_timestamp_is_timezone_aware_utc(self):
        df = pl.DataFrame({"a": ["x"]})
        result = label_set(df, ["a"], ["x"], "s")
        ts = result["timestamp"][0]
        assert ts.tzinfo is not None
        assert ts.utcoffset() == timedelta(0)

    def test_timestamp_is_recent(self):
        df = pl.DataFrame({"a": ["x"]})
        before = datetime.now(ZoneInfo("UTC")) - timedelta(seconds=1)
        result = label_set(df, ["a"], ["x"], "s")
        after = datetime.now(ZoneInfo("UTC")) + timedelta(seconds=1)
        ts = result["timestamp"][0]
        assert before <= ts <= after

    def test_timestamp_is_identical_for_all_rows(self):
        df = pl.DataFrame({"a": ["x"], "b": ["y"]})
        result = label_set(df, ["a", "b"], ["x"], "s")
        ts = result["timestamp"].to_list()
        assert ts[0] == ts[1]

    def test_empty_labels_list_gives_zero(self):
        df = pl.DataFrame({"a": ["x", "y"]})
        result = label_set(df, ["a"], [], "s")
        assert result["labels_percent"].to_list() == [0.0]


class TestConformiteInterval:
    def test_all_values_in_interval(self):
        df = pl.DataFrame({"a": [1.0, 2.0, 3.0]})
        result = interval_validity(df, ["a"], 0.0, 5.0, "s")
        assert result["interval_percent"].to_list() == [1.0]

    def test_no_value_in_interval(self):
        df = pl.DataFrame({"a": [10.0, 20.0, 30.0]})
        result = interval_validity(df, ["a"], 0.0, 5.0, "s")
        assert result["interval_percent"].to_list() == [0.0]

    def test_partial_match(self):
        df = pl.DataFrame({"a": [1.0, 2.0, 100.0, 3.0]})
        result = interval_validity(df, ["a"], 0.0, 5.0, "s")
        assert result["interval_percent"].to_list() == [0.75]

    def test_single_value_in_interval(self):
        df = pl.DataFrame({"a": [3.0]})
        result = interval_validity(df, ["a"], 0.0, 5.0, "s")
        assert result["interval_percent"].to_list() == [1.0]

    def test_single_value_out_of_interval(self):
        df = pl.DataFrame({"a": [10.0]})
        result = interval_validity(df, ["a"], 0.0, 5.0, "s")
        assert result["interval_percent"].to_list() == [0.0]

    def test_min_bound_is_inclusive(self):
        df = pl.DataFrame({"a": [0.0]})
        result = interval_validity(df, ["a"], 0.0, 5.0, "s")
        assert result["interval_percent"].to_list() == [1.0]

    def test_max_bound_is_inclusive(self):
        df = pl.DataFrame({"a": [5.0]})
        result = interval_validity(df, ["a"], 0.0, 5.0, "s")
        assert result["interval_percent"].to_list() == [1.0]

    def test_both_bounds_inclusive(self):
        df = pl.DataFrame({"a": [0.0, 5.0]})
        result = interval_validity(df, ["a"], 0.0, 5.0, "s")
        assert result["interval_percent"].to_list() == [1.0]

    def test_just_below_min_excluded(self):
        df = pl.DataFrame({"a": [-0.001]})
        result = interval_validity(df, ["a"], 0.0, 5.0, "s")
        assert result["interval_percent"].to_list() == [0.0]

    def test_just_above_max_excluded(self):
        df = pl.DataFrame({"a": [5.001]})
        result = interval_validity(df, ["a"], 0.0, 5.0, "s")
        assert result["interval_percent"].to_list() == [0.0]

    def test_negative_interval(self):
        df = pl.DataFrame({"a": [-10.0, -5.0, 0.0, 5.0]})
        result = interval_validity(df, ["a"], -8.0, -2.0, "s")
        assert result["interval_percent"].to_list() == [0.25]

    def test_nulls_are_not_counted_as_matches(self):
        df = pl.DataFrame({"a": [1.0, None, 2.0, None]})
        result = interval_validity(df, ["a"], 0.0, 5.0, "s")
        # 2 matchs / 4 lignes
        assert result["interval_percent"].to_list() == [0.5]

    def test_all_nulls_gives_zero(self):
        df = pl.DataFrame({"a": pl.Series([None, None], dtype=pl.Float64)})
        result = interval_validity(df, ["a"], 0.0, 5.0, "s")
        assert result["interval_percent"].to_list() == [0.0]

    def test_multiple_columns(self):
        df = pl.DataFrame(
            {
                "a": [1.0, 2.0, 3.0, 4.0],
                "b": [1.0, 100.0, 2.0, 100.0],
                "c": [0.0, 0.0, 0.0, 0.0],
            }
        )
        result = interval_validity(df, ["a", "b", "c"], 0.0, 5.0, "s")
        assert result["interval_percent"].to_list() == [1.0, 0.5, 1.0]

    def test_column_order_is_preserved(self):
        df = pl.DataFrame({"a": [1.0], "b": [1.0], "c": [1.0]})
        result = interval_validity(df, ["c", "a"], 0.0, 5.0, "s")
        assert result["column_name"].to_list() == ["c", "a"]

    def test_output_height_matches_columns(self):
        df = pl.DataFrame({"a": [1.0], "b": [1.0], "c": [1.0]})
        result = interval_validity(df, ["a", "b", "c"], 0.0, 5.0, "s")
        assert result.height == 3

    def test_different_columns_different_results(self):
        df = pl.DataFrame(
            {
                "a": [1.0, 1.0, 1.0, 1.0],
                "b": [10.0, 10.0, 10.0, 10.0],
            }
        )
        result = interval_validity(df, ["a", "b"], 0.0, 5.0, "s")
        assert result["interval_percent"].to_list() == [1.0, 0.0]

    def test_output_columns(self):
        df = pl.DataFrame({"a": [1.0]})
        result = interval_validity(df, ["a"], 0.0, 5.0, "s")
        assert result.columns == [
            "source",
            "column_name",
            "interval_percent",
            "timestamp",
        ]

    def test_source_is_broadcast_on_every_row(self):
        df = pl.DataFrame({"a": [1.0], "b": [1.0], "c": [1.0]})
        result = interval_validity(df, ["a", "b", "c"], 0.0, 5.0, "my-source")
        assert result["source"].to_list() == ["my-source"] * 3

    def test_empty_source_string(self):
        df = pl.DataFrame({"a": [1.0]})
        result = interval_validity(df, ["a"], 0.0, 5.0, "")
        assert result["source"].to_list() == [""]

    def test_interval_percent_dtype_is_float64(self):
        df = pl.DataFrame({"a": [1.0, 100.0]})
        result = interval_validity(df, ["a"], 0.0, 5.0, "s")
        assert result["interval_percent"].dtype == pl.Float64

    def test_timestamp_is_timezone_aware_utc(self):
        df = pl.DataFrame({"a": [1.0]})
        result = interval_validity(df, ["a"], 0.0, 5.0, "s")
        ts = result["timestamp"][0]
        assert ts.tzinfo is not None
        assert ts.utcoffset() == timedelta(0)

    def test_timestamp_is_recent(self):
        df = pl.DataFrame({"a": [1.0]})
        before = datetime.now(ZoneInfo("UTC")) - timedelta(seconds=1)
        result = interval_validity(df, ["a"], 0.0, 5.0, "s")
        after = datetime.now(ZoneInfo("UTC")) + timedelta(seconds=1)
        ts = result["timestamp"][0]
        assert before <= ts <= after

    def test_timestamp_is_identical_for_all_rows(self):
        df = pl.DataFrame({"a": [1.0], "b": [1.0]})
        result = interval_validity(df, ["a", "b"], 0.0, 5.0, "s")
        ts = result["timestamp"].to_list()
        assert ts[0] == ts[1]

    def test_min_equal_max(self):
        df = pl.DataFrame({"a": [3.0, 3.0, 4.0]})
        result = interval_validity(df, ["a"], 3.0, 3.0, "s")
        assert result["interval_percent"].to_list() == [pytest.approx(2 / 3)]

    def test_min_greater_than_max_gives_zero(self):
        df = pl.DataFrame({"a": [1.0, 2.0, 3.0]})
        result = interval_validity(df, ["a"], 5.0, 0.0, "s")
        assert result["interval_percent"].to_list() == [0.0]

    def test_float_precision(self):
        df = pl.DataFrame({"a": [0.1, 0.2, 0.3]})
        result = interval_validity(df, ["a"], 0.0, 0.25, "s")
        assert result["interval_percent"].to_list() == pytest.approx([2 / 3])

    def test_integer_dataframe(self):
        df = pl.DataFrame({"a": [1, 2, 3, 4]})
        result = interval_validity(df, ["a"], 2, 3, "s")
        assert result["interval_percent"].to_list() == [0.5]


class TestConformiteFK:
    def test_all_values_included(self):
        df1 = pl.DataFrame({"a": ["x", "y", "z"]})
        df2 = pl.DataFrame({"b": ["x", "y", "z", "w"]})
        result = foreign_key_validity(df1, df2, [("a", "b")], ("s1", "s2"))
        assert result["fk_percent"].to_list() == [1.0]

    def test_no_value_included(self):
        df1 = pl.DataFrame({"a": ["x", "y"]})
        df2 = pl.DataFrame({"b": ["p", "q"]})
        result = foreign_key_validity(df1, df2, [("a", "b")], ("s1", "s2"))
        assert result["fk_percent"].to_list() == [0.0]

    def test_partial_inclusion(self):
        df1 = pl.DataFrame({"a": ["x", "y", "z", "w"]})
        df2 = pl.DataFrame({"b": ["x", "y"]})
        result = foreign_key_validity(df1, df2, [("a", "b")], ("s1", "s2"))
        assert result["fk_percent"].to_list() == [0.5]

    def test_single_value_match(self):
        df1 = pl.DataFrame({"a": ["x"]})
        df2 = pl.DataFrame({"b": ["x"]})
        result = foreign_key_validity(df1, df2, [("a", "b")], ("s1", "s2"))
        assert result["fk_percent"].to_list() == [1.0]

    def test_single_value_no_match(self):
        df1 = pl.DataFrame({"a": ["x"]})
        df2 = pl.DataFrame({"b": ["y"]})
        result = foreign_key_validity(df1, df2, [("a", "b")], ("s1", "s2"))
        assert result["fk_percent"].to_list() == [0.0]

    def test_numeric_values(self):
        df1 = pl.DataFrame({"a": [1, 2, 3, 4]})
        df2 = pl.DataFrame({"b": [1, 2]})
        result = foreign_key_validity(df1, df2, [("a", "b")], ("s1", "s2"))
        assert result["fk_percent"].to_list() == [0.5]

    def test_float_values(self):
        df1 = pl.DataFrame({"a": [1.0, 2.0, 3.0]})
        df2 = pl.DataFrame({"b": [1.5, 2.0]})
        result = foreign_key_validity(df1, df2, [("a", "b")], ("s1", "s2"))
        assert result["fk_percent"].to_list() == pytest.approx([1 / 3])

    # Doublons

    def test_duplicates_in_df1_counted_separately(self):
        df1 = pl.DataFrame({"a": ["x", "x", "y"]})
        df2 = pl.DataFrame({"b": ["x"]})
        result = foreign_key_validity(df1, df2, [("a", "b")], ("s1", "s2"))
        # "x" apparaît 2 fois dans df1 → 2/3
        assert result["fk_percent"].to_list() == pytest.approx([2 / 3])

    def test_duplicates_in_df2_do_not_change_result(self):
        df1 = pl.DataFrame({"a": ["x", "y"]})
        df2 = pl.DataFrame({"b": ["x", "x", "x"]})
        result = foreign_key_validity(df1, df2, [("a", "b")], ("s1", "s2"))
        assert result["fk_percent"].to_list() == [0.5]

    def test_nulls_in_df1_not_counted_as_match(self):
        df1 = pl.DataFrame({"a": ["x", None, "y", None]})
        df2 = pl.DataFrame({"b": ["x", "y"]})
        result = foreign_key_validity(df1, df2, [("a", "b")], ("s1", "s2"))
        # 2 matchs / 4 lignes
        assert result["fk_percent"].to_list() == [0.5]

    def test_nulls_in_df2_not_matched(self):
        df1 = pl.DataFrame({"a": ["x", "y"]})
        df2 = pl.DataFrame({"b": ["x", None]})
        result = foreign_key_validity(df1, df2, [("a", "b")], ("s1", "s2"))
        assert result["fk_percent"].to_list() == [0.5]

    def test_all_nulls_df1(self):
        df1 = pl.DataFrame({"a": pl.Series([None, None], dtype=pl.String)})
        df2 = pl.DataFrame({"b": ["x"]})
        result = foreign_key_validity(df1, df2, [("a", "b")], ("s1", "s2"))
        assert result["fk_percent"].to_list() == [0.0]

    def test_multiple_pairs(self):
        df1 = pl.DataFrame(
            {
                "a": ["x", "y", "z", "w"],
                "b": [1, 2, 3, 4],
                "c": ["p", "q", "r", "s"],
            }
        )
        df2 = pl.DataFrame(
            {
                "x": ["x", "y"],
                "y": [1, 2],
                "z": [
                    "p",
                    "q",
                ],
            }
        )
        result = foreign_key_validity(
            df1,
            df2,
            [("a", "x"), ("b", "y"), ("c", "z")],
            ("s1", "s2"),
        )
        assert result["fk_percent"].to_list() == [0.5, 0.5, 0.5]

    def test_pair_order_is_preserved(self):
        df1 = pl.DataFrame({"a": ["x"], "b": ["y"], "c": ["z"]})
        df2 = pl.DataFrame({"x": ["x"], "y": ["y"], "z": ["z"]})
        result = foreign_key_validity(
            df1,
            df2,
            [("c", "z"), ("a", "x"), ("b", "y")],
            ("s1", "s2"),
        )
        assert result["column_1"].to_list() == ["c", "a", "b"]
        assert result["column_2"].to_list() == ["z", "x", "y"]

    def test_output_height_matches_columns(self):
        df1 = pl.DataFrame({"a": ["x"], "b": ["y"], "c": ["z"]})
        df2 = pl.DataFrame({"x": ["x"], "y": ["y"], "z": ["z"]})
        result = foreign_key_validity(
            df1,
            df2,
            [("a", "x"), ("b", "y"), ("c", "z")],
            ("s1", "s2"),
        )
        assert result.height == 3

    def test_same_column_names_in_df1_and_df2(self):
        df1 = pl.DataFrame({"a": ["x", "y"]})
        df2 = pl.DataFrame({"a": ["x"]})
        result = foreign_key_validity(df1, df2, [("a", "a")], ("s1", "s2"))
        assert result["fk_percent"].to_list() == [0.5]

    def test_denominator_is_df1_height_for_every_pair(self):
        # Les colonnes ont des longueurs identiques (même df1),
        # donc le dénominateur ne doit pas changer selon la colonne.
        df1 = pl.DataFrame({"a": ["x", "x", "y", "y"]})
        df2 = pl.DataFrame({"x": ["x"], "y": ["y"]})
        result = foreign_key_validity(
            df1,
            df2,
            [("a", "x"), ("a", "y")],
            ("s1", "s2"),
        )
        assert result["fk_percent"].to_list() == [0.5, 0.5]

    # Structure de la sortie

    def test_output_columns(self):
        df1 = pl.DataFrame({"a": ["x"]})
        df2 = pl.DataFrame({"b": ["x"]})
        result = foreign_key_validity(df1, df2, [("a", "b")], ("s1", "s2"))
        assert result.columns == [
            "source_1",
            "source_2",
            "column_1",
            "column_2",
            "fk_percent",
            "timestamp",
        ]

    def test_sources_are_broadcast(self):
        df1 = pl.DataFrame({"a": ["x"], "b": ["x"], "c": ["x"]})
        df2 = pl.DataFrame({"x": ["x"], "y": ["x"], "z": ["x"]})
        result = foreign_key_validity(
            df1,
            df2,
            [("a", "x"), ("b", "y"), ("c", "z")],
            ("srcA", "srcB"),
        )
        assert result["source_1"].to_list() == ["srcA"] * 3
        assert result["source_2"].to_list() == ["srcB"] * 3

    def test_column_names_are_preserved_in_output(self):
        df1 = pl.DataFrame({"foo": ["x"]})
        df2 = pl.DataFrame({"bar": ["x"]})
        result = foreign_key_validity(df1, df2, [("foo", "bar")], ("s1", "s2"))
        assert result["column_1"].to_list() == ["foo"]
        assert result["column_2"].to_list() == ["bar"]

    def test_empty_source_strings(self):
        df1 = pl.DataFrame({"a": ["x"]})
        df2 = pl.DataFrame({"b": ["x"]})
        result = foreign_key_validity(df1, df2, [("a", "b")], ("", ""))
        assert result["source_1"].to_list() == [""]
        assert result["source_2"].to_list() == [""]

    def test_match_proportion_dtype_is_float64(self):
        df1 = pl.DataFrame({"a": ["x"]})
        df2 = pl.DataFrame({"b": ["x"]})
        result = foreign_key_validity(df1, df2, [("a", "b")], ("s1", "s2"))
        assert result["fk_percent"].dtype == pl.Float64

    # Timestamp

    def test_timestamp_is_timezone_aware_utc(self):
        df1 = pl.DataFrame({"a": ["x"]})
        df2 = pl.DataFrame({"b": ["x"]})
        result = foreign_key_validity(df1, df2, [("a", "b")], ("s1", "s2"))
        ts = result["timestamp"][0]
        assert ts.tzinfo is not None
        assert ts.utcoffset() == timedelta(0)

    def test_timestamp_is_recent(self):
        df1 = pl.DataFrame({"a": ["x"]})
        df2 = pl.DataFrame({"b": ["x"]})
        before = datetime.now(ZoneInfo("UTC")) - timedelta(seconds=1)
        result = foreign_key_validity(df1, df2, [("a", "b")], ("s1", "s2"))
        after = datetime.now(ZoneInfo("UTC")) + timedelta(seconds=1)
        ts = result["timestamp"][0]
        assert before <= ts <= after

    def test_timestamp_is_identical_for_all_rows(self):
        df1 = pl.DataFrame({"a": ["x"], "b": ["x"]})
        df2 = pl.DataFrame({"x": ["x"], "y": ["x"]})
        result = foreign_key_validity(
            df1,
            df2,
            [("a", "x"), ("b", "y")],
            ("s1", "s2"),
        )
        ts = result["timestamp"].to_list()
        assert ts[0] == ts[1]

    # Cas limites

    def test_empty_df2_gives_zero(self):
        df1 = pl.DataFrame({"a": ["x", "y"]})
        df2 = pl.DataFrame({"b": pl.Series([], dtype=pl.String)})
        result = foreign_key_validity(df1, df2, [("a", "b")], ("s1", "s2"))
        assert result["fk_percent"].to_list() == [0.0]

    def test_df2_with_only_nulls(self):
        df1 = pl.DataFrame({"a": ["x", "y"]})
        df2 = pl.DataFrame({"b": pl.Series([None, None], dtype=pl.String)})
        result = foreign_key_validity(df1, df2, [("a", "b")], ("s1", "s2"))
        assert result["fk_percent"].to_list() == [0.0]

    def test_no_columns_raises_on_concat(self):
        # pl.concat([]) lève une erreur : la liste de paires ne peut pas être vide.
        df1 = pl.DataFrame({"a": ["x"]})
        df2 = pl.DataFrame({"b": ["x"]})
        with pytest.raises(Exception):
            foreign_key_validity(df1, df2, [], ("s1", "s2"))

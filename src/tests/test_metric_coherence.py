"""Tests unitaires pour coherence.py (à lancer avec `pytest`)."""

from datetime import date, datetime

import numpy as np
import polars as pl
import pytest

from metrics.coherence import (
    compute_dissimilarity_matrix,
    encode_num_rel,
    encode_proj_rel,
    filter_rel,
    multimodal_distance,
)


class TestFilterRel:
    def test_drops_column_with_all_unique_values(self):
        df = pl.DataFrame({"id": [1, 2, 3, 4], "color": ["a", "a", "b", "b"]})
        result = filter_rel(df)
        assert result.columns == ["color"]

    def test_keeps_all_columns_when_none_is_unique(self):
        df = pl.DataFrame({"a": [1, 1, 2], "b": ["x", "y", "x"]})
        result = filter_rel(df)
        assert result.columns == ["a", "b"]
        assert result.equals(df)

    def test_drops_multiple_unique_columns(self):
        df = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "uuid": ["u1", "u2", "u3"],
                "category": ["a", "a", "b"],
            }
        )
        assert filter_rel(df).columns == ["category"]

    def test_all_columns_unique_returns_empty_frame(self):
        df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        result = filter_rel(df)
        assert result.width == 0

    def test_preserves_column_order(self):
        df = pl.DataFrame({"z": [1, 1, 2], "id": [1, 2, 3], "a": ["x", "x", "y"]})
        assert filter_rel(df).columns == ["z", "a"]

    def test_preserves_row_count_and_values(self):
        df = pl.DataFrame({"id": [1, 2, 3], "v": [10, 10, 20]})
        result = filter_rel(df)
        assert result.height == 3
        assert result["v"].to_list() == [10, 10, 20]

    def test_does_not_mutate_input(self):
        df = pl.DataFrame({"id": [1, 2, 3], "v": [10, 10, 20]})
        filter_rel(df)
        assert df.columns == ["id", "v"]

    def test_null_counts_as_a_distinct_value(self):
        # n_unique() compte null comme une valeur : [1, None, 3] -> 3 uniques
        df = pl.DataFrame({"k": [1, None, 3], "v": [1, 1, 2]})
        assert filter_rel(df).columns == ["v"]

    @pytest.mark.parametrize(
        "constant",
        [["a", "a", "a"], [1, 1, 1], [True, True, True], [0.5, 0.5, 0.5]],
        ids=["str", "int", "bool", "float"],
    )
    def test_drops_constant_column(self, constant):
        # Une colonne constante n'apporte aucune information relationnelle et
        # ferait crash encode_proj_rel (perplexity = 0) si elle est textuelle.
        df = pl.DataFrame({"const": constant, "v": ["x", "x", "y"], "id": [1, 2, 3]})
        assert filter_rel(df).columns == ["v"]

    def test_drops_column_that_is_entirely_null(self):
        # n_unique() == 1 pour une colonne 100 % null -> traitée comme constante
        df = pl.DataFrame(
            {"empty": pl.Series([None, None, None], dtype=pl.String), "v": [1, 1, 2]}
        )
        assert filter_rel(df).columns == ["v"]

    def test_keeps_column_with_exactly_two_distinct_values(self):
        # borne basse : 2 valeurs distinctes suffisent à garder la colonne
        df = pl.DataFrame({"binary": ["a", "b", "a", "b"], "id": [1, 2, 3, 4]})
        assert filter_rel(df).columns == ["binary"]

    def test_single_row_drops_every_column(self):
        df = pl.DataFrame({"a": [1], "b": ["x"]})
        assert filter_rel(df).width == 0


def _frame_with_k_non_null(n: int, k: int) -> pl.DataFrame:
    """Colonne `c` de n lignes dont k valeurs non nulles (non constante, non unique)."""
    values = [i % 3 for i in range(k)] + [None] * (n - k)
    return pl.DataFrame({"c": pl.Series(values, dtype=pl.Int64)})


class TestFilterRelCompleteness:
    def test_default_not_keeps_columns_with_nulls(self):
        df = _frame_with_k_non_null(n=10, k=2)
        assert filter_rel(df).width == 0

    def test_eighty_threshold_is_same_as_default(self):
        df = _frame_with_k_non_null(n=10, k=2)
        assert filter_rel(df, seuil_completude=0.8).equals(filter_rel(df))

    def test_drops_column_below_threshold(self):
        df = _frame_with_k_non_null(n=10, k=2)  # 20 % de complétude
        assert filter_rel(df, seuil_completude=0.5).width == 0

    def test_keeps_column_above_threshold(self):
        df = _frame_with_k_non_null(n=10, k=8)  # 80 %
        assert filter_rel(df, seuil_completude=0.5).columns == ["c"]

    def test_threshold_is_inclusive(self):
        df = _frame_with_k_non_null(n=10, k=5)  # exactement 50 %
        assert filter_rel(df, seuil_completude=0.5).columns == ["c"]

    def test_just_below_threshold_is_dropped(self):
        df = _frame_with_k_non_null(n=10, k=4)  # 40 % < 50 %
        assert filter_rel(df, seuil_completude=0.5).width == 0

    @pytest.mark.parametrize(
        "n,k,seuil",
        [(50, 7, 0.14), (50, 14, 0.28), (25, 7, 0.28), (50, 28, 0.56)],
    )
    def test_exact_boundary_is_not_broken_by_float_rounding(self, n, k, seuil):
        # k/n == seuil exactement, mais n * seuil donne 7.000000000000001 :
        # un test `k >= n * seuil` rejetterait à tort ces colonnes.
        df = _frame_with_k_non_null(n=n, k=k)
        assert filter_rel(df, seuil_completude=seuil).columns == ["c"]

    def test_threshold_one_keeps_only_fully_filled_columns(self):
        df = pl.DataFrame({"full": [1, 1, 2, 2], "holes": [1, None, 2, 2]})
        assert filter_rel(df, seuil_completude=1.0).columns == ["full"]

    def test_threshold_applies_per_column(self):
        df = pl.DataFrame(
            {
                "dense": [1, 2, 1, 2, 1, 2, 1, 2, 1, None],  # 90 %
                "sparse": [
                    1,
                    2,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                ],  # 20 %
            }
        )
        assert filter_rel(df, seuil_completude=0.5).columns == ["dense"]

    def test_single_value_plus_nulls_is_dropped_with_reasonable_threshold(self):
        # Le cas limite à l'origine du crash TSNE (perplexity = 0).
        df = pl.DataFrame({"rare": ["a"] + [None] * 49, "v": [0, 1] * 25})
        assert filter_rel(df, seuil_completude=0.5).columns == ["v"]

    def test_complete_but_constant_column_is_still_dropped(self):
        df = pl.DataFrame({"const": ["a"] * 4, "v": [1, 1, 2, 2]})
        assert filter_rel(df, seuil_completude=1.0).columns == ["v"]

    def test_complete_but_unique_column_is_still_dropped(self):
        df = pl.DataFrame({"id": [1, 2, 3, 4], "v": [1, 1, 2, 2]})
        assert filter_rel(df, seuil_completude=1.0).columns == ["v"]

    def test_preserves_order_and_rows(self):
        df = pl.DataFrame(
            {
                "b": [1, 2, 1, 2],
                "gone": [None, None, None, 1],
                "a": ["x", "y", "x", "y"],
            }
        )
        out = filter_rel(df, seuil_completude=0.5)
        assert out.columns == ["b", "a"]
        assert out.height == 4

    @pytest.mark.parametrize("seuil", [0.0, 0.5, 1.0])
    def test_single_distinct_value_plus_few_nulls_is_dropped(self, seuil):
        # 9 fois "a" + 1 null : complétude 90 % mais une seule valeur distincte
        # hors null -> écartée quel que soit le seuil.
        df = pl.DataFrame({"mono": ["a"] * 9 + [None], "v": [1, 2] * 5})
        assert filter_rel(df, seuil_completude=seuil).columns == ["v"]

    def test_two_distinct_values_plus_nulls_is_kept(self):
        # borne basse du critère « valeurs distinctes hors null » : 2 suffisent
        df = pl.DataFrame({"c": ["a", "b", "a", None, "b", "a"]})
        assert filter_rel(df).columns == ["c"]

    def test_constant_column_is_dropped_whether_or_not_it_has_nulls(self):
        df = pl.DataFrame(
            {
                "const_full": ["a"] * 6,
                "const_nulls": ["a", "a", "a", "a", None, None],
                "v": [1, 1, 2, 2, 3, 3],
            }
        )
        assert filter_rel(df).columns == ["v"]

    def test_completeness_exactly_zero_non_null_values(self):
        df = pl.DataFrame(
            {"all_null": pl.Series([None] * 4, dtype=pl.Float64), "v": [1, 1, 2, 2]}
        )
        assert filter_rel(df, seuil_completude=0.0).columns == ["v"]

    @pytest.mark.parametrize("seuil", [0.0, 1.0])
    def test_bounds_are_valid(self, seuil):
        filter_rel(pl.DataFrame({"a": [1, 1, 2]}), seuil_completude=seuil)

    @pytest.mark.parametrize("seuil", [-0.01, 1.01, 50, float("nan")])
    def test_invalid_threshold_raises(self, seuil):
        with pytest.raises(AssertionError, match="seuil_completude"):
            filter_rel(pl.DataFrame({"a": [1, 1, 2]}), seuil_completude=seuil)


class TestComputeDissimilarityMatrix:
    def test_shape_is_n_by_n(self):
        D = compute_dissimilarity_matrix(["a", "bb", "ccc", "dddd"])
        assert D.shape == (4, 4)

    def test_is_symmetric(self):
        D = compute_dissimilarity_matrix(["chat", "chien", "chameau", "rat"])
        np.testing.assert_array_equal(D, D.T)

    def test_diagonal_is_zero(self):
        D = compute_dissimilarity_matrix(["chat", "chien", "rat"])
        np.testing.assert_array_equal(np.diag(D), np.zeros(3))

    def test_identical_labels_have_zero_distance(self):
        D = compute_dissimilarity_matrix(["abc", "abc"])
        assert D[0, 1] == 0.0

    def test_completely_different_same_length_is_one(self):
        D = compute_dissimilarity_matrix(["abc", "xyz"])
        assert D[0, 1] == pytest.approx(1.0)

    def test_known_value_kitten_sitting(self):
        # distance de Levenshtein = 3, longueur max = 7
        D = compute_dissimilarity_matrix(["kitten", "sitting"])
        assert D[0, 1] == pytest.approx(3 / 7)

    def test_single_substitution(self):
        D = compute_dissimilarity_matrix(["chat", "chap"])
        assert D[0, 1] == pytest.approx(1 / 4)

    def test_normalisation_uses_longest_label(self):
        # "a" -> "abcd" : 3 insertions, longueur max 4
        D = compute_dissimilarity_matrix(["a", "abcd"])
        assert D[0, 1] == pytest.approx(3 / 4)

    def test_values_are_between_zero_and_one(self):
        labels = ["", "a", "abc", "xyz", "kitten", "sitting", "hello world"]
        D = compute_dissimilarity_matrix(labels)
        assert D.min() >= 0.0
        assert D.max() <= 1.0

    def test_two_empty_strings_do_not_divide_by_zero(self):
        D = compute_dissimilarity_matrix(["", ""])
        assert D[0, 1] == 0.0
        assert D[1, 0] == 0.0

    def test_empty_string_vs_non_empty_is_one(self):
        D = compute_dissimilarity_matrix(["", "abc"])
        assert D[0, 1] == pytest.approx(1.0)

    def test_accepts_list(self):
        D = compute_dissimilarity_matrix(["a", "b"])
        assert D.shape == (2, 2)

    def test_accepts_polars_series(self):
        D = compute_dissimilarity_matrix(pl.Series(["kitten", "sitting"]))
        assert D[0, 1] == pytest.approx(3 / 7)

    def test_list_and_series_give_same_result(self):
        labels = ["chat", "chien", "rat", "chameau"]
        np.testing.assert_array_equal(
            compute_dissimilarity_matrix(labels),
            compute_dissimilarity_matrix(pl.Series(labels)),
        )

    def test_single_label_gives_1x1_zero_matrix(self):
        D = compute_dissimilarity_matrix(["solo"])
        np.testing.assert_array_equal(D, np.zeros((1, 1)))

    def test_no_labels_gives_empty_matrix(self):
        D = compute_dissimilarity_matrix([])
        assert D.shape == (0, 0)

    def test_is_case_sensitive(self):
        D = compute_dissimilarity_matrix(["abc", "ABC"])
        assert D[0, 1] == pytest.approx(1.0)

    def test_is_case_sensitive_partial(self):
        D = compute_dissimilarity_matrix(["abc", "aBc"])
        assert D[0, 1] == pytest.approx(1 / 3)


class TestMultimodalDistance:
    def test_arrays_use_euclidean_norm(self):
        a = np.array([0.0, 0.0])
        b = np.array([3.0, 4.0])
        assert multimodal_distance(a, b) == pytest.approx(5.0)

    def test_arrays_identical_gives_zero(self):
        a = np.array([1.5, -2.0])
        assert multimodal_distance(a, a.copy()) == 0.0

    def test_arrays_symmetric(self):
        a = np.array([1.0, 2.0])
        b = np.array([-3.0, 7.0])
        assert multimodal_distance(a, b) == pytest.approx(multimodal_distance(b, a))

    def test_arrays_higher_dimension(self):
        a = np.array([1.0, 2.0, 2.0])
        assert multimodal_distance(a, np.zeros(3)) == pytest.approx(3.0)

    def test_integers_use_absolute_difference(self):
        assert multimodal_distance(3, 10) == 7

    def test_floats_use_absolute_difference(self):
        assert multimodal_distance(1.5, -2.0) == pytest.approx(3.5)

    def test_scalars_symmetric(self):
        assert multimodal_distance(2, 9) == multimodal_distance(9, 2)

    def test_scalars_identical_gives_zero(self):
        assert multimodal_distance(4.2, 4.2) == 0

    def test_numpy_scalars_use_absolute_difference(self):
        # np.float64 n'est pas un ndarray -> branche abs(a - b)
        assert multimodal_distance(np.float64(1.0), np.float64(4.0)) == pytest.approx(
            3.0
        )

    def test_mixed_array_and_scalar_falls_back_to_elementwise_abs(self):
        # Comportement actuel : un seul ndarray -> abs(a - b) par broadcasting,
        # le résultat est donc un tableau et non un float.
        result = multimodal_distance(np.array([1.0, 5.0]), 3.0)
        np.testing.assert_array_equal(result, np.array([2.0, 2.0]))

    def test_python_lists_are_not_supported(self):
        # Les listes ne sont pas des ndarray et list - list lève TypeError
        with pytest.raises(TypeError):
            multimodal_distance([1, 2], [3, 4])

    def test_incompatible_array_shapes_raise(self):
        with pytest.raises(ValueError):
            multimodal_distance(np.array([1.0, 2.0]), np.array([1.0, 2.0, 3.0]))


@pytest.fixture
def df_with_strings() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "n": [1, 2, 3, 4, 5, 6],
            "city": ["paris", "parix", "lyon", "lyons", "nice", "paris"],
        }
    )


class TestEncodeProjRel:
    def test_string_column_becomes_array_of_two_floats(self, df_with_strings):
        result = encode_proj_rel(df_with_strings)
        assert result.schema["city"] == pl.Array(pl.Float64, 2)

    def test_non_string_columns_are_untouched(self, df_with_strings):
        result = encode_proj_rel(df_with_strings)
        assert result["n"].to_list() == df_with_strings["n"].to_list()
        assert result.schema["n"] == df_with_strings.schema["n"]

    def test_row_count_and_column_names_preserved(self, df_with_strings):
        result = encode_proj_rel(df_with_strings)
        assert result.height == df_with_strings.height
        assert result.columns == df_with_strings.columns

    def test_same_label_gets_same_embedding(self, df_with_strings):
        result = encode_proj_rel(df_with_strings)
        coords = result["city"].to_list()
        assert coords[0] == coords[5]  # "paris" apparaît deux fois

    def test_distinct_labels_get_distinct_embeddings(self, df_with_strings):
        result = encode_proj_rel(df_with_strings)
        coords = {tuple(c) for c in result["city"].to_list()}
        assert len(coords) == df_with_strings["city"].n_unique()

    def test_embeddings_are_finite(self, df_with_strings):
        result = encode_proj_rel(df_with_strings)
        arr = np.array(result["city"].to_list())
        assert arr.shape == (6, 2)
        assert np.isfinite(arr).all()

    def test_is_deterministic(self, df_with_strings):
        r1 = encode_proj_rel(df_with_strings)
        r2 = encode_proj_rel(df_with_strings)
        assert r1.equals(r2)

    def test_does_not_mutate_input(self, df_with_strings):
        before = df_with_strings.clone()
        encode_proj_rel(df_with_strings)
        assert df_with_strings.equals(before)
        assert df_with_strings.schema["city"] == pl.Utf8

    def test_no_string_column_returns_same_frame(self):
        df = pl.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]})
        assert encode_proj_rel(df).equals(df)

    def test_multiple_string_columns_are_all_encoded(self):
        df = pl.DataFrame(
            {
                "first": ["ana", "anna", "bob", "rob", "bobby", "ana"],
                "last": ["dupont", "dupond", "martin", "marten", "durand", "dupont"],
                "age": [20, 30, 40, 50, 60, 70],
            }
        )
        result = encode_proj_rel(df)
        assert result.schema["first"] == pl.Array(pl.Float64, 2)
        assert result.schema["last"] == pl.Array(pl.Float64, 2)
        assert result.schema["age"] == df.schema["age"]

    def test_null_values_stay_null(self):
        df = pl.DataFrame({"s": ["alpha", None, "beta", "gamma", "alphb", "delta"]})
        result = encode_proj_rel(df)
        values = result["s"].to_list()
        assert values[1] is None
        assert all(v is not None for i, v in enumerate(values) if i != 1)

    def test_minimum_viable_number_of_labels(self):
        # 3 labels -> perplexity = int(3 * 0.5) = 1 : cas limite accepté par TSNE
        df = pl.DataFrame({"s": ["aa", "ab", "zz"]})
        result = encode_proj_rel(df)
        assert result.schema["s"] == pl.Array(pl.Float64, 2)

    def test_single_unique_label_raises(self):
        # 1 label -> perplexity = int(1 * 0.5) = 0, rejeté par TSNE
        df = pl.DataFrame({"s": ["same", "same", "same"]})
        with pytest.raises(Exception):
            encode_proj_rel(df)


class TestEncodeNumRel:
    def test_boolean_is_cast_to_int8(self):
        df = pl.DataFrame({"flag": [True, False, True]})
        result = encode_num_rel(df)
        assert result.schema["flag"] == pl.Int8
        assert result["flag"].to_list() == [1, 0, 1]

    def test_date_is_cast_to_int32(self):
        df = pl.DataFrame({"d": [date(1970, 1, 1), date(1970, 1, 2), date(2000, 1, 1)]})
        result = encode_num_rel(df)
        assert result.schema["d"] == pl.Int32

    def test_date_values_are_days_since_epoch(self):
        df = pl.DataFrame({"d": [date(1970, 1, 1), date(1970, 1, 2), date(2000, 1, 1)]})
        result = encode_num_rel(df)
        assert result["d"].to_list() == [0, 1, 10957]

    def test_date_before_epoch_is_negative(self):
        df = pl.DataFrame({"d": [date(1969, 12, 31)]})
        assert encode_num_rel(df)["d"].to_list() == [-1]

    def test_other_dtypes_are_untouched(self):
        df = pl.DataFrame(
            {
                "i": [1, 2, 3],
                "f": [1.5, 2.5, 3.5],
                "s": ["a", "b", "c"],
            }
        )
        result = encode_num_rel(df)
        assert result.schema == df.schema
        assert result.equals(df)

    def test_datetime_is_not_converted(self):
        df = pl.DataFrame(
            {"dt": [datetime(2020, 1, 1, 12, 0), datetime(2021, 6, 1, 8, 30)]}
        )
        result = encode_num_rel(df)
        assert result.schema["dt"] == df.schema["dt"]

    def test_mixed_frame_only_converts_bool_and_date(self):
        df = pl.DataFrame(
            {
                "flag": [True, False],
                "d": [date(1970, 1, 3), date(1970, 1, 4)],
                "x": [0.1, 0.2],
                "name": ["a", "b"],
            }
        )
        result = encode_num_rel(df)
        assert result.schema["flag"] == pl.Int8
        assert result.schema["d"] == pl.Int32
        assert result.schema["x"] == pl.Float64
        assert result.schema["name"] == pl.Utf8
        assert result["flag"].to_list() == [1, 0]
        assert result["d"].to_list() == [2, 3]

    def test_nulls_are_preserved(self):
        df = pl.DataFrame(
            {
                "flag": [True, None, False],
                "d": [date(1970, 1, 2), None, date(1970, 1, 3)],
            }
        )
        result = encode_num_rel(df)
        assert result["flag"].to_list() == [1, None, 0]
        assert result["d"].to_list() == [1, None, 2]

    def test_column_names_and_order_preserved(self):
        df = pl.DataFrame({"b": [True], "a": [1], "c": [date(1970, 1, 1)]})
        assert encode_num_rel(df).columns == ["b", "a", "c"]

    def test_row_count_preserved(self):
        df = pl.DataFrame({"flag": [True, False, True, False]})
        assert encode_num_rel(df).height == 4

    def test_does_not_mutate_input(self):
        df = pl.DataFrame({"flag": [True, False]})
        encode_num_rel(df)
        assert df.schema["flag"] == pl.Boolean

    def test_empty_dataframe_with_typed_columns(self):
        df = pl.DataFrame(
            {"flag": pl.Series([], dtype=pl.Boolean), "d": pl.Series([], dtype=pl.Date)}
        )
        result = encode_num_rel(df)
        assert result.height == 0
        assert result.schema["flag"] == pl.Int8
        assert result.schema["d"] == pl.Int32

    def test_dataframe_without_columns(self):
        assert encode_num_rel(pl.DataFrame()).width == 0

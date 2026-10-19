import polars as pl

from orchestration.tasks.transform.helpers import fuzzy_join


class TestFuzzyJoin:
    # how="left"

    def test_left_single_match(self):
        df1 = pl.DataFrame({"tags": ["apple"], "x": [1]})
        df2 = pl.DataFrame({"ref": ["I like apple a lot"], "y": ["A"]})
        result = fuzzy_join(df1, df2, "tags", "ref", how="left")
        assert result.height == 1
        assert result["x"].to_list() == [1]
        assert result["y"].to_list() == ["A"]

    def test_left_keeps_unmatched_rows(self):
        df1 = pl.DataFrame({"tags": ["apple", "nope"], "x": [1, 2]})
        df2 = pl.DataFrame({"ref": ["apple pie"], "y": ["A"]})
        result = fuzzy_join(df1, df2, "tags", "ref", how="left").sort("x")
        assert result["x"].to_list() == [1, 2]
        assert result["y"].to_list() == ["A", None]

    def test_left_multiple_matches_returns_all_pairs(self):
        """Régression : 1 ligne df1 qui matche N lignes df2 → N lignes."""
        df1 = pl.DataFrame({"tags": ["apple"], "x": [1]})
        df2 = pl.DataFrame(
            {
                "ref": ["apple pie", "green apple", "apple juice", "banana"],
                "y": ["A", "B", "C", "D"],
            }
        )
        result = fuzzy_join(df1, df2, "tags", "ref", how="left").sort("y")
        # Doit contenir toutes les paires valides (A, B, C), pas seulement A
        assert result.height == 3
        assert result["y"].to_list() == ["A", "B", "C"]
        assert result["x"].to_list() == [1, 1, 1]

    # how="inner"

    def test_inner_drops_non_matching_rows(self):
        df1 = pl.DataFrame({"tags": ["apple", "zzz"], "x": [1, 2]})
        df2 = pl.DataFrame({"ref": ["apple pie"], "y": ["A"]})
        result = fuzzy_join(df1, df2, "tags", "ref", how="inner")
        assert result.height == 1
        assert result["x"].to_list() == [1]

    def test_inner_multiple_matches_returns_all_pairs(self):
        """Régression : toutes les paires (df1, df2) qui matchent."""
        df1 = pl.DataFrame({"tags": ["a", "b"], "x": [1, 2]})
        df2 = pl.DataFrame(
            {
                "ref": ["aa", "ab", "bc", "xx"],
                "y": ["p", "q", "r", "s"],
            }
        )
        result = fuzzy_join(df1, df2, "tags", "ref", how="inner")
        actual = set(zip(result["x"].to_list(), result["y"].to_list()))
        # a  -> aa, ab   ;   b -> ab, bc
        assert actual == {(1, "p"), (1, "q"), (2, "q"), (2, "r")}
        assert result.height == 4

    # how="right"

    def test_right_keeps_unmatched_rows(self):
        df1 = pl.DataFrame({"tags": ["apple"], "x": [1]})
        df2 = pl.DataFrame({"ref": ["apple pie", "banana"], "y": ["A", "B"]})
        result = fuzzy_join(df1, df2, "tags", "ref", how="right").sort("y")
        assert result["y"].to_list() == ["A", "B"]
        assert result["x"].to_list() == [1, None]

    def test_right_multiple_matches_returns_all_pairs(self):
        """Régression : toutes les paires côté df2."""
        df1 = pl.DataFrame({"tags": ["apple"], "x": [1]})
        df2 = pl.DataFrame(
            {
                "ref": ["apple pie", "apple juice", "apple tart"],
                "y": ["A", "B", "C"],
            }
        )
        result = fuzzy_join(df1, df2, "tags", "ref", how="right").sort("y")
        assert result.height == 3
        assert result["y"].to_list() == ["A", "B", "C"]
        assert result["x"].to_list() == [1, 1, 1]

    # Split "/" : plusieurs tags par cellule

    def test_multiple_tags_per_cell(self):
        """'apple/pie' → deux tags testés indépendamment."""
        df1 = pl.DataFrame({"tags": ["apple/pie"], "x": [1]})
        df2 = pl.DataFrame(
            {
                "ref": ["apple pie", "banana", "cherry tart"],
                "y": ["A", "B", "C"],
            }
        )
        result = fuzzy_join(df1, df2, "tags", "ref", how="inner").sort("y")
        # "apple" et "pie" matchent toutes deux "apple pie" → 1 paire après unique
        assert result["y"].to_list() == ["A"]

    def test_multiple_tags_independent_matches(self):
        """Chaque tag d'une même ligne df1 peut matcher une df2 différente."""
        df1 = pl.DataFrame({"tags": ["red/fruit"], "x": [1]})
        df2 = pl.DataFrame(
            {
                "ref": ["red apple", "fruit salad", "yellow banana"],
                "y": ["A", "B", "C"],
            }
        )
        result = fuzzy_join(df1, df2, "tags", "ref", how="inner").sort("y")
        assert result["y"].to_list() == ["A", "B"]

    # included="column_2"

    def test_included_column_2(self):
        """Les tags de df2.column_2 sont cherchés dans df1.column_1."""
        df1 = pl.DataFrame(
            {
                "text": ["I like apple pie", "banana split"],
                "x": [1, 2],
            }
        )
        df2 = pl.DataFrame(
            {
                "tags": ["apple", "pie", "banana"],
                "y": ["A", "B", "C"],
            }
        )
        result = fuzzy_join(
            df1,
            df2,
            "text",
            "tags",
            included="column_2",
            how="inner",
        )
        actual = set(zip(result["x"].to_list(), result["y"].to_list()))
        # apple -> x=1 ; pie -> x=1 ; banana -> x=2
        assert actual == {(1, "A"), (1, "B"), (2, "C")}

    # literal=True (échappement des caractères spéciaux regex)

    def test_literal_dot(self):
        df1 = pl.DataFrame({"tags": ["a.c"], "x": [1]})
        df2 = pl.DataFrame(
            {
                "ref": ["xa.c", "abc"],  # "abc" matcherait si regex
                "y": ["LIT", "REGEX"],
            }
        )
        result = fuzzy_join(df1, df2, "tags", "ref", how="inner")
        assert result["y"].to_list() == ["LIT"]

    def test_literal_star(self):
        df1 = pl.DataFrame({"tags": ["a*b"], "x": [1]})
        df2 = pl.DataFrame(
            {
                "ref": ["xa*b", "aXXb"],
                "y": ["LIT", "REGEX"],
            }
        )
        result = fuzzy_join(df1, df2, "tags", "ref", how="inner")
        assert result["y"].to_list() == ["LIT"]

    # Déduplication

    def test_duplicate_tags_produce_one_row(self):
        """'apple/apple' ne doit pas dupliquer les paires après unique()."""
        df1 = pl.DataFrame({"tags": ["apple/apple"], "x": [1]})
        df2 = pl.DataFrame({"ref": ["apple pie"], "y": ["A"]})
        result = fuzzy_join(df1, df2, "tags", "ref", how="inner")
        assert result.height == 1

    def test_duplicate_pairs_deduplicated(self):
        df1 = pl.DataFrame({"tags": ["ab"], "x": [1]})
        df2 = pl.DataFrame({"ref": ["abc"], "y": ["A"]})
        result = fuzzy_join(df1, df2, "tags", "ref", how="inner")
        assert result.height == 1

    # Cas vides / aucun match

    def test_empty_df1_left(self):
        df1 = pl.DataFrame(
            {"tags": [], "x": []},
            schema={"tags": pl.Utf8, "x": pl.Int64},
        )
        df2 = pl.DataFrame({"ref": ["apple"], "y": ["A"]})
        result = fuzzy_join(df1, df2, "tags", "ref", how="left")
        assert result.height == 0
        assert set(result.columns) == {"tags", "x", "ref", "y"}

    def test_empty_df2_left(self):
        df1 = pl.DataFrame({"tags": ["apple"], "x": [1]})
        df2 = pl.DataFrame(
            {"ref": [], "y": []},
            schema={"ref": pl.Utf8, "y": pl.Utf8},
        )
        result = fuzzy_join(df1, df2, "tags", "ref", how="left")
        assert result.height == 1
        assert result["x"].to_list() == [1]
        assert result["y"].to_list() == [None]

    def test_no_match_any_mode(self):
        df1 = pl.DataFrame({"tags": ["xxx"], "x": [1]})
        df2 = pl.DataFrame({"ref": ["yyy"], "y": ["A"]})

        left = fuzzy_join(df1, df2, "tags", "ref", how="left")
        assert left.height == 1 and left["y"].to_list() == [None]

        inner = fuzzy_join(df1, df2, "tags", "ref", how="inner")
        assert inner.height == 0

        right = fuzzy_join(df1, df2, "tags", "ref", how="right")
        assert right.height == 1 and right["x"].to_list() == [None]

    # Ordre de sortie

    def test_result_sorted_by_df1_index(self):
        df1 = pl.DataFrame({"tags": ["b", "a"], "x": [1, 2]})
        df2 = pl.DataFrame({"ref": ["aa", "bb", "ab"], "y": [10, 20, 30]})
        for how in ("left", "inner"):
            result = fuzzy_join(df1, df2, "tags", "ref", how=how)
            xs = result["x"].to_list()
            assert xs == sorted(xs), f"{how} : résultat non trié par df1"

    def test_result_sorted_by_df2_index(self):
        df1 = pl.DataFrame({"tags": ["a", "b"], "x": [1, 2]})
        df2 = pl.DataFrame({"ref": ["bb", "aa", "ab"], "y": [10, 20, 30]})
        result = fuzzy_join(df1, df2, "tags", "ref", how="right")
        ys = result["y"].to_list()
        assert ys == sorted(ys)

from datetime import date
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from metrics.coherence import analyze_grouped_distribution, filter_rel

DATA_PATH = Path(__file__).parent / "resources" / "prescriptions_doliprane.csv"

ID_COLUMNS = ["id_ordonnance", "id_patient"]
CONTROL_COLUMNS = ["sexe", "medecin", "medicament"]

# (x, y, seuil minimal de M1). Seuils fixés avec de la marge sur les valeurs
# observées (données figées + t-SNE seedé => résultat reproductible).
RELATIONS = [
    ("poids_kg", "dosage_mg", 0.70),
    ("duree_jours", "indication", 0.80),
    ("forme", "categorie_age", 0.40),
    ("date_prescription", "saison", 0.90),
    ("age_ans", "categorie_age", 0.70),
]
RELATION_IDS = [f"{x}|{y}" for x, y, _ in RELATIONS]

# Dépendances fonctionnelles exactes : y détermine entièrement x.
EXACT_DEPENDENCIES = [
    ("voie_orale", "forme"),
    ("saison", "date_prescription"),
    ("categorie_age", "age_ans"),
]

MAX_CONTROL_M1 = 0.30

DUREE_PAR_INDICATION: dict[str, int] = {
    "post-vaccinal": 2,
    "fièvre": 3,
    "syndrome grippal": 4,
    "douleur": 5,
}

# (poids_max_exclusif, dosage_mg) — première tranche dont poids < borne
DOSAGE_BANDS: list[tuple[float, int]] = [
    (10.0, 100),
    (15.0, 150),
    (20.0, 200),
    (30.0, 300),
    (50.0, 500),
    (float("inf"), 1000),
]


def dosage_attendu(poids_kg: float) -> int:
    for borne, dosage in DOSAGE_BANDS:
        if poids_kg < borne:
            return dosage
    raise AssertionError("unreachable")


def saison_de(d: date) -> str:
    return {
        12: "hiver",
        1: "hiver",
        2: "hiver",
        3: "printemps",
        4: "printemps",
        5: "printemps",
        6: "ete",
        7: "ete",
        8: "ete",
        9: "automne",
        10: "automne",
        11: "automne",
    }[d.month]


# Fixtures & helpers


@pytest.fixture(scope="module")
def raw_df() -> pl.DataFrame:
    return pl.read_csv(
        DATA_PATH,
        schema_overrides={"date_prescription": pl.Date, "voie_orale": pl.Boolean},
    )


@pytest.fixture(scope="module")
def result(raw_df: pl.DataFrame) -> pl.DataFrame:
    # L'analyse (t-SNE par colonne texte) est coûteuse : calculée une seule fois.
    return analyze_grouped_distribution(raw_df)


def metric(result: pl.DataFrame, x: str, y: str, name: str) -> float:
    row = result.filter((pl.col("x") == x) & (pl.col("y") == y))
    assert row.height == 1, f"paire ({x}, {y}) absente ou dupliquée"
    return row[name].item()


def max_control(result: pl.DataFrame, x: str, name: str) -> float:
    return max(metric(result, x, c, name) for c in CONTROL_COLUMNS)


# Contrat du jeu de données (garde-fou : on teste le test)


class TestDataset:
    def test_has_about_fifty_rows(self, raw_df):
        assert 45 <= raw_df.height <= 55

    def test_is_multimodal(self, raw_df):
        dtypes = set(raw_df.schema.values())
        assert pl.String in dtypes
        assert pl.Float64 in dtypes
        assert pl.Int64 in dtypes
        assert pl.Date in dtypes
        assert pl.Boolean in dtypes

    @pytest.mark.parametrize("col", ID_COLUMNS)
    def test_identifier_columns_are_unique(self, raw_df, col):
        assert raw_df[col].n_unique() == raw_df.height

    def test_no_nulls(self, raw_df):
        assert raw_df.null_count().sum_horizontal().item() == 0

    def test_dosage_follows_weight_for_vast_majority(self, raw_df):
        ok = sum(
            dosage_attendu(p) == d
            for p, d in zip(raw_df["poids_kg"], raw_df["dosage_mg"])
        )
        ratio = ok / raw_df.height
        # « grande majorité » mais pas 100 % : le jeu contient des anomalies
        assert 0.90 <= ratio < 1.0

    def test_duration_follows_indication_for_vast_majority(self, raw_df):
        ok = sum(
            DUREE_PAR_INDICATION[i] == d
            for i, d in zip(raw_df["indication"], raw_df["duree_jours"])
        )
        ratio = ok / raw_df.height
        assert 0.90 <= ratio < 1.0

    def test_season_is_derived_from_date(self, raw_df):
        assert all(
            saison_de(d) == s
            for d, s in zip(raw_df["date_prescription"], raw_df["saison"])
        )

    def test_oral_route_is_false_only_for_suppositories(self, raw_df):
        assert all(
            (not oral) == (forme == "suppositoire")
            for oral, forme in zip(raw_df["voie_orale"], raw_df["forme"])
        )

    def test_age_category_is_consistent_with_age(self, raw_df):
        bounds = {
            "nourrisson": (0, 1),
            "enfant": (2, 11),
            "adolescent": (12, 17),
            "adulte": (18, 120),
        }
        assert all(
            bounds[c][0] <= a <= bounds[c][1]
            for c, a in zip(raw_df["categorie_age"], raw_df["age_ans"])
        )


# Structure de la sortie


class TestOutputStructure:
    def test_schema(self, result):
        assert result.schema == {
            "x": pl.String,
            "y": pl.String,
            "M1": pl.Float64,
            "M2": pl.Float64,
            "M3": pl.Float64,
        }

    def test_identifier_columns_are_filtered_out(self, result):
        used = set(result["x"]) | set(result["y"])
        assert used.isdisjoint(ID_COLUMNS)

    def test_analysed_columns_are_exactly_the_non_unique_ones(self, raw_df, result):
        expected = set(filter_rel(raw_df).columns)
        assert set(result["x"]) == expected
        assert set(result["y"]) == expected

    def test_contains_every_ordered_pair_exactly_once(self, raw_df, result):
        cols = filter_rel(raw_df).columns
        expected = {(x, y) for x in cols for y in cols if x != y}
        pairs = list(zip(result["x"], result["y"]))

        assert len(pairs) == len(cols) * (len(cols) - 1)
        assert len(set(pairs)) == len(pairs)  # pas de doublon
        assert set(pairs) == expected

    def test_no_self_pairs(self, result):
        assert result.filter(pl.col("x") == pl.col("y")).height == 0

    def test_every_column_type_is_analysed(self, result):
        # texte, float, int, date, bool : chacun apparaît bien comme x
        xs = set(result["x"])
        assert {
            "medecin",
            "poids_kg",
            "dosage_mg",
            "date_prescription",
            "voie_orale",
        } <= xs

    def test_is_reproducible(self, raw_df, result):
        # Pas d'égalité stricte : `group_by` ne garantit pas l'ordre des groupes,
        # donc l'ordre de sommation (et l'arrondi flottant) varie à ~1e-16 près.
        again = analyze_grouped_distribution(raw_df)
        assert again["x"].equals(result["x"])
        assert again["y"].equals(result["y"])
        for col in ("M1", "M2", "M3"):
            np.testing.assert_allclose(
                again[col].to_numpy(), result[col].to_numpy(), rtol=1e-9, atol=1e-9
            )

    def test_does_not_mutate_input(self, raw_df):
        before = raw_df.clone()
        analyze_grouped_distribution(raw_df)
        assert raw_df.equals(before)
        assert raw_df.schema["voie_orale"] == pl.Boolean
        assert raw_df.schema["date_prescription"] == pl.Date


# Invariants mathématiques des métriques


class TestMetricInvariants:
    def test_all_values_are_finite(self, result):
        for col in ("M1", "M2", "M3"):
            assert np.isfinite(result[col].to_numpy()).all(), col

    def test_m1_is_a_proportion(self, result):
        # loi de la variance totale : 0 <= var_intra <= var_globale
        m1 = result["M1"].to_numpy()
        assert m1.min() >= -1e-9
        assert m1.max() <= 1 + 1e-9

    def test_m2_is_zero_or_at_least_one(self, result):
        # max des variances de groupe >= moyenne pondérée des variances de groupe
        m2 = result["M2"].to_numpy()
        assert (m2 >= 0).all()
        assert ((m2 < 1e-6) | (m2 >= 1 - 1e-6)).all()

    def test_m3_is_non_negative(self, result):
        assert (result["M3"].to_numpy() >= 0).all()


# Détection des relations métier


class TestBusinessRelations:
    @pytest.mark.parametrize("x,y,min_m1", RELATIONS, ids=RELATION_IDS)
    def test_related_pair_has_high_m1(self, result, x, y, min_m1):
        assert metric(result, x, y, "M1") >= min_m1

    @pytest.mark.parametrize("x,y,_", RELATIONS, ids=RELATION_IDS)
    def test_related_pair_beats_every_control_on_m1(self, result, x, y, _):
        related = metric(result, x, y, "M1")
        assert related > 2 * max_control(result, x, "M1")

    @pytest.mark.parametrize("x,y,_", RELATIONS, ids=RELATION_IDS)
    def test_related_pair_beats_every_control_on_m3(self, result, x, y, _):
        # Mêmes x donc même unité : M3 est comparable entre y.
        related = metric(result, x, y, "M3")
        assert related > max_control(result, x, "M3")

    @pytest.mark.parametrize("x,y", EXACT_DEPENDENCIES)
    def test_exact_functional_dependency(self, result, x, y):
        # y détermine x : variance intra-groupe nulle partout
        assert metric(result, x, y, "M1") == pytest.approx(1.0, abs=1e-6)
        assert metric(result, x, y, "M2") == pytest.approx(0.0, abs=1e-6)

    @pytest.mark.parametrize(
        "x", ["poids_kg", "duree_jours", "forme", "dosage_mg", "voie_orale"]
    )
    @pytest.mark.parametrize("control", CONTROL_COLUMNS)
    def test_unrelated_controls_explain_little_variance(self, result, x, control):
        assert metric(result, x, control, "M1") < MAX_CONTROL_M1

    def test_sex_is_the_least_informative_grouping_for_weight(self, result):
        # Témoin « pur bruit » : sexe n'explique quasiment rien du poids.
        assert metric(result, "poids_kg", "sexe", "M1") < 0.05

    def test_anomalies_keep_m1_below_perfect_for_dosage_by_weight_band(self, result):
        # Grouper le poids par dosage : fort (règle métier) mais < 1 à cause
        # des anomalies de prescription et de la largeur des tranches.
        m1 = metric(result, "poids_kg", "dosage_mg", "M1")
        assert 0.70 <= m1 < 0.999


# Colonnes dégénérées


class TestDegenerateColumns:
    def test_constant_text_column_is_ignored_and_does_not_crash(self, raw_df, result):
        # Ex. : une extraction où tous les médicaments sont « Doliprane ».
        df = raw_df.with_columns(pl.lit("Doliprane").alias("medicament"))
        out = analyze_grouped_distribution(df)

        assert "medicament" not in set(out["x"]) | set(out["y"])
        # Les autres colonnes sont analysées comme avant.
        assert out.height == 12 * 11
        assert metric(out, "poids_kg", "dosage_mg", "M1") == pytest.approx(
            metric(result, "poids_kg", "dosage_mg", "M1"), abs=1e-9
        )

    def test_text_column_with_single_value_and_nulls(self, raw_df, result):
        # Une seule valeur non nulle + des null : écartée même sans seuil
        # (le critère porte sur les valeurs distinctes HORS null).
        df = raw_df.with_columns(
            pl.Series("rare", ["a"] + [None] * (raw_df.height - 1))
        )
        out = analyze_grouped_distribution(df)
        assert "rare" not in set(out["x"]) | set(out["y"])
        assert out.height == result.height


# Seuil de complétude


class TestCompletenessThreshold:
    def test_sparse_column_is_removed_by_completeness_threshold(self, raw_df, result):
        # Cas limite qui plantait : 1 valeur non nulle + 49 null.
        df = raw_df.with_columns(
            pl.Series("rare", ["a"] + [None] * (raw_df.height - 1))
        )
        out = analyze_grouped_distribution(df, seuil_completude=0.5)

        assert "rare" not in set(out["x"]) | set(out["y"])
        assert out.height == result.height
        assert metric(out, "poids_kg", "dosage_mg", "M1") == pytest.approx(
            metric(result, "poids_kg", "dosage_mg", "M1"), abs=1e-9
        )

    def test_threshold_one_changes_nothing_on_a_complete_dataset(self, raw_df, result):
        out = analyze_grouped_distribution(raw_df, seuil_completude=1.0)
        assert out["x"].equals(result["x"]) and out["y"].equals(result["y"])
        for col in ("M1", "M2", "M3"):
            np.testing.assert_allclose(
                out[col].to_numpy(), result[col].to_numpy(), rtol=1e-9, atol=1e-9
            )

    def test_partially_filled_column_follows_the_threshold(self, raw_df):
        # `medecin` rempli à 40 % (20 valeurs sur 50)
        medecin = [None] * 30 + raw_df["medecin"].to_list()[30:]
        df = raw_df.with_columns(pl.Series("medecin", medecin))

        below = analyze_grouped_distribution(df, seuil_completude=0.5)
        above = analyze_grouped_distribution(df, seuil_completude=0.3)

        assert "medecin" not in set(below["x"]) | set(below["y"])
        assert "medecin" in set(above["x"]) | set(above["y"])
        assert above.height > below.height

    def test_invalid_threshold_is_rejected_by_analysis(self, raw_df):
        with pytest.raises(AssertionError, match="seuil_completude"):
            analyze_grouped_distribution(raw_df, seuil_completude=1.5)

    def test_complete_enough_but_single_distinct_value(self, raw_df, result):
        # Remplie à 60 % (passe le seuil) mais une seule valeur distincte hors null.
        df = raw_df.with_columns(pl.Series("mono", ["a"] * 30 + [None] * 20))
        out = analyze_grouped_distribution(df, seuil_completude=0.5)
        assert "mono" not in set(out["x"]) | set(out["y"])
        assert out.height == result.height

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "LIMITE CONNUE : si x contient des null et qu'un groupe de y est "
            "entièrement nul sur x, la moyenne du groupe vaut NaN et contamine "
            "M1/M2/M3. Un seuil de complétude ne l'empêche pas (60 % passe 0.5)."
        ),
    )
    def test_numeric_column_with_nulls_yields_finite_metrics(self, raw_df):
        p = pl.Series(
            "p", [float(i % 7) if i < 30 else None for i in range(raw_df.height)]
        )
        out = analyze_grouped_distribution(raw_df.with_columns(p), seuil_completude=0.5)
        for col in ("M1", "M2", "M3"):
            assert np.isfinite(out[col].to_numpy()).all(), col


class TestEndToEndCoherence:
    def test_end_to_end_algorithm(self):
        df = pl.read_csv(DATA_PATH, try_parse_dates=True)
        out = analyze_grouped_distribution(df)

        relationships = out.filter(
            (pl.col("M1") > 0.50) & ((pl.col("M2") > 5) | (pl.col("M3") > 10))
        )
        relationships.write_parquet("/tmp/relationships.parquet")

        assert out.height == 13 * 12
        assert metric(out, "duree_jours", "indication", "M1") > 0.8

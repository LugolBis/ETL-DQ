import polars as pl

from metrics.completude import compute_completude


class TestCompletude:
    def test_compute_completude(self):
        df = pl.DataFrame(
            {
                "A": [1, 2, None, 4],  # 75 %
                "B": ["x", None, "y", "z"],  # 75 %
                "C": [1, 2, 3, 4],  # 100 %
            }
        )

        df_completude = compute_completude(df, ["A", "B", "C"], "SourceTest")

        assert df_completude["source"].to_list() == ["SourceTest"] * 3
        assert df_completude["column_name"].to_list() == ["A", "B", "C"]
        assert df_completude["completude_percent"].to_list() == [0.75, 0.75, 1.0]
        assert df_completude.schema["timestamp"] == pl.datatypes.Datetime("us", "UTC")

"""Unit tests for area_change estimator."""

import polars as pl
import pytest

from pyfia.estimation.estimators.area_change import (
    AreaChangeEstimator,
    area_change,
)


def change_rows(columns: dict) -> pl.LazyFrame:
    """Change-matrix rows, with subplot-footprint defaults for columns not given."""
    n = len(next(iter(columns.values())))
    defaults = {
        "SUBPTYP": [1] * n,
        "PROP_BASIS": ["SUBP"] * n,
        "CONDPROP_UNADJ": [1.0] * n,
        "COND_NONSAMPLE_REASN_CD": [None] * n,
        "PREV_COND_NONSAMPLE_REASN_CD": [None] * n,
        "ADJ_FACTOR_SUBP": [1.0] * n,
        "ADJ_FACTOR_MACR": [1.0] * n,
    }
    return pl.DataFrame({**defaults, **columns}).lazy()


class TestGetRequiredTables:
    """Tests for get_required_tables method."""

    def test_returns_required_tables(self):
        """Test that required tables are returned."""
        # Create a mock config
        config = {"land_type": "forest", "change_type": "net"}

        # We need a mock db object - create minimal mock
        class MockDB:
            db_path = "/fake/path"
            tables = {}

        estimator = AreaChangeEstimator(MockDB(), config)
        tables = estimator.get_required_tables()

        assert "SUBP_COND_CHNG_MTRX" in tables
        assert "COND" in tables
        assert "PLOT" in tables
        assert "POP_PLOT_STRATUM_ASSGN" in tables
        assert "POP_STRATUM" in tables
        assert len(tables) == 5


class TestGetCondColumns:
    """Tests for get_cond_columns method."""

    def test_forest_land_type(self):
        """Test condition columns for forest land type."""

        class MockDB:
            db_path = "/fake/path"
            tables = {}

        config = {"land_type": "forest", "change_type": "net"}
        estimator = AreaChangeEstimator(MockDB(), config)
        cols = estimator.get_cond_columns()

        assert "CN" in cols
        assert "PLT_CN" in cols
        assert "CONDID" in cols
        assert "COND_STATUS_CD" in cols
        assert "CONDPROP_UNADJ" in cols
        # Timberland columns should NOT be present
        assert "SITECLCD" not in cols
        assert "RESERVCD" not in cols

    def test_timber_land_type(self):
        """Test condition columns for timber land type."""

        class MockDB:
            db_path = "/fake/path"
            tables = {}

        config = {"land_type": "timber", "change_type": "net"}
        estimator = AreaChangeEstimator(MockDB(), config)
        cols = estimator.get_cond_columns()

        # Should include timberland columns
        assert "SITECLCD" in cols
        assert "RESERVCD" in cols

    def test_with_grp_by_string(self):
        """Test condition columns with single grouping column."""

        class MockDB:
            db_path = "/fake/path"
            tables = {}

        config = {"land_type": "forest", "grp_by": "FORTYPCD"}
        estimator = AreaChangeEstimator(MockDB(), config)
        cols = estimator.get_cond_columns()

        assert "FORTYPCD" in cols

    def test_with_grp_by_list(self):
        """Test condition columns with multiple grouping columns."""

        class MockDB:
            db_path = "/fake/path"
            tables = {}

        config = {"land_type": "forest", "grp_by": ["FORTYPCD", "OWNGRPCD"]}
        estimator = AreaChangeEstimator(MockDB(), config)
        cols = estimator.get_cond_columns()

        assert "FORTYPCD" in cols
        assert "OWNGRPCD" in cols

    def test_no_duplicate_columns(self):
        """Test that grouping columns don't duplicate core columns."""

        class MockDB:
            db_path = "/fake/path"
            tables = {}

        config = {"land_type": "forest", "grp_by": ["PLT_CN", "CONDID"]}
        estimator = AreaChangeEstimator(MockDB(), config)
        cols = estimator.get_cond_columns()

        # Count occurrences - should only appear once
        assert cols.count("PLT_CN") == 1
        assert cols.count("CONDID") == 1


class TestInLandType:
    """Tests for _in_land_type method."""

    class MockDB:
        db_path = "/fake/path"
        tables = {}

    def test_forest_land_type(self):
        """COND_STATUS_CD 1 is forest land, at either measurement."""
        estimator = AreaChangeEstimator(self.MockDB(), {"land_type": "forest"})
        df = pl.DataFrame(
            {
                "CURR_COND_STATUS_CD": [1, 2, 3, 4, 5, 1],
                "PREV_COND_STATUS_CD": [2, 1, 1, 1, 1, 1],
            }
        )

        result = df.select(
            estimator._in_land_type("current").alias("curr"),
            estimator._in_land_type("previous").alias("prev"),
        )

        assert result["curr"].to_list() == [True, False, False, False, False, True]
        assert result["prev"].to_list() == [False, True, True, True, True, True]

    def test_timber_land_type(self):
        """Timberland is productive (SITECLCD 1-6), unreserved forest land (#157)."""
        estimator = AreaChangeEstimator(self.MockDB(), {"land_type": "timber"})
        df = pl.DataFrame(
            {
                "CURR_COND_STATUS_CD": [1, 1, 1, 2, 1],
                "SITECLCD": [3, 7, 3, None, None],
                "RESERVCD": [0, 0, 1, None, 0],
                "PREV_COND_STATUS_CD": [1, 1, 2, 1, 1],
                "PREV_SITECLCD": [7, 6, None, 1, 2],
                "PREV_RESERVCD": [0, 0, None, 0, 1],
            }
        )

        result = df.select(
            estimator._in_land_type("current").alias("curr"),
            estimator._in_land_type("previous").alias("prev"),
        )

        assert result["curr"].to_list() == [True, False, False, False, False]
        assert result["prev"].to_list() == [False, True, False, True, False]


class TestCalculateValues:
    """Tests for calculate_values method."""

    @pytest.fixture
    def mock_estimator(self):
        """Create mock estimator for testing."""

        class MockDB:
            db_path = "/fake/path"
            tables = {}

        return MockDB()

    def test_net_change_calculation(self, mock_estimator):
        """Test net change calculation (gains - losses)."""
        config = {"change_type": "net", "land_type": "forest"}
        estimator = AreaChangeEstimator(mock_estimator, config)

        # Create test data
        df = change_rows(
            {
                "CURR_COND_STATUS_CD": [
                    1,
                    2,
                    1,
                    2,
                ],  # Current: forest, non-forest, forest, non-forest
                "PREV_COND_STATUS_CD": [
                    2,
                    1,
                    1,
                    2,
                ],  # Previous: non-forest, forest, forest, non-forest
                "SUBPTYP_PROP_CHNG": [1.0, 1.0, 1.0, 1.0],
            }
        )

        result = estimator.calculate_values(df).collect()

        # Row 0: non-forest -> forest = +1 (gain)
        # Row 1: forest -> non-forest = -1 (loss)
        # Row 2: forest -> forest = 0 (no change)
        # Row 3: non-forest -> non-forest = 0 (no change)
        expected = [1.0, -1.0, 0.0, 0.0]
        assert result["CHANGE_VALUE"].to_list() == expected

    def test_gross_gain_calculation(self, mock_estimator):
        """Test gross gain calculation (only gains)."""
        config = {"change_type": "gross_gain", "land_type": "forest"}
        estimator = AreaChangeEstimator(mock_estimator, config)

        df = change_rows(
            {
                "CURR_COND_STATUS_CD": [1, 2, 1, 2],
                "PREV_COND_STATUS_CD": [2, 1, 1, 2],
                "SUBPTYP_PROP_CHNG": [1.0, 1.0, 1.0, 1.0],
            }
        )

        result = estimator.calculate_values(df).collect()

        # Only count gains (non-forest -> forest)
        expected = [1.0, 0.0, 0.0, 0.0]
        assert result["CHANGE_VALUE"].to_list() == expected

    def test_gross_loss_calculation(self, mock_estimator):
        """Test gross loss calculation (only losses)."""
        config = {"change_type": "gross_loss", "land_type": "forest"}
        estimator = AreaChangeEstimator(mock_estimator, config)

        df = change_rows(
            {
                "CURR_COND_STATUS_CD": [1, 2, 1, 2],
                "PREV_COND_STATUS_CD": [2, 1, 1, 2],
                "SUBPTYP_PROP_CHNG": [1.0, 1.0, 1.0, 1.0],
            }
        )

        result = estimator.calculate_values(df).collect()

        # Only count losses (forest -> non-forest)
        expected = [0.0, 1.0, 0.0, 0.0]
        assert result["CHANGE_VALUE"].to_list() == expected

    def test_proportion_weighting(self, mock_estimator):
        """Test that SUBPTYP_PROP_CHNG weights the change value."""
        config = {"change_type": "net", "land_type": "forest"}
        estimator = AreaChangeEstimator(mock_estimator, config)

        df = change_rows(
            {
                "CURR_COND_STATUS_CD": [1, 1],
                "PREV_COND_STATUS_CD": [2, 2],
                "SUBPTYP_PROP_CHNG": [0.5, 0.25],
            }
        )

        result = estimator.calculate_values(df).collect()

        # Gains weighted by proportion
        expected = [0.5, 0.25]
        assert result["CHANGE_VALUE"].to_list() == expected

    def test_null_proportion_contributes_nothing(self, mock_estimator):
        """A null SUBPTYP_PROP_CHNG counts as zero, as in EVALIDator."""
        config = {"change_type": "net", "land_type": "forest"}
        estimator = AreaChangeEstimator(mock_estimator, config)

        df = change_rows(
            {
                "CURR_COND_STATUS_CD": [1],
                "PREV_COND_STATUS_CD": [2],
                "SUBPTYP_PROP_CHNG": [None],
            }
        )

        result = estimator.calculate_values(df).collect()

        assert result["CHANGE_VALUE"][0] == 0.0

    def test_macroplot_basis_uses_macr_adjustment(self, mock_estimator):
        """The adjustment factor follows the condition's PROP_BASIS (#151)."""
        config = {"change_type": "net", "land_type": "forest"}
        estimator = AreaChangeEstimator(mock_estimator, config)

        df = change_rows(
            {
                "CURR_COND_STATUS_CD": [1, 1],
                "PREV_COND_STATUS_CD": [2, 2],
                "SUBPTYP_PROP_CHNG": [1.0, 0.5],
                "PROP_BASIS": ["SUBP", "MACR"],
                "ADJ_FACTOR_SUBP": [1.2, 1.2],
                "ADJ_FACTOR_MACR": [3.0, 3.0],
            }
        )

        result = estimator.calculate_values(df).collect()

        assert result["CHANGE_VALUE"].to_list() == pytest.approx([1.2, 1.5])


class TestApplyFilters:
    """Tests for apply_filters method."""

    @pytest.fixture
    def mock_estimator(self):
        """Create mock estimator."""

        class MockDB:
            db_path = "/fake/path"
            tables = {}

        return MockDB()

    def test_filters_null_current_status(self, mock_estimator):
        """Test that null current status is filtered out."""
        config = {"land_type": "forest"}
        estimator = AreaChangeEstimator(mock_estimator, config)

        df = change_rows(
            {
                "CURR_COND_STATUS_CD": [1, None, 1],
                "PREV_COND_STATUS_CD": [2, 2, 2],
            }
        )

        result = estimator.apply_filters(df).collect()
        assert len(result) == 2

    def test_filters_null_previous_status(self, mock_estimator):
        """Test that null previous status is filtered out."""
        config = {"land_type": "forest"}
        estimator = AreaChangeEstimator(mock_estimator, config)

        df = change_rows(
            {
                "CURR_COND_STATUS_CD": [1, 1, 1],
                "PREV_COND_STATUS_CD": [2, None, 2],
            }
        )

        result = estimator.apply_filters(df).collect()
        assert len(result) == 2

    def test_filters_both_null_status(self, mock_estimator):
        """Test that rows with both statuses null are filtered out."""
        config = {"land_type": "forest"}
        estimator = AreaChangeEstimator(mock_estimator, config)

        df = change_rows(
            {
                "CURR_COND_STATUS_CD": [1, None, 1],
                "PREV_COND_STATUS_CD": [2, None, 2],
            }
        )

        result = estimator.apply_filters(df).collect()
        assert len(result) == 2

    def test_keeps_the_condition_footprint_row_only(self, mock_estimator):
        """One row per subplot: SUBPTYP 1 for SUBP conditions, 3 for MACR (#151)."""
        estimator = AreaChangeEstimator(mock_estimator, {"land_type": "forest"})

        df = change_rows(
            {
                "CURR_COND_STATUS_CD": [1] * 6,
                "PREV_COND_STATUS_CD": [2] * 6,
                "SUBPTYP": [1, 2, 3, 1, 3, 1],
                "PROP_BASIS": ["SUBP", "SUBP", "SUBP", "MACR", "MACR", None],
            }
        )

        result = estimator.apply_filters(df).collect()
        assert result.select("SUBPTYP", "PROP_BASIS").rows() == [
            (1, "SUBP"),
            (3, "MACR"),
        ]

    def test_drops_conditions_nonsampled_at_either_time(self, mock_estimator):
        """Nonsampled conditions leave the estimate, whichever time they're at."""
        estimator = AreaChangeEstimator(mock_estimator, {"land_type": "forest"})

        df = change_rows(
            {
                "CURR_COND_STATUS_CD": [1, 5, 2, 1],
                "PREV_COND_STATUS_CD": [2, 1, 5, 1],
                "COND_NONSAMPLE_REASN_CD": [None, 2, None, 0],
                "PREV_COND_NONSAMPLE_REASN_CD": [None, None, 3, None],
            }
        )

        result = estimator.apply_filters(df).collect()
        assert result["CURR_COND_STATUS_CD"].to_list() == [1, 1]

    def test_nonsample_reason_stored_as_text(self, mock_estimator):
        """Some state databases store COND_NONSAMPLE_REASN_CD as text."""
        estimator = AreaChangeEstimator(mock_estimator, {"land_type": "forest"})

        df = change_rows(
            {
                "CURR_COND_STATUS_CD": [1, 5, 1],
                "PREV_COND_STATUS_CD": [2, 1, 1],
                "COND_NONSAMPLE_REASN_CD": ["", "02", None],
                "PREV_COND_NONSAMPLE_REASN_CD": [None, None, "00"],
            }
        )

        result = estimator.apply_filters(df).collect()
        assert len(result) == 2


class TestAggregateToPlot:
    """Tests for aggregate_to_plot method."""

    @pytest.fixture
    def mock_estimator(self):
        """Create mock estimator."""

        class MockDB:
            db_path = "/fake/path"
            tables = {}

        return MockDB()

    def test_sums_change_values_per_plot(self, mock_estimator):
        """Test that change values are summed per plot."""
        config = {"land_type": "forest"}
        estimator = AreaChangeEstimator(mock_estimator, config)

        df = pl.DataFrame(
            {
                "PLT_CN": ["A", "A", "A", "A", "B", "B", "B", "B"],
                "STATECD": [13] * 8,
                "INVYR": [2023] * 8,
                "REMPER": [5.0] * 8,
                "STRATUM_CN": ["S1"] * 8,
                "EXPNS": [6000.0] * 8,
                "ADJ_FACTOR_SUBP": [1.0] * 8,
                "CHANGE_VALUE": [1.0, 0.0, 0.0, -1.0, 0.0, 0.0, 0.0, 0.0],
            }
        ).lazy()

        result = estimator.aggregate_to_plot(df).collect()

        # Should have 2 plots
        assert len(result) == 2

        # Check aggregation
        plot_a = result.filter(pl.col("PLT_CN") == "A")
        plot_b = result.filter(pl.col("PLT_CN") == "B")

        # Plot A: sum = 1 - 1 = 0, normalized = 0/4 = 0
        assert plot_a["PLOT_CHANGE_NORM"][0] == 0.0

        # Plot B: sum = 0, normalized = 0/4 = 0
        assert plot_b["PLOT_CHANGE_NORM"][0] == 0.0

    def test_normalizes_by_four_subplots(self, mock_estimator):
        """Test that change values are divided by 4 for normalization."""
        config = {"land_type": "forest"}
        estimator = AreaChangeEstimator(mock_estimator, config)

        df = pl.DataFrame(
            {
                "PLT_CN": ["A", "A", "A", "A"],
                "STATECD": [13] * 4,
                "INVYR": [2023] * 4,
                "REMPER": [5.0] * 4,
                "STRATUM_CN": ["S1"] * 4,
                "EXPNS": [6000.0] * 4,
                "ADJ_FACTOR_SUBP": [1.0] * 4,
                "CHANGE_VALUE": [1.0, 1.0, 1.0, 1.0],  # All 4 subplots gained
            }
        ).lazy()

        result = estimator.aggregate_to_plot(df).collect()

        # Sum = 4, normalized = 4/4 = 1.0
        assert result["PLOT_CHANGE_NORM"][0] == 1.0

    def test_includes_grouping_columns(self, mock_estimator):
        """Test that grouping columns are preserved."""
        config = {"land_type": "forest", "grp_by": "OWNGRPCD"}
        estimator = AreaChangeEstimator(mock_estimator, config)

        df = pl.DataFrame(
            {
                "PLT_CN": ["A", "A", "B", "B"],
                "STATECD": [13] * 4,
                "INVYR": [2023] * 4,
                "REMPER": [5.0] * 4,
                "STRATUM_CN": ["S1"] * 4,
                "EXPNS": [6000.0] * 4,
                "ADJ_FACTOR_SUBP": [1.0] * 4,
                "OWNGRPCD": [10, 10, 40, 40],
                "CHANGE_VALUE": [1.0, 0.0, 0.0, 0.0],
            }
        ).lazy()

        result = estimator.aggregate_to_plot(df).collect()

        assert "OWNGRPCD" in result.columns


class TestApplyExpansionFactors:
    """Tests for apply_expansion_factors method."""

    @pytest.fixture
    def mock_estimator(self):
        """Create mock estimator."""

        class MockDB:
            db_path = "/fake/path"
            tables = {}

        return MockDB()

    def test_applies_expansion_factor(self, mock_estimator):
        """Test that EXPNS is applied."""
        config = {"annual": False}
        estimator = AreaChangeEstimator(mock_estimator, config)

        df = pl.DataFrame(
            {
                "PLOT_CHANGE_NORM": [1.0],  # 1 normalized change
                "EXPNS": [6000.0],
                "ADJ_FACTOR_SUBP": [1.0],
                "REMPER": [5.0],
            }
        ).lazy()

        result = estimator.apply_expansion_factors(df).collect()

        # Without annual: 1.0 * 6000 * 1.0 = 6000
        assert result["AREA_CHANGE"][0] == 6000.0

    def test_annualization(self, mock_estimator):
        """Test that annualization divides by REMPER."""
        config = {"annual": True}
        estimator = AreaChangeEstimator(mock_estimator, config)

        df = pl.DataFrame(
            {
                "PLOT_CHANGE_NORM": [1.0],
                "EXPNS": [6000.0],
                "ADJ_FACTOR_SUBP": [1.0],
                "REMPER": [5.0],
            }
        ).lazy()

        result = estimator.apply_expansion_factors(df).collect()

        # With annual: 6000 / 5 = 1200
        assert result["AREA_CHANGE"][0] == 1200.0

    def test_no_annualization(self, mock_estimator):
        """Test that annual=False skips REMPER division."""
        config = {"annual": False}
        estimator = AreaChangeEstimator(mock_estimator, config)

        df = pl.DataFrame(
            {
                "PLOT_CHANGE_NORM": [1.0],
                "EXPNS": [6000.0],
                "ADJ_FACTOR_SUBP": [1.0],
                "REMPER": [5.0],
            }
        ).lazy()

        result = estimator.apply_expansion_factors(df).collect()

        # Without annual: not divided by REMPER
        assert result["AREA_CHANGE"][0] == 6000.0

    def test_adj_factor_not_reapplied(self, mock_estimator):
        """The adjustment factor is applied per row in calculate_values."""
        config = {"annual": False}
        estimator = AreaChangeEstimator(mock_estimator, config)

        df = pl.DataFrame(
            {
                "PLOT_CHANGE_NORM": [1.0],
                "EXPNS": [6000.0],
                "ADJ_FACTOR_SUBP": [1.1],  # 10% adjustment
                "REMPER": [5.0],
            }
        ).lazy()

        result = estimator.apply_expansion_factors(df).collect()

        assert result["AREA_CHANGE"][0] == 6000.0


class TestCalculateTotals:
    """Tests for calculate_totals method."""

    @pytest.fixture
    def mock_estimator(self):
        """Create mock estimator."""

        class MockDB:
            db_path = "/fake/path"
            tables = {}

        return MockDB()

    def test_sums_area_change(self, mock_estimator):
        """Test that area change values are summed."""
        config = {"land_type": "forest"}
        estimator = AreaChangeEstimator(mock_estimator, config)

        df = pl.DataFrame(
            {
                "PLT_CN": ["A", "B", "C"],
                "STATECD": [13, 13, 13],
                "AREA_CHANGE": [1000.0, -500.0, 200.0],
            }
        ).lazy()

        result = estimator.calculate_totals(df)

        # Sum: 1000 - 500 + 200 = 700
        assert result["AREA_CHANGE_TOTAL"][0] == 700.0

    def test_counts_unique_plots(self, mock_estimator):
        """Test that N_PLOTS counts unique plots."""
        config = {"land_type": "forest"}
        estimator = AreaChangeEstimator(mock_estimator, config)

        df = pl.DataFrame(
            {
                "PLT_CN": ["A", "B", "C"],
                "STATECD": [13, 13, 13],
                "AREA_CHANGE": [1000.0, -500.0, 200.0],
            }
        ).lazy()

        result = estimator.calculate_totals(df)

        assert result["N_PLOTS"][0] == 3

    def test_groups_by_state(self, mock_estimator):
        """Test that results are grouped by STATECD."""
        config = {"land_type": "forest"}
        estimator = AreaChangeEstimator(mock_estimator, config)

        df = pl.DataFrame(
            {
                "PLT_CN": ["A", "B", "C", "D"],
                "STATECD": [13, 13, 47, 47],
                "AREA_CHANGE": [1000.0, -500.0, 200.0, 300.0],
            }
        ).lazy()

        result = estimator.calculate_totals(df)

        assert len(result) == 2
        assert "STATECD" in result.columns

    def test_groups_by_custom_columns(self, mock_estimator):
        """Test grouping by custom columns."""
        config = {"land_type": "forest", "grp_by": "OWNGRPCD"}
        estimator = AreaChangeEstimator(mock_estimator, config)

        df = pl.DataFrame(
            {
                "PLT_CN": ["A", "B", "C", "D"],
                "STATECD": [13, 13, 13, 13],
                "OWNGRPCD": [10, 10, 40, 40],
                "AREA_CHANGE": [1000.0, 500.0, -200.0, -300.0],
            }
        ).lazy()

        result = estimator.calculate_totals(df)

        assert len(result) == 2
        assert "OWNGRPCD" in result.columns


class TestAreaChangeIntegration:
    """Integration tests for area_change function (requires database)."""

    @pytest.fixture
    def fia_db(self):
        """Get FIA database path."""
        import os
        from pathlib import Path

        # Try environment variable first
        env_path = os.getenv("PYFIA_DATABASE_PATH")
        if env_path:
            # MotherDuck connection strings don't need file existence check
            if env_path.startswith("md:") or env_path.startswith("motherduck:"):
                return env_path
            if Path(env_path).exists():
                return env_path

        # Try default location
        default_path = Path("data/georgia.duckdb")
        if default_path.exists():
            return str(default_path)

        # Fall back to MotherDuck if token available
        if os.getenv("MOTHERDUCK_TOKEN"):
            return "md:fia_ga_eval2023"

        pytest.skip("No FIA database found")

    def test_area_change_basic(self, fia_db):
        """Test basic area_change call."""
        from pyfia import FIA

        db = FIA(fia_db)
        db.clip_most_recent()

        result = area_change(db)

        assert not result.is_empty()
        assert "STATECD" in result.columns
        assert "AREA_CHANGE_TOTAL" in result.columns
        assert "N_PLOTS" in result.columns

    def test_area_change_net(self, fia_db):
        """Test net area change (default)."""
        from pyfia import FIA

        db = FIA(fia_db)
        db.clip_most_recent()

        result = area_change(db, change_type="net")

        # Net change can be positive or negative
        assert "AREA_CHANGE_TOTAL" in result.columns
        # Verify it's a real number, not null
        assert result["AREA_CHANGE_TOTAL"][0] is not None

    def test_area_change_gross_gain(self, fia_db):
        """Test gross gain calculation."""
        from pyfia import FIA

        db = FIA(fia_db)
        db.clip_most_recent()

        result = area_change(db, change_type="gross_gain")

        # Gross gain should be non-negative
        assert result["AREA_CHANGE_TOTAL"][0] >= 0

    def test_area_change_gross_loss(self, fia_db):
        """Test gross loss calculation."""
        from pyfia import FIA

        db = FIA(fia_db)
        db.clip_most_recent()

        result = area_change(db, change_type="gross_loss")

        # Gross loss should be non-negative (it's magnitude)
        assert result["AREA_CHANGE_TOTAL"][0] >= 0

    def test_net_equals_gain_minus_loss(self, fia_db):
        """Test that net = gross_gain - gross_loss."""
        from pyfia import FIA

        db = FIA(fia_db)
        db.clip_most_recent()

        net_result = area_change(db, change_type="net")
        gain_result = area_change(db, change_type="gross_gain")
        loss_result = area_change(db, change_type="gross_loss")

        net = net_result["AREA_CHANGE_TOTAL"][0]
        gain = gain_result["AREA_CHANGE_TOTAL"][0]
        loss = loss_result["AREA_CHANGE_TOTAL"][0]

        # net = gain - loss
        assert abs(net - (gain - loss)) < 1  # Allow small rounding difference

    def test_area_change_annual(self, fia_db):
        """Test annual vs non-annual calculation."""
        from pyfia import FIA

        db = FIA(fia_db)
        db.clip_most_recent()

        annual_result = area_change(db, annual=True)
        total_result = area_change(db, annual=False)

        # Non-annual should be larger (not divided by REMPER)
        # Since REMPER is typically 5-7 years, total should be ~5-7x annual
        annual_val = abs(annual_result["AREA_CHANGE_TOTAL"][0])
        total_val = abs(total_result["AREA_CHANGE_TOTAL"][0])

        if annual_val > 0:
            ratio = total_val / annual_val
            assert 2 < ratio < 10  # Should be roughly REMPER

    def test_area_change_with_grouping(self, fia_db):
        """Test area_change with grouping."""
        from pyfia import FIA

        db = FIA(fia_db)
        db.clip_most_recent()

        result = area_change(db, grp_by="OWNGRPCD")

        assert not result.is_empty()
        assert "OWNGRPCD" in result.columns
        assert len(result) > 1

    def test_area_change_with_multiple_grouping(self, fia_db):
        """Test area_change with multiple grouping columns."""
        from pyfia import FIA

        db = FIA(fia_db)
        db.clip_most_recent()

        result = area_change(db, grp_by=["STATECD", "OWNGRPCD"])

        assert "STATECD" in result.columns
        assert "OWNGRPCD" in result.columns

    def test_area_change_plots_count(self, fia_db):
        """Test that N_PLOTS is reasonable."""
        from pyfia import FIA

        db = FIA(fia_db)
        db.clip_most_recent()

        result = area_change(db)

        # Should have at least some plots
        n_plots = result["N_PLOTS"][0]
        assert n_plots > 0

    def test_state_code_correct(self, fia_db):
        """Test that STATECD is correct for database."""
        from pyfia import FIA

        db = FIA(fia_db)
        db.clip_most_recent()

        result = area_change(db)

        # Georgia = 13 (if using georgia.duckdb)
        assert "STATECD" in result.columns
        # Just verify it's a valid state code (1-56)
        statecd = result["STATECD"][0]
        assert 1 <= statecd <= 78


class TestAreaChangeEdgeCases:
    """Test edge cases for area_change."""

    @pytest.fixture
    def fia_db(self):
        """Get FIA database path."""
        import os
        from pathlib import Path

        env_path = os.getenv("PYFIA_DATABASE_PATH")
        if env_path:
            # MotherDuck connection strings don't need file existence check
            if env_path.startswith("md:") or env_path.startswith("motherduck:"):
                return env_path
            if Path(env_path).exists():
                return env_path

        default_path = Path("data/georgia.duckdb")
        if default_path.exists():
            return str(default_path)

        # Fall back to MotherDuck if token available
        if os.getenv("MOTHERDUCK_TOKEN"):
            return "md:fia_ga_eval2023"

        pytest.skip("No FIA database found")

    def test_area_change_sign(self, fia_db):
        """Test that signs are correct for gain/loss."""
        from pyfia import FIA

        db = FIA(fia_db)
        db.clip_most_recent()

        gain = area_change(db, change_type="gross_gain")["AREA_CHANGE_TOTAL"][0]
        loss = area_change(db, change_type="gross_loss")["AREA_CHANGE_TOTAL"][0]
        net = area_change(db, change_type="net")["AREA_CHANGE_TOTAL"][0]

        # Both gross values should be non-negative
        assert gain >= 0
        assert loss >= 0

        # Net can be any sign
        # If loss > gain, net should be negative
        if loss > gain:
            assert net < 0
        elif gain > loss:
            assert net > 0


class TestAreaChangeEstimatorInit:
    """Test AreaChangeEstimator initialization."""

    def test_init_stores_config(self):
        """Test that config is stored correctly."""

        class MockDB:
            db_path = "/fake/path"
            tables = {}

        config = {"land_type": "forest", "change_type": "net", "annual": True}
        estimator = AreaChangeEstimator(MockDB(), config)

        assert estimator.config == config

    def test_init_sets_plot_change_data_none(self):
        """Test that plot_change_data is initialized as None."""

        class MockDB:
            db_path = "/fake/path"
            tables = {}

        config = {}
        estimator = AreaChangeEstimator(MockDB(), config)

        assert estimator.plot_change_data is None

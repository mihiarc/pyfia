"""Tests for carbon pool estimation."""

import polars as pl
import pytest

from pyfia.estimation import carbon, biomass


class TestCarbonBasicEstimation:
    """Test basic carbon pool estimation functionality."""

    def test_carbon_live_pool(self, sample_fia_instance, sample_evaluation):
        """Test live tree carbon estimation (default pool)."""
        result = carbon(sample_fia_instance, pool="live")

        # Basic result validation
        assert isinstance(result, pl.DataFrame)
        assert len(result) > 0

        # Check required columns
        assert "CARBON_ACRE" in result.columns
        assert "POOL" in result.columns

        # Check values are reasonable
        estimate = result["CARBON_ACRE"][0]
        assert estimate > 0, "Carbon estimate should be positive"

        # Pool should be LIVE
        assert result["POOL"][0] == "LIVE"

    def test_carbon_ag_pool(self, sample_fia_instance, sample_evaluation):
        """Test aboveground carbon estimation."""
        result = carbon(sample_fia_instance, pool="ag")

        assert isinstance(result, pl.DataFrame)
        assert len(result) > 0
        assert result["CARBON_ACRE"][0] > 0
        assert result["POOL"][0] == "AG"

    def test_carbon_bg_pool(self, sample_fia_instance, sample_evaluation):
        """Test belowground carbon estimation."""
        result = carbon(sample_fia_instance, pool="bg")

        assert isinstance(result, pl.DataFrame)
        assert len(result) > 0
        assert result["CARBON_ACRE"][0] > 0
        assert result["POOL"][0] == "BG"


class TestCarbonComponentRelationships:
    """Test relationships between carbon pools."""

    def test_live_equals_ag_plus_bg(self, sample_fia_instance, sample_evaluation):
        """Live pool should approximately equal AG + BG."""
        live = carbon(sample_fia_instance, pool="live")
        ag = carbon(sample_fia_instance, pool="ag")
        bg = carbon(sample_fia_instance, pool="bg")

        live_carbon = live["CARBON_ACRE"][0]
        ag_carbon = ag["CARBON_ACRE"][0]
        bg_carbon = bg["CARBON_ACRE"][0]

        # Live should be very close to AG + BG (within 1% for rounding)
        expected = ag_carbon + bg_carbon
        diff_pct = abs(live_carbon - expected) / expected * 100
        assert diff_pct < 1.0, f"Live ({live_carbon}) != AG ({ag_carbon}) + BG ({bg_carbon})"

    def test_ag_larger_than_bg(self, sample_fia_instance, sample_evaluation):
        """Aboveground carbon should typically be larger than belowground."""
        ag = carbon(sample_fia_instance, pool="ag")
        bg = carbon(sample_fia_instance, pool="bg")

        # AG is typically ~4-5x larger than BG
        assert ag["CARBON_ACRE"][0] > bg["CARBON_ACRE"][0]


class TestCarbonVsBiomass:
    """Test carbon estimates match biomass-derived values."""

    def test_carbon_matches_biomass_carbon(self, sample_fia_instance, sample_evaluation):
        """Carbon should match CARB_ACRE from biomass estimator."""
        # Biomass returns CARB_ACRE which is already carbon (biomass * 0.47)
        bio = biomass(sample_fia_instance, component="TOTAL")
        carb = carbon(sample_fia_instance, pool="live")

        bio_carbon = bio["CARB_ACRE"][0]
        carb_carbon = carb["CARBON_ACRE"][0]

        # They should be the same since both apply 0.47 conversion
        diff_pct = abs(bio_carbon - carb_carbon) / bio_carbon * 100
        assert diff_pct < 0.1, f"Carbon ({carb_carbon}) != Biomass carbon ({bio_carbon})"

    def test_ag_carbon_matches_ag_biomass(self, sample_fia_instance, sample_evaluation):
        """AG carbon should match AG biomass CARB_ACRE."""
        bio_ag = biomass(sample_fia_instance, component="AG")
        carb_ag = carbon(sample_fia_instance, pool="ag")

        bio_carbon = bio_ag["CARB_ACRE"][0]
        carb_carbon = carb_ag["CARBON_ACRE"][0]

        diff_pct = abs(bio_carbon - carb_carbon) / bio_carbon * 100
        assert diff_pct < 0.1


class TestCarbonGrouping:
    """Test carbon estimation with grouping options."""

    def test_grp_by_forest_type(self, sample_fia_instance, sample_evaluation):
        """Grouping by forest type should return multiple rows."""
        result = carbon(sample_fia_instance, pool="live", grp_by="FORTYPCD")

        assert "FORTYPCD" in result.columns
        assert len(result) > 1  # Multiple forest types

    def test_grp_by_multiple_columns(self, sample_fia_instance, sample_evaluation):
        """Grouping by multiple columns should work."""
        result = carbon(sample_fia_instance, pool="ag", grp_by=["FORTYPCD", "OWNGRPCD"])

        assert "FORTYPCD" in result.columns
        assert "OWNGRPCD" in result.columns
        assert len(result) >= 1

    def test_by_species(self, sample_fia_instance, sample_evaluation):
        """by_species=True should group by SPCD."""
        result = carbon(sample_fia_instance, pool="live", by_species=True)

        assert "SPCD" in result.columns
        assert len(result) > 1  # Multiple species


class TestCarbonDomainFiltering:
    """Test carbon estimation with domain filtering."""

    def test_tree_domain_reduces_carbon(self, sample_fia_instance, sample_evaluation):
        """Tree domain filter should reduce total carbon."""
        # All trees
        all_trees = carbon(sample_fia_instance, pool="live", totals=True)

        # Only large trees (DIA >= 15 inches)
        large_trees = carbon(sample_fia_instance, pool="live", tree_domain="DIA >= 15.0", totals=True)

        # Large trees should have less total carbon
        assert large_trees["CARBON_TOTAL"][0] < all_trees["CARBON_TOTAL"][0]

    def test_species_domain(self, sample_fia_instance, sample_evaluation):
        """Species filtering via tree_domain should work."""
        # Filter to loblolly pine (SPCD = 131)
        pine = carbon(sample_fia_instance, pool="live", tree_domain="SPCD == 131")

        assert isinstance(pine, pl.DataFrame)
        assert len(pine) > 0


class TestCarbonVariance:
    """Test carbon variance estimation."""

    def test_variance_columns_present(self, sample_fia_instance, sample_evaluation):
        """variance=True should add SE columns."""
        result = carbon(sample_fia_instance, pool="live", variance=True, totals=True)

        assert "CARBON_ACRE_SE" in result.columns
        assert "CARBON_TOTAL_SE" in result.columns

    def test_variance_values_positive(self, sample_fia_instance, sample_evaluation):
        """Variance estimates should be positive."""
        result = carbon(sample_fia_instance, pool="live", variance=True)

        # SE can be 0 if only one plot, but should be defined
        assert result["CARBON_ACRE_SE"][0] >= 0


class TestDeadPool:
    """Test dead carbon pool estimation."""

    def test_dead_pool_returns_values(self, sample_fia_instance, sample_evaluation):
        """Dead pool should return non-negative values with warning."""
        with pytest.warns(UserWarning, match="standing dead"):
            result = carbon(sample_fia_instance, pool="dead")

        assert "CARBON_ACRE" in result.columns
        assert result["POOL"][0] == "DEAD"
        assert result["CARBON_ACRE"][0] >= 0  # Could be 0 if no dead trees


class TestTotalPool:
    """Test total ecosystem carbon pool."""

    def test_total_pool_warns_incomplete(self, sample_fia_instance, sample_evaluation):
        """Total pool should warn about incomplete implementation."""
        with pytest.warns(UserWarning, match="not fully implemented"):
            result = carbon(sample_fia_instance, pool="total")

        assert "CARBON_ACRE" in result.columns
        assert result["CARBON_ACRE"][0] > 0


class TestNotImplementedPools:
    """Test pools that are not yet implemented."""

    def test_litter_raises_not_implemented(self, sample_fia_instance, sample_evaluation):
        """Litter pool should raise NotImplementedError."""
        with pytest.raises(NotImplementedError, match="Litter"):
            carbon(sample_fia_instance, pool="litter")

    def test_soil_raises_not_implemented(self, sample_fia_instance, sample_evaluation):
        """Soil pool should raise NotImplementedError."""
        with pytest.raises(NotImplementedError, match="Soil"):
            carbon(sample_fia_instance, pool="soil")


class TestInvalidInputs:
    """Test handling of invalid inputs."""

    def test_invalid_pool_raises_error(self, sample_fia_instance, sample_evaluation):
        """Invalid pool should raise ValueError."""
        with pytest.raises(ValueError, match="Invalid pool"):
            carbon(sample_fia_instance, pool="invalid")

    def test_pool_case_insensitive(self, sample_fia_instance, sample_evaluation):
        """Pool name should be case-insensitive."""
        lower = carbon(sample_fia_instance, pool="live")
        upper = carbon(sample_fia_instance, pool="LIVE")
        mixed = carbon(sample_fia_instance, pool="Live")

        # All should return same values (use pytest.approx for float comparison)
        assert lower["CARBON_ACRE"][0] == pytest.approx(upper["CARBON_ACRE"][0])
        assert lower["CARBON_ACRE"][0] == pytest.approx(mixed["CARBON_ACRE"][0])


class TestLandType:
    """Test land type filtering."""

    def test_forest_land_type(self, sample_fia_instance, sample_evaluation):
        """Default forest land type should work."""
        result = carbon(sample_fia_instance, pool="live", land_type="forest")
        assert result["CARBON_ACRE"][0] > 0

    def test_timber_subset_of_forest(self, sample_fia_instance, sample_evaluation):
        """Timberland should have less or equal carbon than forestland."""
        forest = carbon(sample_fia_instance, pool="live", land_type="forest", totals=True)
        timber = carbon(sample_fia_instance, pool="live", land_type="timber", totals=True)

        # Timberland is a subset of forestland
        assert timber["CARBON_TOTAL"][0] <= forest["CARBON_TOTAL"][0]


class TestOutputOptions:
    """Test output options (totals, most_recent)."""

    def test_totals_true(self, sample_fia_instance, sample_evaluation):
        """totals=True should include CARBON_TOTAL."""
        result = carbon(sample_fia_instance, pool="live", totals=True)
        assert "CARBON_TOTAL" in result.columns

    def test_totals_false(self, sample_fia_instance, sample_evaluation):
        """totals=False should exclude CARBON_TOTAL."""
        result = carbon(sample_fia_instance, pool="live", totals=False)
        assert "CARBON_TOTAL" not in result.columns

    def test_most_recent(self, sample_fia_instance, sample_evaluation):
        """most_recent=True should work."""
        result = carbon(sample_fia_instance, pool="live", most_recent=True)
        assert isinstance(result, pl.DataFrame)
        assert result["CARBON_ACRE"][0] > 0

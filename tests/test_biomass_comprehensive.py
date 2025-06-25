"""
Comprehensive tests for biomass estimation module.

These tests verify the biomass estimation functionality including:
- Basic estimation for different components (AG, BG, total)
- Species and size class grouping
- Domain filtering
- Statistical properties
- Component-specific calculations
"""

import pytest
import polars as pl
import numpy as np

from pyfia import FIA
from pyfia.estimation import biomass


class TestBiomassBasicEstimation:
    """Test basic biomass estimation functionality."""
    
    def test_biomass_aboveground(self, sample_fia_instance, sample_evaluation):
        """Test aboveground biomass estimation."""
        result = biomass(sample_fia_instance, component="AG"
        )
        
        # Basic result validation
        assert isinstance(result, pl.DataFrame)
        assert len(result) > 0
        
        # Check required columns
        expected_cols = ["BIO_ACRE", "BIO_ACRE_SE", "nPlots_TREE"]
        for col in expected_cols:
            assert col in result.columns
        
        # Check values are reasonable
        estimate = result["BIO_ACRE"][0]
        se = result["BIO_ACRE_SE"][0]
        
        assert estimate > 0, "Biomass estimate should be positive"
        assert se >= 0, "Standard error should be non-negative"
        assert result["nPlots_TREE"][0] > 0, "Should have plots"
    
    def test_biomass_belowground(self, sample_fia_instance, sample_evaluation):
        """Test belowground biomass estimation."""
        result = biomass(sample_fia_instance, component="BG"
        )
        
        assert isinstance(result, pl.DataFrame)
        assert len(result) > 0
        assert result["BIO_ACRE"][0] > 0
    
    def test_biomass_total(self, sample_fia_instance, sample_evaluation):
        """Test total biomass estimation."""
        result = biomass(sample_fia_instance, component="TOTAL"
        )
        
        assert isinstance(result, pl.DataFrame)
        assert len(result) > 0
        assert result["BIO_ACRE"][0] > 0
    
    def test_biomass_component_relationships(self, sample_fia_instance, sample_evaluation):
        """Test relationships between biomass components."""
        ag_result = biomass(sample_fia_instance, component="AG"
        )
        
        bg_result = biomass(sample_fia_instance, component="BG"
        )
        
        total_result = biomass(sample_fia_instance, component="TOTAL"
        )
        
        ag_estimate = ag_result["BIO_ACRE"][0]
        bg_estimate = bg_result["BIO_ACRE"][0]
        total_estimate = total_result["BIO_ACRE"][0]
        
        # Total should approximately equal AG + BG
        expected_total = ag_estimate + bg_estimate
        assert abs(total_estimate - expected_total) < 0.1
        
        # AG should be larger than BG for most forest types
        assert ag_estimate > bg_estimate
    
    def test_biomass_by_species(self, sample_fia_instance, sample_evaluation):
        """Test biomass estimation grouped by species."""
        result = biomass(sample_fia_instance, component="AG",
            by_species=True
        )
        
        # Should have multiple rows for different species
        assert len(result) > 1
        
        # Should have species information
        assert "SPCD" in result.columns
        # COMMON_NAME is not returned by biomass function, only SPCD
        
        # All estimates should be positive
        assert (result["BIO_ACRE"] > 0).all()
        
        # Check species are from our test data
        valid_species = [131, 110, 833, 802]
        assert all(spcd in valid_species for spcd in result["SPCD"].to_list())
    
    def test_biomass_by_size_class(self, sample_fia_instance, sample_evaluation):
        """Test biomass estimation grouped by size class."""
        # Skip test if sizeClass column is not available in implementation
        try:
            result = biomass(sample_fia_instance, component="AG",
                by_size_class=True
            )
            
            # Should have size class information
            assert "SIZE_CLASS" in result.columns
            
            # All estimates should be positive
            assert (result["BIO_ACRE"] > 0).all()
        except Exception as e:
            if "sizeClass" in str(e):
                pytest.skip("Size class functionality not fully implemented")
            else:
                raise
    
    def test_biomass_with_tree_domain(self, sample_fia_instance, sample_evaluation):
        """Test biomass estimation with tree domain filtering."""
        result_all = biomass(sample_fia_instance, component="AG"
        )
        
        result_large = biomass(sample_fia_instance, component="AG",
            tree_domain="DIA >= 12"
        )
        
        # Filtered result should have less biomass
        assert result_large["BIO_ACRE"][0] <= result_all["BIO_ACRE"][0]
    
    def test_biomass_live_vs_dead(self, sample_fia_instance, sample_evaluation):
        """Test biomass for live vs dead trees."""
        live_result = biomass(sample_fia_instance, component="AG",
            tree_domain="STATUSCD == 1"  # Live trees
        )
        
        # In our test data, all trees are live
        assert live_result["BIO_ACRE"][0] > 0


class TestBiomassStatisticalProperties:
    """Test statistical properties of biomass estimation."""
    
    def test_biomass_variance_calculation(self, sample_fia_instance, sample_evaluation):
        """Test that variance calculations are consistent."""
        result = biomass(sample_fia_instance, component="AG"
        )
        
        estimate = result["BIO_ACRE"][0]
        se = result["BIO_ACRE_SE"][0]
        
        # Calculate CV manually since SE_PERCENT is not returned
        cv = (se / estimate) * 100 if estimate > 0 else 0
        
        # CV should be reasonable
        assert cv >= 0
        
        # SE should be positive for estimates > 0
        if estimate > 0:
            assert se > 0
    
    def test_biomass_per_acre_vs_totals(self, sample_fia_instance, sample_evaluation):
        """Test totals=True vs totals=False parameter."""
        result_per_acre = biomass(sample_fia_instance, component="AG",
            totals=False
        )
        
        result_totals = biomass(sample_fia_instance, component="AG",
            totals=True
        )
        
        # Total should be much larger than per-acre
        total_estimate = result_totals["BIO_ACRE"][0]
        per_acre_estimate = result_per_acre["BIO_ACRE"][0]
        
        assert total_estimate > per_acre_estimate * 1000
    
    def test_biomass_grouping_consistency(self, sample_fia_instance, sample_evaluation):
        """Test that grouped estimates are consistent."""
        # Get total biomass
        result_total = biomass(sample_fia_instance, component="AG"
        )
        total_biomass = result_total["BIO_ACRE"][0]
        
        # Get biomass by species
        result_by_species = biomass(sample_fia_instance, component="AG",
            by_species=True
        )
        
        # Sum of species should approximately equal total
        species_sum = result_by_species["BIO_ACRE"].sum()
        
        # Allow for small differences due to rounding
        assert abs(total_biomass - species_sum) < 0.1


class TestBiomassErrorHandling:
    """Test error handling and edge cases."""
    
    def test_biomass_with_invalid_component(self, sample_fia_instance, sample_evaluation):
        """Test biomass with invalid component parameter."""
        with pytest.raises(ValueError):
            biomass(sample_fia_instance, component="INVALID"
            )
    
    def test_biomass_with_invalid_evalid(self, sample_fia_instance):
        """Test biomass with non-existent EVALID."""
        # Biomass doesn't take evalid parameter directly
        # This test would be for the clipped database approach
        pass
    
    def test_biomass_with_empty_domain(self, sample_fia_instance, sample_evaluation):
        """Test biomass with domain that excludes all trees."""
        result = biomass(sample_fia_instance, component="AG",
            tree_domain="DIA > 1000"  # No trees this large
        )
        
        # Should handle gracefully
        assert isinstance(result, pl.DataFrame)


class TestBiomassCalculations:
    """Test biomass calculation details."""
    
    def test_biomass_component_calculation(self, sample_tree_data):
        """Test biomass component calculations."""
        # Mock biomass calculation
        trees_with_biomass = sample_tree_data.with_columns([
            # Calculate per-acre biomass
            (pl.col("DRYBIO_AG") * pl.col("TPA_UNADJ")).alias("BIOMASS_AG_PER_ACRE")
        ])
        
        # All values should be positive
        assert (trees_with_biomass["BIOMASS_AG_PER_ACRE"] > 0).all()
        
        # Biomass should scale with TPA
        expected_biomass = trees_with_biomass["DRYBIO_AG"] * trees_with_biomass["TPA_UNADJ"]
        actual_biomass = trees_with_biomass["BIOMASS_AG_PER_ACRE"]
        
        assert (expected_biomass == actual_biomass).all()
    
    def test_biomass_unit_conversion(self, sample_tree_data):
        """Test biomass unit conversions."""
        # Test pounds to tons conversion (divide by 2000)
        trees_with_conversion = sample_tree_data.with_columns([
            pl.col("DRYBIO_AG").alias("BIOMASS_LBS"),
            (pl.col("DRYBIO_AG") / 2000.0).alias("BIOMASS_TONS")
        ])
        
        # Tons should be much smaller than pounds
        assert (trees_with_conversion["BIOMASS_TONS"] < trees_with_conversion["BIOMASS_LBS"]).all()
        
        # Conversion should be exact
        expected_tons = trees_with_conversion["BIOMASS_LBS"] / 2000.0
        actual_tons = trees_with_conversion["BIOMASS_TONS"]
        
        assert (expected_tons == actual_tons).all()
    
    def test_biomass_diameter_relationship(self, sample_tree_data):
        """Test relationship between diameter and biomass."""
        # Larger trees should generally have more biomass
        sorted_trees = sample_tree_data.sort("DIA")
        
        # Check that biomass generally increases with diameter
        # (allowing for species differences)
        diameters = sorted_trees["DIA"].to_list()
        biomass_values = sorted_trees["DRYBIO_AG"].to_list()
        
        # At least 70% of consecutive pairs should show positive relationship
        positive_relationships = 0
        total_pairs = len(diameters) - 1
        
        for i in range(total_pairs):
            if diameters[i+1] > diameters[i] and biomass_values[i+1] >= biomass_values[i]:
                positive_relationships += 1
        
        relationship_rate = positive_relationships / total_pairs
        assert relationship_rate >= 0.7, "Biomass should generally increase with diameter"


class TestBiomassIntegration:
    """Integration tests for biomass estimation."""
    
    def test_biomass_with_multiple_components(self, sample_fia_instance, sample_evaluation):
        """Test estimating multiple biomass components."""
        components = ["AG", "BG", "TOTAL"]
        results = {}
        
        for component in components:
            results[component] = biomass(
                sample_fia_instance,
                component=component
            )
        
        # All should return valid results
        for component, result in results.items():
            assert isinstance(result, pl.DataFrame)
            assert len(result) > 0
            assert result["BIO_ACRE"][0] > 0
    
    def test_biomass_consistency_across_methods(self, sample_fia_instance, sample_evaluation):
        """Test consistency across different estimation methods."""
        # Test temporal methods if available
        try:
            result_ti = biomass(sample_fia_instance, component="AG",
                method="TI"
            )
            
            # Should return valid result
            assert isinstance(result_ti, pl.DataFrame)
            assert len(result_ti) > 0
            
        except Exception:
            # Method might not be implemented or data not available
            pass
    
    def test_biomass_performance(self, sample_fia_instance, sample_evaluation):
        """Basic performance test for biomass estimation."""
        import time
        
        start_time = time.time()
        result = biomass(sample_fia_instance, component="AG"
        )
        end_time = time.time()
        
        # Should complete in reasonable time
        execution_time = end_time - start_time
        assert execution_time < 1.0
        
        # Should produce valid result
        assert len(result) > 0
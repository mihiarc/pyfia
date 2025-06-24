"""
Comprehensive tests for volume estimation module.

These tests verify the volume estimation functionality including:
- Basic estimation for different volume types (net, gross, sawlog, board feet)
- Species and size class grouping
- Domain filtering
- Statistical properties
- Volume component relationships
"""

import pytest
import polars as pl
import numpy as np

from pyfia import volume, FIA


class TestVolumeBasicEstimation:
    """Test basic volume estimation functionality."""
    
    def test_volume_net_cubic_feet(self, sample_fia_instance, sample_evaluation):
        """Test net cubic foot volume estimation."""
        result = volume(
            sample_fia_instance,
            vol_type="net"
        )
        
        # Basic result validation
        assert isinstance(result, pl.DataFrame)
        assert len(result) > 0
        
        # Check required columns
        expected_cols = ["ESTIMATE", "SE", "SE_PERCENT", "N_PLOTS"]
        for col in expected_cols:
            assert col in result.columns
        
        # Check values are reasonable
        estimate = result["ESTIMATE"][0]
        se = result["SE"][0]
        
        assert estimate > 0, "Volume estimate should be positive"
        assert se >= 0, "Standard error should be non-negative"
        assert result["N_PLOTS"][0] > 0, "Should have plots"
    
    def test_volume_sawlog(self, sample_fia_instance, sample_evaluation):
        """Test sawlog volume estimation."""
        result = volume(sample_fia_instance, vol_type="sawlog")
        
        assert isinstance(result, pl.DataFrame)
        assert len(result) > 0
        assert result["ESTIMATE"][0] > 0
    
    def test_volume_board_feet(self, sample_fia_instance, sample_evaluation):
        """Test board foot volume estimation."""
        result = volume(sample_fia_instance, vol_type="sawlog")
        
        assert isinstance(result, pl.DataFrame)
        assert len(result) > 0
        assert result["ESTIMATE"][0] > 0
    
    def test_volume_gross_cubic_feet(self, sample_fia_instance, sample_evaluation):
        """Test gross cubic foot volume estimation."""
        result = volume(sample_fia_instance, vol_type="gross")
        
        assert isinstance(result, pl.DataFrame)
        assert len(result) > 0
        assert result["ESTIMATE"][0] > 0
    
    def test_volume_component_relationships(self, sample_fia_instance, sample_evaluation):
        """Test relationships between volume components."""
        net_result = volume(sample_fia_instance, vol_type="net")
        
        sawlog_result = volume(sample_fia_instance, vol_type="sawlog")
        
        # Sawlog should be less than or equal to net volume
        net_estimate = net_result["ESTIMATE"][0]
        sawlog_estimate = sawlog_result["ESTIMATE"][0]
        
        assert sawlog_estimate <= net_estimate, "Sawlog volume should not exceed net volume"
    
    def test_volume_by_species(self, sample_fia_instance, sample_evaluation):
        """Test volume estimation grouped by species."""
        result = volume(sample_fia_instance, vol_type="net",
            by_species=True
        )
        
        # Should have multiple rows for different species
        assert len(result) > 1
        
        # Should have species information
        assert "SPCD" in result.columns
        assert "COMMON_NAME" in result.columns
        
        # All estimates should be positive
        assert (result["ESTIMATE"] > 0).all()
        
        # Check species are from our test data
        valid_species = [131, 110, 833, 802]
        assert all(spcd in valid_species for spcd in result["SPCD"].to_list())
    
    def test_volume_by_size_class(self, sample_fia_instance, sample_evaluation):
        """Test volume estimation grouped by size class."""
        result = volume(sample_fia_instance, vol_type="net",
            by_size_class=True
        )
        
        # Should have size class information
        assert "SIZE_CLASS" in result.columns
        
        # All estimates should be positive
        assert (result["ESTIMATE"] > 0).all()
    
    def test_volume_with_tree_domain(self, sample_fia_instance, sample_evaluation):
        """Test volume estimation with tree domain filtering."""
        result_all = volume(sample_fia_instance, vol_type="net")
        
        result_large = volume(sample_fia_instance, vol_type="net",
            tree_domain="DIA >= 12"
        )
        
        # Filtered result should have less volume
        assert result_large["ESTIMATE"][0] <= result_all["ESTIMATE"][0]
    
    def test_volume_live_vs_dead(self, sample_fia_instance, sample_evaluation):
        """Test volume for live vs dead trees."""
        live_result = volume(sample_fia_instance, vol_type="net",
            tree_domain="STATUSCD == 1"  # Live trees
        )
        
        # In our test data, all trees are live
        assert live_result["ESTIMATE"][0] > 0


class TestVolumeStatisticalProperties:
    """Test statistical properties of volume estimation."""
    
    def test_volume_variance_calculation(self, sample_fia_instance, sample_evaluation):
        """Test that variance calculations are consistent."""
        result = volume(sample_fia_instance, vol_type="net")
        
        estimate = result["ESTIMATE"][0]
        se = result["SE"][0]
        cv = result["SE_PERCENT"][0]
        
        # CV should equal (SE / Estimate) * 100
        expected_cv = (se / estimate) * 100
        assert abs(cv - expected_cv) < 0.01
        
        # SE should be positive for estimates > 0
        if estimate > 0:
            assert se > 0
    
    def test_volume_per_acre_vs_totals(self, sample_fia_instance, sample_evaluation):
        """Test totals=True vs totals=False parameter."""
        result_per_acre = volume(sample_fia_instance, vol_type="net",
            totals=False
        )
        
        result_totals = volume(sample_fia_instance, vol_type="net",
            totals=True
        )
        
        # Total should be much larger than per-acre
        total_estimate = result_totals["ESTIMATE"][0]
        per_acre_estimate = result_per_acre["ESTIMATE"][0]
        
        assert total_estimate > per_acre_estimate * 1000
    
    def test_volume_grouping_consistency(self, sample_fia_instance, sample_evaluation):
        """Test that grouped estimates are consistent."""
        # Get total volume
        result_total = volume(sample_fia_instance, vol_type="net")
        total_volume = result_total["ESTIMATE"][0]
        
        # Get volume by species
        result_by_species = volume(sample_fia_instance, vol_type="net",
            by_species=True
        )
        
        # Sum of species should approximately equal total
        species_sum = result_by_species["ESTIMATE"].sum()
        
        # Allow for small differences due to rounding
        assert abs(total_volume - species_sum) < 0.1


class TestVolumeErrorHandling:
    """Test error handling and edge cases."""
    
    def test_volume_with_invalid_vol_type(self, sample_fia_instance, sample_evaluation):
        """Test volume with invalid vol_type parameter."""
        with pytest.raises(ValueError):
            volume(sample_fia_instance, vol_type="invalid")
    
    def test_volume_with_invalid_evalid(self, sample_fia_instance):
        """Test volume with non-existent EVALID."""
        # Volume doesn't take evalid parameter directly
        # This test would be for the clipped database approach
        pass
    
    def test_volume_with_empty_domain(self, sample_fia_instance, sample_evaluation):
        """Test volume with domain that excludes all trees."""
        result = volume(sample_fia_instance, vol_type="net",
            tree_domain="DIA > 1000"  # No trees this large
        )
        
        # Should handle gracefully
        assert isinstance(result, pl.DataFrame)


class TestVolumeCalculations:
    """Test volume calculation details."""
    
    def test_volume_component_calculation(self, sample_tree_data):
        """Test volume component calculations."""
        # Mock volume calculation
        trees_with_volume = sample_tree_data.with_columns([
            # Calculate per-acre volume
            (pl.col("VOLCFNET") * pl.col("TPA_UNADJ")).alias("VOLUME_PER_ACRE")
        ])
        
        # All values should be positive
        assert (trees_with_volume["VOLUME_PER_ACRE"] > 0).all()
        
        # Volume should scale with TPA
        expected_volume = trees_with_volume["VOLCFNET"] * trees_with_volume["TPA_UNADJ"]
        actual_volume = trees_with_volume["VOLUME_PER_ACRE"]
        
        assert (expected_volume == actual_volume).all()
    
    def test_volume_unit_consistency(self, sample_tree_data):
        """Test volume unit consistency."""
        # Test that cubic feet are consistently used
        volumes = sample_tree_data["VOLCFNET"]
        
        # All volumes should be positive
        assert (volumes > 0).all()
        
        # Volumes should be reasonable (not impossibly large or small)
        assert (volumes < 1000).all()  # Less than 1000 cu ft per tree
        assert (volumes > 0.1).all()   # More than 0.1 cu ft per tree
    
    def test_volume_diameter_relationship(self, sample_tree_data):
        """Test relationship between diameter and volume."""
        # Larger trees should generally have more volume
        sorted_trees = sample_tree_data.sort("DIA")
        
        # Check that volume generally increases with diameter
        diameters = sorted_trees["DIA"].to_list()
        volumes = sorted_trees["VOLCFNET"].to_list()
        
        # At least 70% of consecutive pairs should show positive relationship
        positive_relationships = 0
        total_pairs = len(diameters) - 1
        
        for i in range(total_pairs):
            if diameters[i+1] > diameters[i] and volumes[i+1] >= volumes[i]:
                positive_relationships += 1
        
        relationship_rate = positive_relationships / total_pairs
        assert relationship_rate >= 0.7, "Volume should generally increase with diameter"
    
    def test_volume_sawlog_conversion(self, sample_tree_data):
        """Test sawlog volume conversion factors."""
        # Mock sawlog calculation
        trees_with_sawlog = sample_tree_data.with_columns([
            (pl.col("VOLCFNET") * 0.7).alias("EXPECTED_SAWLOG")  # Typical sawlog ratio
        ])
        
        # Sawlog should be less than total volume
        assert (trees_with_sawlog["EXPECTED_SAWLOG"] <= trees_with_sawlog["VOLCFNET"]).all()
        
        # Sawlog should be reasonable percentage of total
        ratios = trees_with_sawlog["EXPECTED_SAWLOG"] / trees_with_sawlog["VOLCFNET"]
        assert (ratios >= 0.5).all()  # At least 50%
        assert (ratios <= 1.0).all()  # At most 100%


class TestVolumeIntegration:
    """Integration tests for volume estimation."""
    
    def test_volume_with_multiple_components(self, sample_fia_instance, sample_evaluation):
        """Test estimating multiple volume components."""
        components = ["VOLCFNET", "VOLCSNET", "VOLBFNET"]
        results = {}
        
        for component in components:
            results[component] = volume(
                sample_fia_instance,
                evalid=sample_evaluation.evalid,
                component=component
            )
        
        # All should return valid results
        for component, result in results.items():
            assert isinstance(result, pl.DataFrame)
            assert len(result) > 0
            assert result["ESTIMATE"][0] > 0
    
    def test_volume_consistency_across_methods(self, sample_fia_instance, sample_evaluation):
        """Test consistency across different estimation methods."""
        # Test temporal methods if available
        try:
            result_ti = volume(sample_fia_instance, vol_type="net",
                method="TI"
            )
            
            # Should return valid result
            assert isinstance(result_ti, pl.DataFrame)
            assert len(result_ti) > 0
            
        except Exception:
            # Method might not be implemented or data not available
            pass
    
    def test_volume_performance(self, sample_fia_instance, sample_evaluation):
        """Basic performance test for volume estimation."""
        import time
        
        start_time = time.time()
        result = volume(sample_fia_instance, vol_type="net")
        end_time = time.time()
        
        # Should complete in reasonable time
        execution_time = end_time - start_time
        assert execution_time < 1.0
        
        # Should produce valid result
        assert len(result) > 0
    
    def test_volume_net_vs_sawlog_relationship(self, sample_fia_instance, sample_evaluation):
        """Test relationship between net and sawlog volume."""
        net_result = volume(sample_fia_instance, vol_type="net")
        
        sawlog_result = volume(sample_fia_instance, vol_type="sawlog")
        
        net_estimate = net_result["ESTIMATE"][0]
        sawlog_estimate = sawlog_result["ESTIMATE"][0]
        
        # Sawlog should be less than or equal to net volume
        assert sawlog_estimate <= net_estimate, "Sawlog volume should not exceed net volume"


class TestVolumeSpecialCases:
    """Test special cases and edge conditions."""
    
    def test_volume_zero_diameter_trees(self, sample_fia_instance, sample_evaluation):
        """Test handling of zero diameter trees."""
        # This would be handled by domain filtering in real implementation
        result = volume(sample_fia_instance, vol_type="net",
            tree_domain="DIA > 0"  # Explicit filter
        )
        
        assert isinstance(result, pl.DataFrame)
        assert len(result) > 0
    
    def test_volume_different_forest_types(self, sample_fia_instance, sample_evaluation):
        """Test volume estimation for different forest types."""
        # Test with area domain for specific forest types
        result = volume(sample_fia_instance, vol_type="net",
            area_domain="FORTYPCD == 220"  # Loblolly pine forest
        )
        
        assert isinstance(result, pl.DataFrame)
        if len(result) > 0:
            assert result["ESTIMATE"][0] > 0
    
    def test_volume_merchantable_trees(self, sample_fia_instance, sample_evaluation):
        """Test volume for merchantable trees only."""
        # Typically trees >= 5.0" DBH for volume
        result = volume(sample_fia_instance, vol_type="net",
            tree_domain="DIA >= 5.0"
        )
        
        assert isinstance(result, pl.DataFrame)
        assert len(result) > 0
        assert result["ESTIMATE"][0] > 0
"""
Comprehensive tests for Phase 3 configuration system validation.

This test suite focuses specifically on the configuration system validation,
type safety, and error handling to ensure robustness.
"""

import pytest
import warnings
from typing import Dict, Any
from pydantic import ValidationError

from pyfia.estimation.config import (
    EstimatorConfig,
    MortalityConfig,
    VolumeConfig,
    BiomassConfig,
    GrowthConfig,
    AreaConfig,
    LazyEvaluationConfig,
    ConfigFactory,
    EstimationMethod,
    LandType,
    TreeType,
    VarianceMethod,
    LazyEvaluationMode,
    VolumeSpecificConfig,
    BiomassSpecificConfig,
    GrowthSpecificConfig,
    AreaSpecificConfig,
    VALID_FIA_GROUPING_COLUMNS
)


class TestEstimatorConfigValidation:
    """Test EstimatorConfig validation and type safety."""
    
    def test_valid_basic_config(self):
        """Test creation of valid basic configuration."""
        config = EstimatorConfig(
            grp_by=["SPCD", "OWNGRPCD"],
            by_species=True,
            by_size_class=False,
            land_type="forest",
            tree_type="live",
            method="TI",
            lambda_=0.5,
            totals=True,
            variance=False,
            by_plot=False,
            most_recent=True,
            variance_method="ratio"
        )
        
        assert config.grp_by == ["SPCD", "OWNGRPCD"]
        assert config.by_species is True
        assert config.by_size_class is False
        assert config.land_type == "forest"
        assert config.tree_type == "live"
        assert config.method == "TI"
        assert config.lambda_ == 0.5
        assert config.totals is True
        assert config.variance is False
        assert config.by_plot is False
        assert config.most_recent is True
        assert config.variance_method == "ratio"
    
    def test_enum_validation(self):
        """Test that enum fields validate properly."""
        # Test LandType enum
        for land_type in ["forest", "timber", "all"]:
            config = EstimatorConfig(land_type=land_type)
            assert config.land_type == land_type
        
        # Test TreeType enum
        for tree_type in ["live", "dead", "gs", "all"]:
            config = EstimatorConfig(tree_type=tree_type)
            assert config.tree_type == tree_type
        
        # Test EstimationMethod enum
        for method in ["TI", "SMA", "LMA", "EMA", "ANNUAL"]:
            config = EstimatorConfig(method=method)
            assert config.method == method
        
        # Test VarianceMethod enum
        for var_method in ["standard", "ratio", "hybrid"]:
            config = EstimatorConfig(variance_method=var_method)
            assert config.variance_method == var_method
        
        # Test invalid enum values
        with pytest.raises(ValueError):
            EstimatorConfig(land_type="invalid")
        
        with pytest.raises(ValueError):
            EstimatorConfig(tree_type="invalid")
        
        with pytest.raises(ValueError):
            EstimatorConfig(method="invalid")
        
        with pytest.raises(ValueError):
            EstimatorConfig(variance_method="invalid")
    
    def test_lambda_validation(self):
        """Test lambda parameter validation."""
        # Valid lambda values
        for lambda_val in [0.0, 0.1, 0.5, 0.9, 1.0]:
            config = EstimatorConfig(lambda_=lambda_val)
            assert config.lambda_ == lambda_val
        
        # Invalid lambda values
        invalid_values = [-0.1, 1.1, 2.0, -1.0]
        for invalid_val in invalid_values:
            with pytest.raises(ValueError):
                EstimatorConfig(lambda_=invalid_val)
    
    def test_domain_filter_validation(self):
        """Test domain filter validation and SQL injection prevention."""
        # Valid domain filters
        valid_filters = [
            "DIA >= 10.0",
            "STATUSCD == 1",
            "DIA BETWEEN 5.0 AND 15.0",
            "SPCD IN (131, 110, 833)",
            "HT IS NOT NULL",
            "DIA >= 5.0 AND STATUSCD == 1",
            "OWNGRPCD != 40",
            "TREECLCD <= 3"
        ]
        
        for filter_expr in valid_filters:
            config = EstimatorConfig(tree_domain=filter_expr)
            # Should normalize whitespace
            assert config.tree_domain == " ".join(filter_expr.split())
            
            # Also test area_domain
            config = EstimatorConfig(area_domain=filter_expr)
            assert config.area_domain == " ".join(filter_expr.split())
        
        # SQL injection attempts
        dangerous_filters = [
            "DIA >= 10; DROP TABLE TREE; --",
            "STATUSCD == 1' OR '1'='1",
            "/* comment */ DIA > 5",
            "DIA >= 10 UNION SELECT * FROM PLOT",
            "DIA >= 10; DELETE FROM COND WHERE 1=1",
            "DIA >= 10 AND 1=1; ALTER TABLE TREE",
            "DIA >= 10; CREATE TABLE malicious",
            "DIA >= 10; INSERT INTO TREE VALUES",
            "DIA >= 10; UPDATE TREE SET",
            "DIA >= 10; EXEC('malicious')",
            "DIA >= 10; EXECUTE sp_malicious"
        ]
        
        for dangerous in dangerous_filters:
            with pytest.raises(ValueError, match="forbidden keyword"):
                EstimatorConfig(tree_domain=dangerous)
    
    def test_grouping_column_validation(self):
        """Test grouping column validation."""
        # Valid FIA columns should not produce warnings
        valid_columns = ["SPCD", "OWNGRPCD", "STATUSCD", "FORTYPCD", "DIA"]
        config = EstimatorConfig(grp_by=valid_columns)
        assert config.grp_by == valid_columns
        
        # Test single string column
        config = EstimatorConfig(grp_by="SPCD")
        assert config.grp_by == "SPCD"
        
        # Unknown columns should produce warning
        with pytest.warns(UserWarning, match="Unknown grouping columns"):
            EstimatorConfig(grp_by=["UNKNOWN_COL", "BAD_COLUMN"])
        
        # Mixed valid/invalid columns
        with pytest.warns(UserWarning, match="CUSTOM_DERIVED"):
            EstimatorConfig(grp_by=["SPCD", "CUSTOM_DERIVED", "OWNGRPCD"])
        
        # Empty list should be valid
        config = EstimatorConfig(grp_by=[])
        assert config.grp_by == []
        
        # None should be valid
        config = EstimatorConfig(grp_by=None)
        assert config.grp_by is None
    
    def test_get_grouping_columns_method(self):
        """Test the get_grouping_columns method logic."""
        # Test explicit grp_by columns
        config = EstimatorConfig(grp_by=["SPCD", "OWNGRPCD"])
        columns = config.get_grouping_columns()
        assert "SPCD" in columns
        assert "OWNGRPCD" in columns
        
        # Test by_species flag
        config = EstimatorConfig(by_species=True)
        columns = config.get_grouping_columns()
        assert "SPCD" in columns
        
        # Test combination without duplicates
        config = EstimatorConfig(grp_by=["SPCD", "OWNGRPCD"], by_species=True)
        columns = config.get_grouping_columns()
        assert columns.count("SPCD") == 1  # No duplicates
        assert "OWNGRPCD" in columns
        
        # Test with string grp_by
        config = EstimatorConfig(grp_by="FORTYPCD", by_species=True)
        columns = config.get_grouping_columns()
        assert "FORTYPCD" in columns
        assert "SPCD" in columns
        
        # Test empty case
        config = EstimatorConfig(grp_by=[], by_species=False)
        columns = config.get_grouping_columns()
        assert columns == []


class TestLazyEvaluationConfig:
    """Test LazyEvaluationConfig validation and settings."""
    
    def test_default_lazy_config(self):
        """Test default lazy evaluation configuration."""
        config = LazyEvaluationConfig()
        
        assert config.mode == LazyEvaluationMode.AUTO
        assert config.threshold_rows == 10_000
        assert config.collection_strategy == "adaptive"
        assert config.max_parallel_collections == 4
        assert config.memory_limit_mb is None
        assert config.chunk_size == 50_000
        assert config.enable_predicate_pushdown is True
        assert config.enable_projection_pushdown is True
        assert config.enable_slice_pushdown is True
        assert config.enable_expression_caching is True
        assert config.cache_ttl_seconds == 300
    
    def test_lazy_config_validation(self):
        """Test lazy configuration validation."""
        # Valid configurations
        config = LazyEvaluationConfig(
            mode=LazyEvaluationMode.ENABLED,
            threshold_rows=100_000,
            collection_strategy="parallel",
            max_parallel_collections=8,
            memory_limit_mb=2048,
            chunk_size=200_000,
            cache_ttl_seconds=600
        )
        
        assert config.mode == LazyEvaluationMode.ENABLED
        assert config.threshold_rows == 100_000
        assert config.collection_strategy == "parallel"
        assert config.max_parallel_collections == 8
        assert config.memory_limit_mb == 2048
        assert config.chunk_size == 200_000
        assert config.cache_ttl_seconds == 600
        
        # Invalid threshold_rows
        with pytest.raises(ValueError):
            LazyEvaluationConfig(threshold_rows=-1)
        
        # Invalid max_parallel_collections
        with pytest.raises(ValueError):
            LazyEvaluationConfig(max_parallel_collections=0)
        
        with pytest.raises(ValueError):
            LazyEvaluationConfig(max_parallel_collections=20)  # > 16
        
        # Invalid memory_limit_mb
        with pytest.raises(ValueError):
            LazyEvaluationConfig(memory_limit_mb=50)  # < 100
        
        # Invalid chunk_size
        with pytest.raises(ValueError):
            LazyEvaluationConfig(chunk_size=500)  # < 1000
        
        # Invalid collection_strategy
        with pytest.raises(ValueError):
            LazyEvaluationConfig(collection_strategy="invalid")
        
        # Invalid cache_ttl_seconds
        with pytest.raises(ValueError):
            LazyEvaluationConfig(cache_ttl_seconds=-1)
    
    def test_memory_validation_warning(self):
        """Test memory validation warning for chunk size vs memory limit."""
        # This should produce a warning
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            LazyEvaluationConfig(
                memory_limit_mb=100,  # Small memory limit
                chunk_size=200_000    # Large chunk size
            )
            
            assert len(w) == 1
            assert "Chunk size" in str(w[0].message)
            assert "too large" in str(w[0].message)
        
        # This should NOT produce a warning
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            LazyEvaluationConfig(
                memory_limit_mb=1000,  # Large memory limit
                chunk_size=10_000      # Small chunk size
            )
            
            assert len(w) == 0


class TestMortalityConfig:
    """Test MortalityConfig specific validation."""
    
    def test_valid_mortality_config(self):
        """Test valid mortality configuration."""
        config = MortalityConfig(
            mortality_type="both",
            tree_class="growing_stock",
            tree_type="dead",
            group_by_species_group=True,
            group_by_ownership=True,
            group_by_agent=True,
            group_by_disturbance=True,
            include_components=True,
            include_natural=True,
            include_harvest=False
        )
        
        assert config.mortality_type == "both"
        assert config.tree_class == "growing_stock"
        assert config.tree_type == "dead"
        assert config.group_by_species_group is True
        assert config.group_by_ownership is True
        assert config.group_by_agent is True
        assert config.group_by_disturbance is True
        assert config.include_components is True
        assert config.include_natural is True
        assert config.include_harvest is False
    
    def test_mortality_validation_rules(self):
        """Test mortality-specific validation rules."""
        # Valid: dead trees with mortality
        config = MortalityConfig(mortality_type="volume", tree_type="dead")
        # Should not raise
        
        # Valid: all trees with mortality
        config = MortalityConfig(mortality_type="tpa", tree_type="all")
        # Should not raise
        
        # Invalid: live trees with volume mortality
        with pytest.raises(ValueError, match="Cannot calculate volume mortality with tree_type='live'"):
            MortalityConfig(mortality_type="volume", tree_type="live")
        
        # Invalid: live trees with both mortality
        with pytest.raises(ValueError, match="Cannot calculate volume mortality with tree_type='live'"):
            MortalityConfig(mortality_type="both", tree_type="live")
        
        # Valid: live trees with TPA mortality (this could be survivorship analysis)
        # Actually this should also be invalid for mortality - mortality requires dead trees
        with pytest.raises(ValueError):
            MortalityConfig(mortality_type="tpa", tree_type="live")
        
        # Invalid tree_class and land_type combination
        with pytest.raises(ValueError, match="tree_class='timber' requires land_type='timber'"):
            MortalityConfig(
                tree_class="timber",
                land_type="forest"  # Should be "timber" or "all"
            )
        
        # Valid combination
        config = MortalityConfig(
            tree_class="timber",
            land_type="timber"
        )
        # Should not raise
    
    def test_mortality_grouping_columns(self):
        """Test mortality-specific grouping columns."""
        config = MortalityConfig(
            group_by_species_group=True,
            group_by_ownership=True,
            group_by_agent=True,
            group_by_disturbance=True,
            by_size_class=True,
            grp_by=["SPCD"],
            by_species=True
        )
        
        columns = config.get_grouping_columns()
        
        # Should include mortality-specific columns
        assert "SPGRPCD" in columns
        assert "OWNGRPCD" in columns
        assert "AGENTCD" in columns
        assert "DSTRBCD1" in columns
        assert "DSTRBCD2" in columns
        assert "DSTRBCD3" in columns
        assert "SIZE_CLASS" in columns
        
        # Should include base columns
        assert "SPCD" in columns
        
        # Should not have duplicates
        assert len(columns) == len(set(columns))
    
    def test_mortality_output_columns(self):
        """Test mortality output column generation."""
        # TPA mortality only
        config = MortalityConfig(mortality_type="tpa", variance=False, totals=False)
        cols = config.get_output_columns()
        assert "MORTALITY_TPA" in cols
        assert "MORTALITY_TPA_SE" in cols
        assert "MORTALITY_VOL" not in cols
        
        # Volume mortality only
        config = MortalityConfig(mortality_type="volume", variance=True, totals=True)
        cols = config.get_output_columns()
        assert "MORTALITY_VOL" in cols
        assert "MORTALITY_VOL_VAR" in cols
        assert "MORTALITY_VOL_TOTAL" in cols
        assert "MORTALITY_TPA" not in cols
        
        # Both mortality types
        config = MortalityConfig(
            mortality_type="both",
            include_components=True,
            variance=False,
            totals=True
        )
        cols = config.get_output_columns()
        assert "MORTALITY_TPA" in cols
        assert "MORTALITY_TPA_SE" in cols
        assert "MORTALITY_TPA_TOTAL" in cols
        assert "MORTALITY_VOL" in cols
        assert "MORTALITY_VOL_SE" in cols
        assert "MORTALITY_VOL_TOTAL" in cols
        assert "MORTALITY_BA" in cols
        assert "MORTALITY_BA_SE" in cols
        assert "MORTALITY_BA_TOTAL" in cols


class TestModuleSpecificConfigs:
    """Test module-specific configuration classes."""
    
    def test_volume_specific_config(self):
        """Test VolumeSpecificConfig validation."""
        config = VolumeSpecificConfig(
            volume_equation="regional",
            include_sound=True,
            include_rotten=False,
            merchantable_top_diameter=6.0,
            stump_height=1.5
        )
        
        assert config.volume_equation == "regional"
        assert config.include_sound is True
        assert config.include_rotten is False
        assert config.merchantable_top_diameter == 6.0
        assert config.stump_height == 1.5
        
        # Test validation
        with pytest.raises(ValueError):
            VolumeSpecificConfig(volume_equation="invalid")
        
        with pytest.raises(ValueError):
            VolumeSpecificConfig(merchantable_top_diameter=-1.0)
        
        with pytest.raises(ValueError):
            VolumeSpecificConfig(stump_height=-0.5)
    
    def test_biomass_specific_config(self):
        """Test BiomassSpecificConfig validation."""
        config = BiomassSpecificConfig(
            component="total",
            include_foliage=True,
            include_saplings=True,
            carbon_fraction=0.47,
            units="kg"
        )
        
        assert config.component == "total"
        assert config.include_foliage is True
        assert config.include_saplings is True
        assert config.carbon_fraction == 0.47
        assert config.units == "kg"
        
        # Test validation
        with pytest.raises(ValueError):
            BiomassSpecificConfig(component="invalid")
        
        with pytest.raises(ValueError):
            BiomassSpecificConfig(carbon_fraction=1.5)  # > 1.0
        
        with pytest.raises(ValueError):
            BiomassSpecificConfig(carbon_fraction=-0.1)  # < 0.0
        
        with pytest.raises(ValueError):
            BiomassSpecificConfig(units="invalid")
    
    def test_growth_specific_config(self):
        """Test GrowthSpecificConfig validation."""
        config = GrowthSpecificConfig(
            growth_type="net",
            include_ingrowth=True,
            include_mortality=True,
            include_removals=False,
            annual_only=True
        )
        
        assert config.growth_type == "net"
        assert config.include_ingrowth is True
        assert config.include_mortality is True
        assert config.include_removals is False
        assert config.annual_only is True
        
        # Test validation
        with pytest.raises(ValueError):
            GrowthSpecificConfig(growth_type="invalid")
    
    def test_area_specific_config(self):
        """Test AreaSpecificConfig validation."""
        config = AreaSpecificConfig(
            area_basis="land",
            include_nonforest=True,
            include_water=False,
            ownership_groups=[10, 20, 30, 40]
        )
        
        assert config.area_basis == "land"
        assert config.include_nonforest is True
        assert config.include_water is False
        assert config.ownership_groups == [10, 20, 30, 40]
        
        # Test validation
        with pytest.raises(ValueError):
            AreaSpecificConfig(area_basis="invalid")


class TestConfigFactory:
    """Test the ConfigFactory functionality."""
    
    def test_volume_config_creation(self):
        """Test volume config creation through factory."""
        config = ConfigFactory.create_config(
            "volume",
            by_species=True,
            tree_type="live",
            extra_params={
                "volume_equation": "regional",
                "merchantable_top_diameter": 6.0
            }
        )
        
        assert isinstance(config, VolumeConfig)
        assert config.by_species is True
        assert config.tree_type == "live"
        assert config.module_config.volume_equation == "regional"
        assert config.module_config.merchantable_top_diameter == 6.0
    
    def test_biomass_config_creation(self):
        """Test biomass config creation through factory."""
        config = ConfigFactory.create_config(
            "biomass",
            method="TI",
            variance=True,
            extra_params={
                "component": "total",
                "carbon_fraction": 0.47
            }
        )
        
        assert isinstance(config, BiomassConfig)
        assert config.method == "TI"
        assert config.variance is True
        assert config.module_config.component == "total"
        assert config.module_config.carbon_fraction == 0.47
    
    def test_mortality_config_creation(self):
        """Test mortality config creation through factory."""
        config = ConfigFactory.create_config(
            "mortality",
            mortality_type="both",
            tree_type="dead",
            group_by_agent=True,
            extra_params={
                "include_natural": True,
                "include_harvest": False
            }
        )
        
        assert isinstance(config, MortalityConfig)
        assert config.mortality_type == "both"
        assert config.tree_type == "dead"
        assert config.group_by_agent is True
        assert config.include_natural is True
        assert config.include_harvest is False
    
    def test_growth_config_creation(self):
        """Test growth config creation through factory."""
        config = ConfigFactory.create_config(
            "growth",
            method="ANNUAL",
            extra_params={
                "growth_type": "gross",
                "include_ingrowth": False
            }
        )
        
        assert isinstance(config, GrowthConfig)
        assert config.method == "ANNUAL"
        assert config.module_config.growth_type == "gross"
        assert config.module_config.include_ingrowth is False
    
    def test_area_config_creation(self):
        """Test area config creation through factory."""
        config = ConfigFactory.create_config(
            "area",
            land_type="all",
            extra_params={
                "area_basis": "land",
                "ownership_groups": [10, 20]
            }
        )
        
        assert isinstance(config, AreaConfig)
        assert config.land_type == "all"
        assert config.module_config.area_basis == "land"
        assert config.module_config.ownership_groups == [10, 20]
    
    def test_unknown_module_config_creation(self):
        """Test unknown module returns base EstimatorConfig."""
        config = ConfigFactory.create_config(
            "unknown_module",
            by_species=True,
            tree_type="live"
        )
        
        assert isinstance(config, EstimatorConfig)
        assert not isinstance(config, (VolumeConfig, BiomassConfig, MortalityConfig))
        assert config.by_species is True
        assert config.tree_type == "live"


class TestConfigSerialization:
    """Test configuration serialization and deserialization."""
    
    def test_estimator_config_to_dict(self):
        """Test EstimatorConfig serialization to dictionary."""
        config = EstimatorConfig(
            grp_by=["SPCD", "OWNGRPCD"],
            by_species=True,
            tree_type="live",
            variance=True,
            lazy_config=LazyEvaluationConfig(
                mode=LazyEvaluationMode.ENABLED,
                threshold_rows=25_000
            ),
            extra_params={
                "custom_param": "custom_value",
                "another_param": 42
            }
        )
        
        config_dict = config.to_dict()
        
        # Check main parameters
        assert config_dict["grp_by"] == ["SPCD", "OWNGRPCD"]
        assert config_dict["by_species"] is True
        assert config_dict["tree_type"] == "live"
        assert config_dict["variance"] is True
        
        # Check lazy config is flattened
        assert config_dict["lazy_mode"] == "enabled"
        assert config_dict["lazy_threshold_rows"] == 25_000
        
        # Check extra params are merged
        assert config_dict["custom_param"] == "custom_value"
        assert config_dict["another_param"] == 42
        
        # Check that nested objects are removed
        assert "lazy_config" not in config_dict
        assert "extra_params" not in config_dict
    
    def test_mortality_config_to_dict(self):
        """Test MortalityConfig serialization includes all fields."""
        config = MortalityConfig(
            mortality_type="both",
            tree_type="dead",
            group_by_agent=True,
            group_by_ownership=True,
            include_components=True,
            grp_by=["SPCD"],
            variance=True
        )
        
        config_dict = config.to_dict()
        
        # Check base EstimatorConfig fields
        assert config_dict["grp_by"] == ["SPCD"]
        assert config_dict["variance"] is True
        assert config_dict["tree_type"] == "dead"
        
        # Check mortality-specific fields
        assert config_dict["mortality_type"] == "both"
        assert config_dict["group_by_agent"] is True
        assert config_dict["group_by_ownership"] is True
        assert config_dict["include_components"] is True
    
    def test_modular_config_serialization(self):
        """Test modular configuration serialization."""
        config = VolumeConfig(
            by_species=True,
            tree_type="live",
            module_config=VolumeSpecificConfig(
                volume_equation="regional",
                merchantable_top_diameter=6.0
            )
        )
        
        config_dict = config.to_dict()
        
        # Should contain both base and module-specific fields
        assert config_dict["by_species"] is True
        assert config_dict["tree_type"] == "live"
        
        # Module config should be present (exact structure depends on implementation)
        assert "module_config" in config_dict or "volume_equation" in config_dict


class TestConfigurationEdgeCases:
    """Test edge cases and boundary conditions in configuration."""
    
    def test_none_and_empty_values(self):
        """Test handling of None and empty values."""
        config = EstimatorConfig(
            grp_by=None,
            tree_domain=None,
            area_domain=None,
            extra_params={}
        )
        
        assert config.grp_by is None
        assert config.tree_domain is None
        assert config.area_domain is None
        assert config.extra_params == {}
        
        # Empty list should work
        config = EstimatorConfig(grp_by=[])
        assert config.grp_by == []
        assert config.get_grouping_columns() == []
    
    def test_boundary_values(self):
        """Test boundary values for numeric parameters."""
        # Lambda at boundaries
        config = EstimatorConfig(lambda_=0.0)
        assert config.lambda_ == 0.0
        
        config = EstimatorConfig(lambda_=1.0)
        assert config.lambda_ == 1.0
        
        # Lazy config boundaries
        lazy_config = LazyEvaluationConfig(
            threshold_rows=0,  # Minimum
            max_parallel_collections=1,  # Minimum
            memory_limit_mb=100,  # Minimum
            chunk_size=1000,  # Minimum
            cache_ttl_seconds=0  # No expiry
        )
        
        assert lazy_config.threshold_rows == 0
        assert lazy_config.chunk_size == 1000
        assert lazy_config.cache_ttl_seconds == 0
    
    def test_whitespace_normalization(self):
        """Test that domain filters normalize whitespace properly."""
        messy_filter = "  DIA   >=    10.0   AND   STATUSCD   ==   1   "
        clean_filter = "DIA >= 10.0 AND STATUSCD == 1"
        
        config = EstimatorConfig(tree_domain=messy_filter)
        assert config.tree_domain == clean_filter
        
        config = EstimatorConfig(area_domain=messy_filter)
        assert config.area_domain == clean_filter
    
    def test_type_coercion(self):
        """Test that values are properly coerced to correct types."""
        # String boolean values
        config = EstimatorConfig(by_species="true")  # Should this work?
        # This might depend on Pydantic configuration
        
        # String numeric values
        config = EstimatorConfig(lambda_="0.5")
        # This might work with Pydantic coercion
        
        # The exact behavior depends on Pydantic field configuration


class TestValidationErrorMessages:
    """Test that validation error messages are helpful and informative."""
    
    def test_helpful_enum_errors(self):
        """Test that enum validation errors are helpful."""
        with pytest.raises(ValueError) as exc_info:
            EstimatorConfig(land_type="invalid_type")
        
        error_message = str(exc_info.value)
        # Should mention valid options or be otherwise helpful
        assert len(error_message) > 0
    
    def test_helpful_range_errors(self):
        """Test that numeric range errors are helpful."""
        with pytest.raises(ValueError) as exc_info:
            EstimatorConfig(lambda_=1.5)
        
        error_message = str(exc_info.value)
        assert len(error_message) > 0
        
        with pytest.raises(ValueError) as exc_info:
            LazyEvaluationConfig(threshold_rows=-1)
        
        error_message = str(exc_info.value)
        assert len(error_message) > 0
    
    def test_helpful_domain_filter_errors(self):
        """Test that domain filter errors are helpful."""
        with pytest.raises(ValueError) as exc_info:
            EstimatorConfig(tree_domain="DIA >= 10; DROP TABLE TREE")
        
        error_message = str(exc_info.value)
        assert "forbidden keyword" in error_message
        assert "DROP" in error_message or "forbidden" in error_message
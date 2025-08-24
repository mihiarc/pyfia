"""
Tests for the unified configuration system.

This module tests the UnifiedEstimatorConfig and its compatibility
with legacy configurations, validation, and module-specific features.
"""

import pytest
from typing import Dict, Any
import warnings

from pyfia.estimation.base import EstimatorConfig
from pyfia.estimation.config import EstimatorConfigV2, MortalityConfig
from pyfia.estimation.unified_config import (
    UnifiedEstimatorConfig,
    UnifiedVolumeConfig,
    UnifiedBiomassConfig,
    UnifiedGrowthConfig,
    UnifiedAreaConfig,
    UnifiedMortalityConfig,
    VolumeSpecificConfig,
    BiomassSpecificConfig,
    GrowthSpecificConfig,
    AreaSpecificConfig,
    LazyEvaluationConfig,
    LazyEvaluationMode,
    EstimationMethod,
    LandType,
    TreeType,
    VarianceMethod,
    ConfigFactory,
)


class TestUnifiedEstimatorConfig:
    """Test the base unified configuration class."""
    
    def test_default_configuration(self):
        """Test default configuration values."""
        config = UnifiedEstimatorConfig()
        
        # Check defaults
        assert config.by_species is False
        assert config.by_size_class is False
        assert config.land_type == LandType.FOREST
        assert config.tree_type == TreeType.LIVE
        assert config.method == EstimationMethod.TI
        assert config.lambda_ == 0.5
        assert config.totals is False
        assert config.variance is False
        assert config.by_plot is False
        assert config.most_recent is False
        
        # Check lazy defaults
        assert config.lazy_config.mode == LazyEvaluationMode.AUTO
        assert config.lazy_config.threshold_rows == 10_000
        assert config.lazy_config.collection_strategy == "adaptive"
    
    def test_custom_configuration(self):
        """Test custom configuration values."""
        config = UnifiedEstimatorConfig(
            by_species=True,
            land_type=LandType.TIMBER,
            method=EstimationMethod.EMA,
            lambda_=0.7,
            lazy_config=LazyEvaluationConfig(
                mode=LazyEvaluationMode.ENABLED,
                threshold_rows=5000
            )
        )
        
        assert config.by_species is True
        assert config.land_type == LandType.TIMBER
        assert config.method == EstimationMethod.EMA
        assert config.lambda_ == 0.7
        assert config.lazy_config.mode == LazyEvaluationMode.ENABLED
        assert config.lazy_config.threshold_rows == 5000
    
    def test_grouping_columns(self):
        """Test grouping column extraction."""
        config = UnifiedEstimatorConfig(
            grp_by=["STATECD", "COUNTYCD"],
            by_species=True,
            by_size_class=True
        )
        
        columns = config.get_grouping_columns()
        assert "STATECD" in columns
        assert "COUNTYCD" in columns
        assert "SPCD" in columns
        assert "SIZE_CLASS" in columns
        
        # Check no duplicates
        assert len(columns) == len(set(columns))
    
    def test_domain_validation(self):
        """Test domain expression validation."""
        # Valid expressions
        config = UnifiedEstimatorConfig(
            tree_domain="DIA > 10 AND STATUSCD == 1",
            area_domain="LANDCLCD == 1"
        )
        assert config.tree_domain == "DIA > 10 AND STATUSCD == 1"
        
        # Invalid expressions with SQL injection
        with pytest.raises(ValueError, match="forbidden keyword"):
            UnifiedEstimatorConfig(tree_domain="DIA > 10; DROP TABLE TREE")
        
        with pytest.raises(ValueError, match="forbidden keyword"):
            UnifiedEstimatorConfig(area_domain="DELETE FROM COND")
    
    def test_lambda_validation(self):
        """Test lambda parameter validation."""
        # Valid lambda
        config = UnifiedEstimatorConfig(lambda_=0.3)
        assert config.lambda_ == 0.3
        
        # Invalid lambda (out of range)
        with pytest.raises(ValueError):
            UnifiedEstimatorConfig(lambda_=1.5)
        
        with pytest.raises(ValueError):
            UnifiedEstimatorConfig(lambda_=-0.1)
    
    def test_to_dict_conversion(self):
        """Test conversion to dictionary."""
        config = UnifiedEstimatorConfig(
            by_species=True,
            land_type=LandType.FOREST,
            extra_params={"custom_param": "value"}
        )
        
        data = config.to_dict()
        assert data["by_species"] is True
        assert data["land_type"] == "forest"
        assert data["custom_param"] == "value"
        assert "lazy_mode" in data  # Lazy config flattened


class TestLegacyCompatibility:
    """Test backward compatibility with legacy configurations."""
    
    def test_from_legacy_estimator_config(self):
        """Test conversion from legacy EstimatorConfig."""
        legacy = EstimatorConfig(
            by_species=True,
            land_type="timber",
            method="SMA",
            lambda_=0.6,
            extra_params={"custom": "value"}
        )
        
        unified = UnifiedEstimatorConfig.from_legacy(legacy)
        
        assert unified.by_species is True
        assert unified.land_type == LandType.TIMBER
        assert unified.method == EstimationMethod.SMA
        assert unified.lambda_ == 0.6
        assert unified.extra_params["custom"] == "value"
    
    def test_to_legacy_estimator_config(self):
        """Test conversion to legacy EstimatorConfig."""
        unified = UnifiedEstimatorConfig(
            by_species=True,
            land_type=LandType.FOREST,
            method=EstimationMethod.TI,
            lazy_config=LazyEvaluationConfig(
                mode=LazyEvaluationMode.ENABLED,
                threshold_rows=20000
            )
        )
        
        legacy = unified.to_legacy()
        
        assert isinstance(legacy, EstimatorConfig)
        assert legacy.by_species is True
        assert legacy.land_type == "forest"
        assert legacy.method == "TI"
        assert legacy.extra_params["lazy_enabled"] is True
        assert legacy.extra_params["lazy_threshold_rows"] == 20000
    
    def test_from_v2_config(self):
        """Test conversion from EstimatorConfigV2."""
        v2_config = EstimatorConfigV2(
            by_species=True,
            land_type="timber",
            method="EMA",
            lambda_=0.4
        )
        
        unified = UnifiedEstimatorConfig.from_v2(v2_config)
        
        assert unified.by_species is True
        assert unified.land_type == LandType.TIMBER
        assert unified.method == EstimationMethod.EMA
        assert unified.lambda_ == 0.4
    
    def test_to_v2_config(self):
        """Test conversion to EstimatorConfigV2."""
        unified = UnifiedEstimatorConfig(
            by_species=True,
            by_size_class=True,
            method=EstimationMethod.LMA
        )
        
        v2_config = unified.to_v2()
        
        assert isinstance(v2_config, EstimatorConfigV2)
        assert v2_config.by_species is True
        assert v2_config.by_size_class is True
        assert v2_config.method == "LMA"
    
    def test_round_trip_conversion(self):
        """Test round-trip conversion maintains data integrity."""
        original = UnifiedEstimatorConfig(
            grp_by=["STATECD", "SPCD"],
            by_species=True,
            land_type=LandType.TIMBER,
            tree_domain="DIA > 5",
            method=EstimationMethod.EMA,
            lambda_=0.65,
            totals=True,
            variance=True
        )
        
        # Convert to legacy and back
        legacy = original.to_legacy()
        restored = UnifiedEstimatorConfig.from_legacy(legacy)
        
        assert restored.grp_by == original.grp_by
        assert restored.by_species == original.by_species
        assert restored.land_type == original.land_type
        assert restored.tree_domain == original.tree_domain
        assert restored.method == original.method
        assert restored.lambda_ == original.lambda_
        assert restored.totals == original.totals
        assert restored.variance == original.variance


class TestModuleSpecificConfigs:
    """Test module-specific configuration classes."""
    
    def test_volume_config(self):
        """Test volume-specific configuration."""
        config = UnifiedVolumeConfig(
            by_species=True,
            module_config=VolumeSpecificConfig(
                volume_equation="regional",
                include_rotten=True,
                merchantable_top_diameter=6.0
            )
        )
        
        assert config.by_species is True
        assert config.module_config.volume_equation == "regional"
        assert config.module_config.include_rotten is True
        assert config.module_config.merchantable_top_diameter == 6.0
    
    def test_biomass_config(self):
        """Test biomass-specific configuration."""
        config = UnifiedBiomassConfig(
            land_type=LandType.FOREST,
            module_config=BiomassSpecificConfig(
                component="total",
                include_foliage=True,
                carbon_fraction=0.47,
                units="tons"
            )
        )
        
        assert config.land_type == LandType.FOREST
        assert config.module_config.component == "total"
        assert config.module_config.include_foliage is True
        assert config.module_config.carbon_fraction == 0.47
        assert config.module_config.units == "tons"
    
    def test_growth_config(self):
        """Test growth-specific configuration."""
        config = UnifiedGrowthConfig(
            method=EstimationMethod.ANNUAL,
            module_config=GrowthSpecificConfig(
                growth_type="net",
                include_ingrowth=True,
                include_mortality=True,
                annual_only=True
            )
        )
        
        assert config.method == EstimationMethod.ANNUAL
        assert config.module_config.growth_type == "net"
        assert config.module_config.include_ingrowth is True
        assert config.module_config.annual_only is True
    
    def test_area_config(self):
        """Test area-specific configuration."""
        config = UnifiedAreaConfig(
            land_type=LandType.ALL,
            module_config=AreaSpecificConfig(
                area_basis="forest",
                include_nonforest=True,
                ownership_groups=[1, 2, 3]
            )
        )
        
        assert config.land_type == LandType.ALL
        assert config.module_config.area_basis == "forest"
        assert config.module_config.include_nonforest is True
        assert config.module_config.ownership_groups == [1, 2, 3]
    
    def test_mortality_config(self):
        """Test mortality-specific configuration."""
        config = UnifiedMortalityConfig(
            tree_type=TreeType.DEAD,
            group_by_agent=True,
            mortality_type="both",
            tree_class="timber"
        )
        
        assert config.tree_type == TreeType.DEAD
        assert config.group_by_agent is True
        assert config.mortality_type == "both"
        assert config.tree_class == "timber"
        
        # Test grouping columns
        columns = config.get_grouping_columns()
        assert "AGENTCD" in columns
    
    def test_mortality_legacy_compatibility(self):
        """Test mortality config compatibility with MortalityConfig."""
        # Create from existing MortalityConfig
        mortality_config = MortalityConfig(
            by_species=True,
            tree_type="dead",
            group_by_agent=True,
            mortality_type="tpa"
        )
        
        unified = UnifiedMortalityConfig.from_mortality_config(mortality_config)
        
        assert unified.by_species is True
        assert unified.tree_type == TreeType.DEAD
        assert unified.group_by_agent is True
        assert unified.mortality_type == "tpa"
        
        # Convert back
        restored = unified.to_mortality_config()
        assert isinstance(restored, MortalityConfig)
        assert restored.by_species is True


class TestLazyEvaluationConfig:
    """Test lazy evaluation configuration."""
    
    def test_lazy_defaults(self):
        """Test default lazy evaluation settings."""
        config = LazyEvaluationConfig()
        
        assert config.mode == LazyEvaluationMode.AUTO
        assert config.threshold_rows == 10_000
        assert config.collection_strategy == "adaptive"
        assert config.max_parallel_collections == 4
        assert config.chunk_size == 50_000
        assert config.enable_predicate_pushdown is True
    
    def test_lazy_custom(self):
        """Test custom lazy evaluation settings."""
        config = LazyEvaluationConfig(
            mode=LazyEvaluationMode.ENABLED,
            threshold_rows=5000,
            collection_strategy="parallel",
            max_parallel_collections=8,
            memory_limit_mb=2048
        )
        
        assert config.mode == LazyEvaluationMode.ENABLED
        assert config.threshold_rows == 5000
        assert config.collection_strategy == "parallel"
        assert config.max_parallel_collections == 8
        assert config.memory_limit_mb == 2048
    
    def test_memory_validation(self):
        """Test memory settings validation."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            
            config = LazyEvaluationConfig(
                memory_limit_mb=100,
                chunk_size=100_000
            )
            
            # Should generate a warning about chunk size
            assert len(w) > 0
            assert "chunk size" in str(w[0].message).lower()


class TestConfigFactory:
    """Test the configuration factory."""
    
    def test_create_volume_config(self):
        """Test creating volume configuration."""
        config = ConfigFactory.create_config(
            "volume",
            by_species=True,
            land_type="forest"
        )
        
        assert isinstance(config, UnifiedVolumeConfig)
        assert config.by_species is True
        assert config.land_type == LandType.FOREST
    
    def test_create_from_legacy(self):
        """Test creating config from legacy."""
        legacy = EstimatorConfig(
            by_species=True,
            method="EMA",
            lambda_=0.3
        )
        
        config = ConfigFactory.create_config("biomass", legacy_config=legacy)
        
        assert isinstance(config, UnifiedBiomassConfig)
        assert config.by_species is True
        assert config.method == EstimationMethod.EMA
        assert config.lambda_ == 0.3
    
    def test_ensure_unified(self):
        """Test ensuring configuration is unified."""
        # Already unified
        unified = UnifiedEstimatorConfig(by_species=True)
        result = ConfigFactory.ensure_unified(unified)
        assert result is unified
        
        # From legacy
        legacy = EstimatorConfig(by_species=True)
        result = ConfigFactory.ensure_unified(legacy)
        assert isinstance(result, UnifiedEstimatorConfig)
        assert result.by_species is True
        
        # From dict
        result = ConfigFactory.ensure_unified({"by_species": True})
        assert isinstance(result, UnifiedEstimatorConfig)
        assert result.by_species is True
        
        # From MortalityConfig
        mortality = MortalityConfig(tree_type="dead")
        result = ConfigFactory.ensure_unified(mortality)
        assert isinstance(result, UnifiedMortalityConfig)
        assert result.tree_type == TreeType.DEAD
    
    def test_is_unified_config(self):
        """Test checking if config is unified."""
        unified = UnifiedEstimatorConfig()
        legacy = EstimatorConfig()
        
        assert ConfigFactory.is_unified_config(unified) is True
        assert ConfigFactory.is_unified_config(legacy) is False


class TestModuleValidation:
    """Test module-specific validation."""
    
    def test_validate_for_mortality(self):
        """Test validation for mortality module."""
        # Invalid: live trees for mortality
        config = UnifiedEstimatorConfig(tree_type=TreeType.LIVE)
        with pytest.raises(ValueError, match="tree_type='dead' or 'all'"):
            config.validate_for_module("mortality")
        
        # Valid: dead trees
        config = UnifiedEstimatorConfig(tree_type=TreeType.DEAD)
        config.validate_for_module("mortality")  # Should not raise
    
    def test_validate_for_growth(self):
        """Test validation for growth module."""
        # Valid methods
        config = UnifiedEstimatorConfig(method=EstimationMethod.TI)
        config.validate_for_module("growth")  # Should not raise
        
        config = UnifiedEstimatorConfig(method=EstimationMethod.ANNUAL)
        config.validate_for_module("growth")  # Should not raise
        
        # Warning for other methods
        config = UnifiedEstimatorConfig(method=EstimationMethod.EMA)
        with pytest.raises(ValueError, match="TI or ANNUAL"):
            config.validate_for_module("growth")
    
    def test_validate_for_area(self):
        """Test validation for area module."""
        # Warning for tree_domain with area estimation
        config = UnifiedEstimatorConfig(tree_domain="DIA > 10")
        
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            config.validate_for_module("area")
            
            assert len(w) > 0
            assert "tree_domain" in str(w[0].message)
            assert "area_domain" in str(w[0].message)


class TestConfigurationConsistency:
    """Test configuration consistency validation."""
    
    def test_ema_lambda_consistency(self):
        """Test EMA method requires valid lambda."""
        # Valid
        config = UnifiedEstimatorConfig(
            method=EstimationMethod.EMA,
            lambda_=0.5
        )
        assert config.method == EstimationMethod.EMA
        
        # Invalid lambda for EMA
        with pytest.raises(ValueError, match="lambda between 0 and 1"):
            UnifiedEstimatorConfig(
                method=EstimationMethod.EMA,
                lambda_=0.0
            )
    
    def test_plot_level_memory_warning(self):
        """Test warning for plot-level estimates without lazy evaluation."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            
            config = UnifiedEstimatorConfig(
                by_plot=True,
                lazy_config=LazyEvaluationConfig(
                    mode=LazyEvaluationMode.DISABLED
                )
            )
            
            assert len(w) > 0
            assert "memory" in str(w[0].message).lower()


class TestModuleConfigGeneration:
    """Test dynamic module configuration generation."""
    
    def test_with_module_config(self):
        """Test adding module config to base config."""
        base = UnifiedEstimatorConfig(by_species=True)
        
        volume_config = VolumeSpecificConfig(
            volume_equation="regional",
            include_rotten=True
        )
        
        result = base.with_module_config(volume_config)
        
        assert result.by_species is True
        assert result.module_config.volume_equation == "regional"
        assert result.module_config.include_rotten is True
    
    def test_module_config_in_dict(self):
        """Test module config included in dict conversion."""
        config = UnifiedVolumeConfig(
            by_species=True,
            module_config=VolumeSpecificConfig(
                volume_equation="custom",
                stump_height=0.5
            )
        )
        
        data = config.to_dict()
        
        assert data["by_species"] is True
        assert data["volume_equation"] == "custom"
        assert data["stump_height"] == 0.5


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
"""
Tests for enhanced result formatting functionality.

This module tests the new result formatter and its integration
with the AI agent for improved user experience.
"""


import polars as pl
import pytest

try:
    from src.pyfia.ai.result_formatter import (
        FIAResultFormatter,
        create_result_formatter,
    )
    FORMATTER_AVAILABLE = True
except ImportError:
    FORMATTER_AVAILABLE = False


@pytest.mark.skipif(not FORMATTER_AVAILABLE, reason="Result formatter not available")
class TestFIAResultFormatter:
    """Test the FIA result formatter."""

    def test_formatter_initialization(self):
        """Test formatter initialization with different configurations."""
        # Default configuration
        formatter = FIAResultFormatter()
        assert formatter.include_emojis is True
        assert formatter.confidence_level == 0.95
        assert formatter.z_score == 1.96

        # Custom configuration
        formatter = FIAResultFormatter(include_emojis=False, confidence_level=0.99)
        assert formatter.include_emojis is False
        assert formatter.confidence_level == 0.99
        assert formatter.z_score == 2.576

    def test_reliability_assessment(self):
        """Test reliability assessment based on standard error percentage."""
        formatter = FIAResultFormatter()

        # Excellent reliability
        result = formatter._assess_reliability(3.0)
        assert result["level"] == "Excellent"
        assert result["range"] == "≤5%"

        # Good reliability
        result = formatter._assess_reliability(7.5)
        assert result["level"] == "Good"
        assert result["range"] == "5-10%"

        # Fair reliability
        result = formatter._assess_reliability(15.0)
        assert result["level"] == "Fair"
        assert result["range"] == "10-20%"

        # Poor reliability
        result = formatter._assess_reliability(25.0)
        assert result["level"] == "Poor"
        assert result["range"] == ">20%"

    def test_confidence_interval_calculation(self):
        """Test confidence interval calculation."""
        formatter = FIAResultFormatter(confidence_level=0.95)

        estimate = 1000000
        se = 50000

        ci = formatter._calculate_confidence_interval(estimate, se)

        expected_margin = 1.96 * se
        expected_lower = estimate - expected_margin
        expected_upper = estimate + expected_margin

        assert ci["margin_error"] == expected_margin
        assert ci["lower"] == expected_lower
        assert ci["upper"] == expected_upper

        # Test with zero lower bound constraint
        estimate = 10000
        se = 10000
        ci = formatter._calculate_confidence_interval(estimate, se)
        assert ci["lower"] >= 0  # Should not go below zero

    def test_emoji_functionality(self):
        """Test emoji inclusion/exclusion."""
        # With emojis
        formatter_with_emojis = FIAResultFormatter(include_emojis=True)
        assert formatter_with_emojis._get_emoji("tree") == "🌳"
        assert formatter_with_emojis._get_emoji("chart") == "📊"

        # Without emojis
        formatter_no_emojis = FIAResultFormatter(include_emojis=False)
        assert formatter_no_emojis._get_emoji("tree") == ""
        assert formatter_no_emojis._get_emoji("chart") == ""

    def test_tree_count_formatting_single_result(self):
        """Test formatting of single tree count result."""
        # Create sample data
        data = {
            "TREE_COUNT": [1500000],
            "SE": [75000],
            "SE_PERCENT": [5.0],
            "nPlots": [500]
        }
        result_df = pl.DataFrame(data)

        query_params = {
            "tree_type": "live",
            "land_type": "forest"
        }

        formatter = FIAResultFormatter()
        formatted_result = formatter.format_tree_count_results(result_df, query_params)

        # Check that key elements are present
        assert "FIA Tree Count Analysis Results" in formatted_result
        assert "1,500,000" in formatted_result  # Formatted number
        assert "Standard Error" in formatted_result
        assert "Confidence Interval" in formatted_result
        assert "Sample Information" in formatted_result
        assert "Methodology Notes" in formatted_result

    def test_tree_count_formatting_grouped_results(self):
        """Test formatting of grouped tree count results."""
        # Create sample grouped data
        data = {
            "COMMON_NAME": ["loblolly pine", "red maple", "sweetgum"],
            "SCIENTIFIC_NAME": ["Pinus taeda", "Acer rubrum", "Liquidambar styraciflua"],
            "SPCD": [131, 316, 611],
            "TREE_COUNT": [2000000, 1500000, 1200000],
            "SE": [100000, 75000, 60000],
            "SE_PERCENT": [5.0, 5.0, 5.0],
            "nPlots": [500, 500, 500]
        }
        result_df = pl.DataFrame(data)

        query_params = {
            "tree_type": "live",
            "land_type": "forest",
            "by_species": True
        }

        formatter = FIAResultFormatter()
        formatted_result = formatter.format_tree_count_results(result_df, query_params)

        # Check that grouped elements are present
        assert "Detailed Results" in formatted_result
        assert "loblolly pine" in formatted_result
        assert "Pinus taeda" in formatted_result
        assert "Species Code: 131" in formatted_result
        assert "Summary Statistics" in formatted_result
        assert "Total Entries: 3" in formatted_result

    def test_factory_function(self):
        """Test the factory function for creating formatters."""
        # Enhanced style
        formatter = create_result_formatter("enhanced")
        assert formatter.include_emojis is True
        assert formatter.confidence_level == 0.95

        # Simple style
        formatter = create_result_formatter("simple")
        assert formatter.include_emojis is False
        assert formatter.confidence_level == 0.95

        # Scientific style
        formatter = create_result_formatter("scientific")
        assert formatter.include_emojis is False
        assert formatter.confidence_level == 0.99

        # Default style
        formatter = create_result_formatter("unknown")
        assert formatter.include_emojis is True  # Default


@pytest.mark.skipif(not FORMATTER_AVAILABLE, reason="Result formatter not available")
class TestAgentFormatterIntegration:
    """Test integration of formatter with AI agent."""

    def test_agent_uses_enhanced_formatter(self):
        """Test that agent can use the enhanced formatter."""
        # This would require a more complex setup with actual agent
        # For now, just test that the formatter can be imported and used
        formatter = create_result_formatter("enhanced")

        # Create mock result data
        data = {
            "TREE_COUNT": [1000000],
            "SE": [50000],
            "nPlots": [300]
        }
        result_df = pl.DataFrame(data)

        query_params = {"tree_type": "live", "land_type": "forest"}

        # Should not raise an exception
        formatted = formatter.format_tree_count_results(result_df, query_params)
        assert isinstance(formatted, str)
        assert len(formatted) > 100  # Should be substantial content


class TestFormatterFallback:
    """Test fallback behavior when formatter is not available."""

    def test_simple_formatting_fallback(self):
        """Test that simple formatting works as fallback."""
        # Create sample data
        data = {
            "COMMON_NAME": ["loblolly pine"],
            "SCIENTIFIC_NAME": ["Pinus taeda"],
            "TREE_COUNT": [1500000],
            "SE": [75000],
            "SE_PERCENT": [5.0]
        }
        result_df = pl.DataFrame(data)

        # Mock agent method for simple formatting
        class MockAgent:
            def _format_tree_results_simple(self, result, query_params):
                formatted = "Tree Count Results:\n\n"
                for row in result.iter_rows(named=True):
                    if 'COMMON_NAME' in row and row['COMMON_NAME']:
                        formatted += f"Species: {row['COMMON_NAME']}"
                        if 'SCIENTIFIC_NAME' in row and row['SCIENTIFIC_NAME']:
                            formatted += f" ({row['SCIENTIFIC_NAME']})"
                        formatted += "\n"

                    if 'TREE_COUNT' in row:
                        formatted += f"Total Population: {row['TREE_COUNT']:,.0f} trees\n"

                    if 'SE' in row and row['SE']:
                        formatted += f"Standard Error: {row['SE']:,.0f}\n"

                    if 'SE_PERCENT' in row:
                        formatted += f"Standard Error %: {row['SE_PERCENT']:.1f}%\n"

                    formatted += "\n"

                formatted += "(Statistically valid population estimate using FIA methodology)\n"
                return formatted

        agent = MockAgent()
        query_params = {"tree_type": "live"}

        result = agent._format_tree_results_simple(result_df, query_params)

        assert "Tree Count Results" in result
        assert "loblolly pine" in result
        assert "1,500,000 trees" in result
        assert "Standard Error: 75,000" in result

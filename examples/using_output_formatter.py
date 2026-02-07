"""
Example of using the OutputFormatter with estimation functions.

This example demonstrates how to use the centralized output formatter
to ensure consistent output across different estimators.
"""

import polars as pl
from pyfia.constants.constants import EstimatorType
from pyfia.estimation.formatters import OutputFormatter, format_estimation_output


def example_direct_formatter_usage():
    """Example of using OutputFormatter directly."""
    
    # Simulate raw estimation output from area estimator
    raw_output = pl.DataFrame({
        "LAND_TYPE": ["Forest", "Timber", "Other"],
        "FA_TOTAL": [1000.0, 800.0, 200.0],
        "FAD_TOTAL": [2000.0, 2000.0, 2000.0],
        "AREA_PERC": [50.0, 40.0, 10.0],
        "AREA_PERC_VAR": [4.0, 3.0, 1.0],
        "nPlots": [100, 80, 20],
    })
    
    # Create formatter for area estimator
    formatter = OutputFormatter(EstimatorType.AREA)
    
    # Format the output
    formatted = formatter.format_output(
        raw_output,
        variance=False,  # Convert to SE
        totals=True,     # Include total columns
        group_cols=["LAND_TYPE"],
        year=2023
    )
    
    print("Formatted Area Output:")
    print(formatted)
    print()
    
    return formatted


def example_convenience_function():
    """Example using the convenience function."""
    
    # Simulate raw TPA output
    raw_tpa = pl.DataFrame({
        "SPCD": [110, 131, 833],
        "TPA": [125.5, 95.2, 78.3],
        "TPA_VAR": [15.5, 12.2, 9.8],
        "BAA": [85.2, 72.1, 65.5],
        "BAA_VAR": [8.5, 6.8, 5.2],
        "TREE_TOTAL": [50000, 40000, 35000],
        "BA_TOTAL": [35000, 30000, 28000],
        "nPlots_TREE": [250, 230, 210],
    })
    
    # Use convenience function
    formatted = format_estimation_output(
        raw_tpa,
        EstimatorType.TPA,
        variance=False,  # Output SE instead of variance
        totals=True,     # Include totals
        group_cols=["SPCD"],
        year=2023
    )
    
    print("Formatted TPA Output:")
    print(formatted)
    print()
    
    return formatted


def example_biomass_formatting():
    """Example of formatting biomass output."""
    
    # Simulate biomass estimation output
    raw_biomass = pl.DataFrame({
        "SPCD": [110, 131],
        "BIO_ACRE": [25.5, 32.8],
        "BIO_ACRE_SE": [1.2, 1.5],
        "CARB_ACRE": [12.0, 15.4],
        "CARB_ACRE_SE": [0.6, 0.7],
        "BIO_TOTAL": [125000, 164000],
        "CARB_TOTAL": [58750, 77080],
        "nPlots_TREE": [150, 180],
    })
    
    formatter = OutputFormatter(EstimatorType.BIOMASS)
    
    # Add metadata and ensure consistent formatting
    formatted = formatter.format_output(
        raw_biomass,
        variance=False,
        totals=True,
        group_cols=["SPCD"],
        year=2023
    )
    
    print("Formatted Biomass Output:")
    print(formatted)
    print()
    
    return formatted


def example_variance_conversion():
    """Example showing variance/SE conversion."""
    
    # Data with variance values
    data_with_var = pl.DataFrame({
        "VOLUME_ACRE": [1500.0, 1200.0],
        "VOLUME_ACRE_VAR": [225.0, 144.0],  # Variance
        "VOLUME": [750000, 600000],
        "VOLUME_VAR": [56250, 36000],
    })
    
    formatter = OutputFormatter(EstimatorType.VOLUME)
    
    # Convert to SE
    data_with_se = formatter.convert_variance_to_se(data_with_var)
    
    print("Converted to Standard Error:")
    print(data_with_se)
    print()
    
    # Convert back to variance
    data_with_var_again = formatter.convert_se_to_variance(data_with_se)
    
    print("Converted back to Variance:")
    print(data_with_var_again)
    print()
    

def example_custom_formatter():
    """Example of customizing the formatter for specific needs."""
    
    # Create a custom formatter by extending OutputFormatter
    class CustomAreaFormatter(OutputFormatter):
        def format_output(self, df, **kwargs):
            # First apply standard formatting
            df = super().format_output(df, **kwargs)
            
            # Add custom columns
            if "AREA_PERC" in df.columns:
                df = df.with_columns([
                    # Add confidence interval
                    (pl.col("AREA_PERC") - 1.96 * pl.col("AREA_PERC_SE")).alias("CI_LOWER"),
                    (pl.col("AREA_PERC") + 1.96 * pl.col("AREA_PERC_SE")).alias("CI_UPPER"),
                ])
            
            return df
    
    # Use custom formatter
    raw_data = pl.DataFrame({
        "AREA_PERC": [45.5, 54.5],
        "AREA_PERC_VAR": [4.0, 5.0],
        "N_PLOTS": [100, 120],
    })
    
    custom_formatter = CustomAreaFormatter(EstimatorType.AREA)
    formatted = custom_formatter.format_output(
        raw_data,
        variance=False,
        year=2023
    )
    
    print("Custom Formatted Output with Confidence Intervals:")
    print(formatted)
    print()


if __name__ == "__main__":
    print("=== Output Formatter Examples ===\n")
    
    # Run examples
    example_direct_formatter_usage()
    example_convenience_function()
    example_biomass_formatting()
    example_variance_conversion()
    example_custom_formatter()
    
    print("All examples completed successfully!")
"""
Data filtering and processing utilities for pyFIA.

This module provides all filtering functionality including:
- Tree and area filtering functions
- Domain expression parsing
- Domain indicator calculation
- Grouping and classification utilities
"""

from __future__ import annotations

# Core parsing functionality
# Filtering functions
from .filters import apply_area_filters, apply_plot_filters, apply_tree_filters

# Domain indicators
from .indicators import (
    LandTypeCategory,
    add_land_type_categories,
    classify_land_types,
    get_land_domain_indicator,
)
from .parser import DomainExpressionParser

# Classification functions
# Grouping functions
# Validation functions
from .utils import (
    ColumnValidator,
    add_forest_type_group,
    add_ownership_group_name,
    add_species_info,
    assign_forest_type_group,
    assign_size_class,
    assign_species_group,
    assign_tree_basis,
    check_columns,
    create_size_class_expr,
    ensure_columns,
    get_size_class_bounds,
    setup_grouping_columns,
    validate_columns,
)

__all__ = [
    # Core
    "DomainExpressionParser",
    # Filtering functions
    "apply_tree_filters",
    "apply_area_filters",
    "apply_plot_filters",
    # Domain indicators
    "classify_land_types",
    "get_land_domain_indicator",
    "add_land_type_categories",
    "LandTypeCategory",
    # Grouping
    "setup_grouping_columns",
    "create_size_class_expr",
    "get_size_class_bounds",
    "add_species_info",
    "add_ownership_group_name",
    "add_forest_type_group",
    # Classification
    "assign_tree_basis",
    "assign_size_class",
    "assign_forest_type_group",
    "assign_species_group",
    # Validation
    "ColumnValidator",
    "validate_columns",
    "check_columns",
    "ensure_columns",
]

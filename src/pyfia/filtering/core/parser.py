"""
Domain expression parser for FIA filtering.

This module provides a centralized DomainExpressionParser class to handle
all domain expression parsing throughout the pyFIA library, eliminating
code duplication between filters and estimation modules.
"""

from typing import Optional, TypeVar

import polars as pl

# Type variable for DataFrame/LazyFrame operations
FrameType = TypeVar("FrameType", pl.DataFrame, pl.LazyFrame)


class DomainExpressionParser:
    """
    Centralized parser for domain expressions used in FIA filtering.

    This class consolidates all domain expression parsing logic that was
    previously duplicated across multiple modules, providing a single
    source of truth for converting SQL-like domain strings into Polars
    expressions.
    """

    @staticmethod
    def parse(domain_expr: str, domain_type: str = "domain") -> pl.Expr:
        """
        Parse a SQL-like domain expression into a Polars expression.

        Parameters
        ----------
        domain_expr : str
            SQL-like expression string (e.g., "DIA >= 10.0", "STATUSCD == 1")
        domain_type : str, default "domain"
            Type of domain for error messages (e.g., "tree", "area", "plot")

        Returns
        -------
        pl.Expr
            Polars expression that can be used for filtering

        Raises
        ------
        ValueError
            If the domain expression is invalid or cannot be parsed

        Examples
        --------
        >>> expr = DomainExpressionParser.parse("DIA >= 10.0", "tree")
        >>> df_filtered = df.filter(expr)

        >>> expr = DomainExpressionParser.parse("OWNGRPCD == 40", "area")
        >>> df_filtered = df.filter(expr)
        """
        if not domain_expr or not domain_expr.strip():
            raise ValueError(
                f"Invalid {domain_type} domain expression: empty expression provided"
            )

        try:
            return pl.sql_expr(domain_expr)
        except Exception as e:
            # Provide consistent error message format
            raise ValueError(
                f"Invalid {domain_type} domain expression: {domain_expr}"
            ) from e

    @staticmethod
    def apply_to_dataframe(
        df: FrameType, domain_expr: str, domain_type: str = "domain"
    ) -> FrameType:
        """
        Apply a domain expression filter to a DataFrame or LazyFrame.

        This is a convenience method that combines parsing and filtering
        in a single operation. Works with both eager DataFrames and lazy
        LazyFrames, preserving the input type.

        Parameters
        ----------
        df : pl.DataFrame or pl.LazyFrame
            DataFrame or LazyFrame to filter
        domain_expr : str
            SQL-like expression string
        domain_type : str, default "domain"
            Type of domain for error messages

        Returns
        -------
        pl.DataFrame or pl.LazyFrame
            Filtered DataFrame/LazyFrame (same type as input)

        Raises
        ------
        ValueError
            If the domain expression is invalid

        Examples
        --------
        >>> filtered_df = DomainExpressionParser.apply_to_dataframe(
        ...     tree_df, "DIA >= 10.0", "tree"
        ... )
        >>> # Also works with LazyFrames
        >>> filtered_lf = DomainExpressionParser.apply_to_dataframe(
        ...     tree_lf, "DIA >= 10.0", "tree"
        ... )
        """
        expr = DomainExpressionParser.parse(domain_expr, domain_type)
        return df.filter(expr)

    # Alias for semantic clarity when working with LazyFrames
    apply_to_frame = apply_to_dataframe

    @staticmethod
    def create_indicator(
        domain_expr: str, domain_type: str = "domain", indicator_name: str = "indicator"
    ) -> pl.Expr:
        """
        Create a binary indicator column based on a domain expression.

        This method creates an expression that evaluates to 1 when the
        domain condition is met and 0 otherwise, useful for creating
        domain indicators in area estimation.

        Parameters
        ----------
        domain_expr : str
            SQL-like expression string
        domain_type : str, default "domain"
            Type of domain for error messages
        indicator_name : str, default "indicator"
            Name for the resulting indicator column

        Returns
        -------
        pl.Expr
            Expression that creates a binary indicator column

        Examples
        --------
        >>> indicator_expr = DomainExpressionParser.create_indicator(
        ...     "COND_STATUS_CD == 1", "area", "forestIndicator"
        ... )
        >>> df = df.with_columns(indicator_expr)
        """
        parsed_expr = DomainExpressionParser.parse(domain_expr, domain_type)
        return pl.when(parsed_expr).then(1).otherwise(0).alias(indicator_name)

    @staticmethod
    def validate_expression(
        domain_expr: str,
        domain_type: str = "domain",
        available_columns: Optional[list] = None,
    ) -> tuple[bool, Optional[str]]:
        """
        Validate a domain expression without applying it.

        Parameters
        ----------
        domain_expr : str
            SQL-like expression string to validate
        domain_type : str, default "domain"
            Type of domain for error messages
        available_columns : list, optional
            List of column names that should be available for the expression

        Returns
        -------
        tuple[bool, Optional[str]]
            (is_valid, error_message) - True with None if valid,
            False with error message if invalid

        Examples
        --------
        >>> is_valid, error = DomainExpressionParser.validate_expression(
        ...     "DIA >= 10.0", "tree", ["DIA", "HT", "STATUSCD"]
        ... )
        >>> if not is_valid:
        ...     print(f"Invalid expression: {error}")
        """
        # First try to parse the expression
        try:
            _expr = DomainExpressionParser.parse(domain_expr, domain_type)
        except ValueError as e:
            return False, str(e)

        # If column list provided, check if referenced columns exist
        # Note: This is a basic check - full validation would require
        # parsing the expression to extract column references
        if available_columns is not None:
            # Basic heuristic: check for common column references
            import re

            # Find potential column names (alphanumeric with underscores)
            potential_cols = re.findall(r"\b[A-Z][A-Z0-9_]*\b", domain_expr)
            missing_cols = [
                col
                for col in potential_cols
                if col not in available_columns
                and col not in ["AND", "OR", "NOT", "IN", "IS", "NULL"]
            ]
            if missing_cols:
                return False, f"Referenced columns not available: {missing_cols}"

        return True, None

    @staticmethod
    def combine_expressions(
        expressions: list[str], operator: str = "AND", domain_type: str = "domain"
    ) -> pl.Expr:
        """
        Combine multiple domain expressions with a logical operator.

        Parameters
        ----------
        expressions : list[str]
            List of SQL-like expression strings to combine
        operator : str, default "AND"
            Logical operator to use ("AND" or "OR")
        domain_type : str, default "domain"
            Type of domain for error messages

        Returns
        -------
        pl.Expr
            Combined Polars expression

        Raises
        ------
        ValueError
            If any expression is invalid or operator is not supported

        Examples
        --------
        >>> combined = DomainExpressionParser.combine_expressions(
        ...     ["DIA >= 10.0", "STATUSCD == 1"], "AND", "tree"
        ... )
        >>> df_filtered = df.filter(combined)
        """
        if not expressions:
            raise ValueError("No expressions provided to combine")

        if operator.upper() not in ["AND", "OR"]:
            raise ValueError(f"Unsupported operator: {operator}")

        # Parse all expressions
        parsed_exprs = [
            DomainExpressionParser.parse(expr, domain_type) for expr in expressions
        ]

        # Combine using the specified operator
        if operator.upper() == "AND":
            combined = parsed_exprs[0]
            for expr in parsed_exprs[1:]:
                combined = combined & expr
        else:  # OR
            combined = parsed_exprs[0]
            for expr in parsed_exprs[1:]:
                combined = combined | expr

        return combined

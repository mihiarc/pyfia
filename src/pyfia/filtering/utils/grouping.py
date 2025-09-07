"""
Grouping utilities for FIA estimation.

This module provides functions for setting up grouping columns used in
estimation aggregation, including species grouping and size class creation.
"""

from typing import List, Optional, Union

import polars as pl

from .grouping_functions import create_size_class_expr


def setup_grouping_columns(
    data_df: pl.DataFrame,
    grp_by: Optional[Union[str, List[str]]] = None,
    by_species: bool = False,
    by_size_class: bool = False,
    return_dataframe: bool = True,
) -> Union[tuple[pl.DataFrame, List[str]], List[str]]:
    """
    Set up grouping columns for estimation aggregation.

    This function handles the configuration of grouping columns used in
    estimation functions. It processes user-specified grouping columns,
    adds species grouping if requested, and creates size class categories
    based on tree diameter.

    Parameters
    ----------
    data_df : pl.DataFrame
        Dataframe to add grouping columns to (typically tree_cond joined data)
    grp_by : Optional[Union[str, List[str]]], default None
        Column name(s) to group by in addition to species/size class
    by_species : bool, default False
        If True, add SPCD (species code) to grouping columns
    by_size_class : bool, default False
        If True, create size class column based on diameter and add to grouping
    return_dataframe : bool, default True
        If True, return tuple of (modified_dataframe, group_columns).
        If False, return only group_columns list (for compatibility with
        biomass module pattern).

    Returns
    -------
    Union[tuple[pl.DataFrame, List[str]], List[str]]
        If return_dataframe=True: (modified dataframe, list of grouping columns)
        If return_dataframe=False: list of grouping columns only

    Notes
    -----
    Size classes follow FIA standards:
    - 1.0-4.9": Saplings
    - 5.0-9.9": Pole timber
    - 10.0-19.9": Small sawtimber
    - 20.0-29.9": Large sawtimber
    - 30.0+": Very large trees

    Examples
    --------
    >>> # Group by species and size class
    >>> df, groups = setup_grouping_columns(
    ...     tree_cond,
    ...     by_species=True,
    ...     by_size_class=True
    ... )

    >>> # Custom grouping with forest type
    >>> df, groups = setup_grouping_columns(
    ...     tree_cond,
    ...     grp_by="FORTYPCD",
    ...     by_species=True
    ... )
    """
    group_cols = []

    # Process user-specified grouping columns
    if grp_by:
        if isinstance(grp_by, str):
            group_cols = [grp_by]
        else:
            group_cols = list(grp_by)

    # Add species grouping
    if by_species:
        group_cols.append("SPCD")

    # Add size class grouping
    if by_size_class:
        # Use centralized size class function from grouping module
        size_class_expr = create_size_class_expr("DIA", "standard")
        data_df = data_df.with_columns(size_class_expr)
        group_cols.append("SIZE_CLASS")

    # Return based on requested format
    if return_dataframe:
        return data_df, group_cols
    else:
        return group_cols
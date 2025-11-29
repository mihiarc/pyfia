"""
Utility module for joining reference tables in pyFIA.

This module provides functions to easily join FIA reference tables
with estimation results to add descriptive names and metadata.
"""

from typing import Optional, Union

import polars as pl

from ..core.fia import FIA


def join_forest_type_names(
    data: pl.DataFrame,
    db: Union[str, FIA],
    forest_type_col: str = "FORTYPCD",
    name_col: str = "FOREST_TYPE_NAME",
) -> pl.DataFrame:
    """
    Join forest type names from REF_FOREST_TYPE table.

    Parameters
    ----------
    data : pl.DataFrame
        DataFrame containing forest type codes
    db : FIA or str
        FIA database object or path to database
    forest_type_col : str, default "FORTYPCD"
        Column name containing forest type codes
    name_col : str, default "FOREST_TYPE_NAME"
        Name for the joined forest type name column

    Returns
    -------
    pl.DataFrame
        Original data with forest type names added

    Examples
    --------
    >>> results = area(db, grp_by=['FORTYPCD'], totals=True)
    >>> results_with_names = join_forest_type_names(results, db)
    """
    # Ensure we have a FIA object
    if isinstance(db, str):
        db = FIA(db)

    # Check if column exists
    if forest_type_col not in data.columns:
        return data

    # Read forest type reference table
    forest_types = db._reader.read_table(
        "REF_FOREST_TYPE", columns=["VALUE", "MEANING"]
    )

    # Handle lazy frames
    if hasattr(forest_types, "collect"):
        forest_types = forest_types.collect()

    # Prepare reference data
    forest_types = (
        forest_types.with_columns(pl.col("VALUE").cast(pl.Int64))
        .rename({"VALUE": forest_type_col, "MEANING": name_col})
        .unique(forest_type_col)
    )

    # Join with original data
    return data.join(forest_types, on=forest_type_col, how="left")


def join_species_names(
    data: pl.DataFrame,
    db: Union[str, FIA],
    species_col: str = "SPCD",
    common_name_col: str = "COMMON_NAME",
    scientific_name_col: Optional[str] = "SCIENTIFIC_NAME",
    include_scientific: bool = False,
) -> pl.DataFrame:
    """
    Join species names from REF_SPECIES table.

    Parameters
    ----------
    data : pl.DataFrame
        DataFrame containing species codes
    db : FIA or str
        FIA database object or path to database
    species_col : str, default "SPCD"
        Column name containing species codes
    common_name_col : str, default "COMMON_NAME"
        Name for the joined common name column
    scientific_name_col : str, optional
        Name for the joined scientific name column
    include_scientific : bool, default False
        Whether to include scientific names

    Returns
    -------
    pl.DataFrame
        Original data with species names added

    Examples
    --------
    >>> results = tpa(db, bySpecies=True)
    >>> results_with_names = join_species_names(results, db)
    """
    if isinstance(db, str):
        db = FIA(db)

    if species_col not in data.columns:
        return data

    # Determine columns to read
    ref_cols = ["SPCD", "COMMON_NAME"]
    if include_scientific:
        ref_cols.append("SCIENTIFIC_NAME")

    # Read species reference table
    species = db._reader.read_table("REF_SPECIES", columns=ref_cols)

    if hasattr(species, "collect"):
        species = species.collect()

    # Rename columns
    rename_map = {"SPCD": species_col, "COMMON_NAME": common_name_col}
    if include_scientific and scientific_name_col:
        rename_map["SCIENTIFIC_NAME"] = scientific_name_col

    species = species.rename(rename_map).unique(species_col)

    return data.join(species, on=species_col, how="left")


def join_state_names(
    data: pl.DataFrame,
    db: Union[str, FIA],
    state_col: str = "STATECD",
    state_name_col: str = "STATE_NAME",
    state_abbr_col: Optional[str] = "STATE_ABBR",
    include_abbr: bool = True,
) -> pl.DataFrame:
    """
    Join state names and abbreviations from REF_STATE.

    Parameters
    ----------
    data : pl.DataFrame
        DataFrame containing state codes
    db : FIA or str
        FIA database object or path to database
    state_col : str, default "STATECD"
        Column name containing state codes
    state_name_col : str, default "STATE_NAME"
        Name for the joined state name column
    state_abbr_col : str, optional
        Name for the joined state abbreviation column
    include_abbr : bool, default True
        Whether to include state abbreviations

    Returns
    -------
    pl.DataFrame
        Original data with state names added
    """
    if isinstance(db, str):
        db = FIA(db)

    if state_col not in data.columns:
        return data

    # Determine columns to read
    ref_cols = ["VALUE", "MEANING"]
    if include_abbr:
        ref_cols.append("ABBR")

    # Read state reference table
    states = db._reader.read_table("REF_STATE", columns=ref_cols)

    if hasattr(states, "collect"):
        states = states.collect()

    # Prepare reference data
    states = states.with_columns(pl.col("VALUE").cast(pl.Int64))

    # Rename columns
    rename_map = {"VALUE": state_col, "MEANING": state_name_col}
    if include_abbr and state_abbr_col:
        rename_map["ABBR"] = state_abbr_col

    states = states.rename(rename_map).unique(state_col)

    return data.join(states, on=state_col, how="left")


def join_multiple_references(
    data: pl.DataFrame,
    db: Union[str, FIA],
    forest_type: bool = False,
    species: bool = False,
    state: bool = False,
    **kwargs,
) -> pl.DataFrame:
    """
    Join multiple reference tables at once.

    Parameters
    ----------
    data : pl.DataFrame
        DataFrame to join reference tables to
    db : FIA or str
        FIA database object or path to database
    forest_type : bool, default False
        Whether to join forest type names
    species : bool, default False
        Whether to join species names
    state : bool, default False
        Whether to join state names
    **kwargs
        Additional arguments passed to individual join functions

    Returns
    -------
    pl.DataFrame
        Data with requested reference tables joined

    Examples
    --------
    >>> results = area(db, grp_by=['STATECD', 'FORTYPCD'], totals=True)
    >>> results_with_names = join_multiple_references(
    ...     results, db,
    ...     forest_type=True,
    ...     state=True
    ... )
    """
    if forest_type:
        data = join_forest_type_names(data, db, **kwargs)

    if species:
        data = join_species_names(data, db, **kwargs)

    if state:
        data = join_state_names(data, db, **kwargs)

    return data

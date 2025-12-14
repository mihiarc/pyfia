"""
EVALID parser utilities for FIA evaluation identifiers.

EVALIDs encode state, year, and evaluation type in a 6-digit format: SSYYTT
where SS is the state FIPS code, YY is the 2-digit year, and TT is the eval type.

Year interpretation uses standard Y2K windowing:
- 00-30: Interpreted as 2000-2030
- 31-99: Interpreted as 1931-1999
"""

import polars as pl


def add_parsed_evalid_columns(df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
    """
    Add parsed EVALID columns to a Polars DataFrame.

    This function extracts the components from EVALID values and adds them
    as separate columns for sorting and filtering operations. It handles
    Y2K windowing correctly so that year 23 becomes 2023, not 1923.

    Parameters
    ----------
    df : polars.DataFrame or polars.LazyFrame
        DataFrame with an EVALID column.

    Returns
    -------
    polars.DataFrame or polars.LazyFrame
        DataFrame with additional columns:
        - EVALID_YEAR: 4-digit year (e.g., 2023)
        - EVALID_STATE: State FIPS code (e.g., 13 for Georgia)
        - EVALID_TYPE: Evaluation type code (e.g., 1 for volume)

    Examples
    --------
    >>> import polars as pl
    >>> df = pl.DataFrame({"EVALID": [132301, 132319, 481901]})
    >>> result = add_parsed_evalid_columns(df)
    >>> result.select(["EVALID", "EVALID_YEAR", "EVALID_STATE"]).to_dicts()
    [{'EVALID': 132301, 'EVALID_YEAR': 2023, 'EVALID_STATE': 13},
     {'EVALID': 132319, 'EVALID_YEAR': 2023, 'EVALID_STATE': 13},
     {'EVALID': 481901, 'EVALID_YEAR': 2019, 'EVALID_STATE': 48}]
    """
    return df.with_columns(
        [
            # Extract year with proper Y2K windowing interpretation
            # Years 00-30 -> 2000-2030, years 31-99 -> 1931-1999
            pl.when(pl.col("EVALID").cast(pl.Utf8).str.slice(2, 2).cast(pl.Int32) <= 30)
            .then(2000 + pl.col("EVALID").cast(pl.Utf8).str.slice(2, 2).cast(pl.Int32))
            .otherwise(
                1900 + pl.col("EVALID").cast(pl.Utf8).str.slice(2, 2).cast(pl.Int32)
            )
            .alias("EVALID_YEAR"),
            # Extract state code (first 2 digits)
            pl.col("EVALID")
            .cast(pl.Utf8)
            .str.slice(0, 2)
            .cast(pl.Int32)
            .alias("EVALID_STATE"),
            # Extract evaluation type (last 2 digits)
            pl.col("EVALID")
            .cast(pl.Utf8)
            .str.slice(4, 2)
            .cast(pl.Int32)
            .alias("EVALID_TYPE"),
        ]
    )

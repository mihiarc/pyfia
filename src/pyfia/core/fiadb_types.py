"""
FIADB column types for frames read from any FIADB database.

A database whose columns were typed by guessing from CSV files can store the
same column as text in one state and as a number in another. Casting what is
read to the types in :data:`pyfia.constants.fiadb_schema.COLUMN_TYPES` gives
the same dtypes from every database.
"""

from __future__ import annotations

import polars as pl

from ..constants.fiadb_schema import COLUMN_TYPES

#: DuckDB type in the schema -> polars dtype
POLARS_TYPES: dict[str, pl.DataType] = {
    "VARCHAR": pl.Utf8(),
    "BIGINT": pl.Int64(),
    "DOUBLE": pl.Float64(),
    "TIMESTAMP": pl.Datetime("us"),
}

# How DataMart writes DATETIME values
_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


def fiadb_dtype(table: str, column: str) -> pl.DataType | None:
    """Polars dtype of ``table.column`` in the FIADB schema, or None if undefined."""
    declared = COLUMN_TYPES.get(table, {}).get(column)
    return POLARS_TYPES[declared] if declared else None


def cast_to_fiadb_types(df: pl.DataFrame, table: str) -> pl.DataFrame:
    """
    Cast the columns of ``df`` that ``table`` defines to their FIADB types.

    Columns the schema doesn't define are left as they are.

    Parameters
    ----------
    df : pl.DataFrame
        Columns read from ``table``, under their FIADB names.
    table : str
        FIADB table name.

    Returns
    -------
    pl.DataFrame
        ``df`` with FIADB dtypes: String for VARCHAR (including every CN),
        Int64 for BIGINT, Float64 for DOUBLE and Datetime for TIMESTAMP.

    Raises
    ------
    ValueError
        If a numeric column holds text that isn't a number, or an integer
        column holds a value that isn't a whole number.
    """
    declared = COLUMN_TYPES.get(table)
    if not declared:
        return df
    exprs = []
    for name, dtype in df.schema.items():
        col_type = declared.get(name)
        if col_type is None:
            continue
        target = POLARS_TYPES[col_type]
        if dtype == target or (col_type == "TIMESTAMP" and dtype.is_temporal()):
            continue
        try:
            if col_type == "BIGINT" and not dtype.is_integer():
                values = df[name].cast(pl.Float64, strict=True)
                if (values.is_not_null() & (values != values.floor())).any():
                    raise ValueError("values that are not whole numbers")
                exprs.append(pl.col(name).cast(pl.Float64).cast(pl.Int64))
            elif col_type == "TIMESTAMP" and dtype == pl.Utf8:
                df[name].str.to_datetime(_TIMESTAMP_FORMAT, strict=True)
                exprs.append(pl.col(name).str.to_datetime(_TIMESTAMP_FORMAT))
            else:
                df[name].cast(target, strict=True)
                exprs.append(pl.col(name).cast(target))
        except (ValueError, pl.exceptions.PolarsError) as e:
            raise ValueError(
                f"{table}.{name} ({dtype}) doesn't cast to {col_type}: {e}"
            ) from e
    return df.with_columns(exprs) if exprs else df

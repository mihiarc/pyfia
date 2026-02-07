"""Utility functions for pyFIA core operations."""

from typing import Callable, List, Optional, TypeVar, Union

import polars as pl

from .settings import settings

T = TypeVar("T")


def batch_query_by_values(
    values: List[T],
    query_fn: Callable[[List[T]], Union[pl.DataFrame, pl.LazyFrame]],
    batch_size: Optional[int] = None,
) -> Union[pl.DataFrame, pl.LazyFrame]:
    """
    Execute queries in batches to avoid SQL IN clause limits.

    This utility function is used to break up large IN clause queries
    into smaller batches, avoiding database query size limits.

    Parameters
    ----------
    values : List[T]
        Values to batch (e.g., plot CNs)
    query_fn : Callable
        Function that takes a batch of values and returns a DataFrame/LazyFrame.
        The function should build and execute a query using the provided values.
    batch_size : int, optional
        Override default batch size from settings.
        Defaults to settings.sql_batch_size (900).

    Returns
    -------
    pl.DataFrame or pl.LazyFrame
        Concatenated results from all batches.
        Returns empty DataFrame if values is empty.

    Examples
    --------
    >>> def query_plots(cns):
    ...     cn_str = ", ".join(f"'{cn}'" for cn in cns)
    ...     return reader.read_table("PLOT", where=f"CN IN ({cn_str})")
    >>> result = batch_query_by_values(plot_cns, query_plots)
    """
    if batch_size is None:
        batch_size = settings.sql_batch_size

    if not values:
        return pl.DataFrame()

    results = []
    for i in range(0, len(values), batch_size):
        batch = values[i : i + batch_size]
        result = query_fn(batch)
        results.append(result)

    if len(results) == 1:
        return results[0]
    # Ensure consistent types for concat - collect LazyFrames if any exist
    first_is_lazy = isinstance(results[0], pl.LazyFrame)
    if first_is_lazy:
        return pl.concat([r if isinstance(r, pl.LazyFrame) else r.lazy() for r in results])
    else:
        return pl.concat([r.collect() if isinstance(r, pl.LazyFrame) else r for r in results])

#!/usr/bin/env python
"""
Proposed fix for the find_evalid method using the robust EVALID parser.

This shows how to update the find_evalid method to properly handle EVALID sorting.
"""

def find_evalid_fixed(
    self,
    most_recent: bool = True,
    state: Optional[Union[int, List[int]]] = None,
    year: Optional[Union[int, List[int]]] = None,
    eval_type: Optional[str] = None,
) -> List[int]:
    """
    Find EVALID values matching criteria with robust year handling.

    This updated version uses proper EVALID parsing to ensure correct
    chronological sorting regardless of the 2-digit year representation.
    """
    from .evalid_parser import add_parsed_evalid_columns, filter_most_recent_by_group
    import polars as pl
    import warnings

    # Load required tables if not already loaded
    try:
        if "POP_EVAL" not in self.tables:
            self.load_table("POP_EVAL")
        if "POP_EVAL_TYP" not in self.tables:
            self.load_table("POP_EVAL_TYP")

        # Get the data
        pop_eval = self.tables["POP_EVAL"].collect()
        pop_eval_typ = self.tables["POP_EVAL_TYP"].collect()

        # Check if EVALID exists in POP_EVAL
        if "EVALID" not in pop_eval.columns:
            raise ValueError(
                f"EVALID column not found in POP_EVAL table. Available columns: {pop_eval.columns}"
            )

        # Join on CN = EVAL_CN
        df = pop_eval.join(
            pop_eval_typ, left_on="CN", right_on="EVAL_CN", how="left"
        )
    except Exception as e:
        # If tables don't exist or join fails, return empty list with warning
        warnings.warn(f"Could not load evaluation tables: {e}")
        return []

    # Apply filters
    if state is not None:
        if isinstance(state, int):
            state = [state]
        df = df.filter(pl.col("STATECD").is_in(state))

    if year is not None:
        if isinstance(year, int):
            year = [year]
        df = df.filter(pl.col("END_INVYR").is_in(year))

    if eval_type is not None:
        # FIA uses 'EXP' prefix for evaluation types
        # Special case: "ALL" maps to "EXPALL" for area estimation
        if eval_type.upper() == "ALL":
            eval_type_full = "EXPALL"
        else:
            eval_type_full = f"EXP{eval_type}"
        df = df.filter(pl.col("EVAL_TYP") == eval_type_full)

    if most_recent:
        # ROBUST SOLUTION: Add parsed EVALID columns for proper sorting
        df = add_parsed_evalid_columns(df)

        # Special handling for Texas (STATECD=48)
        df_texas = df.filter(pl.col("STATECD") == 48)
        df_other = df.filter(pl.col("STATECD") != 48)

        if not df_texas.is_empty():
            # For Texas, prefer full state evaluations over regional ones
            if "LOCATION_NM" in df_texas.columns:
                # Mark full state evaluations (just "Texas" without parentheses)
                df_texas = df_texas.with_columns(
                    pl.when(pl.col("LOCATION_NM") == "Texas")
                    .then(1)
                    .otherwise(0)
                    .alias("IS_FULL_STATE")
                )
                # Sort by:
                # 1. State and eval type (grouping)
                # 2. Full state preference
                # 3. PARSED year (not END_INVYR in case of discrepancy)
                # 4. EVALID as last resort (lower is older within same year)
                df_texas = (
                    df_texas.sort(
                        ["STATECD", "EVAL_TYP", "IS_FULL_STATE", "EVALID_YEAR", "EVALID_TYPE"],
                        descending=[False, False, True, True, False]
                    )
                    .group_by(["STATECD", "EVAL_TYP"])
                    .first()
                    .drop(["IS_FULL_STATE", "EVALID_YEAR", "EVALID_STATE", "EVALID_TYPE"])
                )
            else:
                # Fallback if LOCATION_NM not available
                df_texas = (
                    df_texas.sort(
                        ["STATECD", "EVAL_TYP", "EVALID_YEAR", "EVALID_TYPE"],
                        descending=[False, False, True, False]
                    )
                    .group_by(["STATECD", "EVAL_TYP"])
                    .first()
                    .drop(["EVALID_YEAR", "EVALID_STATE", "EVALID_TYPE"])
                )

        # For other states, use robust year sorting
        if not df_other.is_empty():
            df_other = (
                df_other.sort(
                    ["STATECD", "EVAL_TYP", "EVALID_YEAR", "EVALID_TYPE"],
                    descending=[False, False, True, False]
                )
                .group_by(["STATECD", "EVAL_TYP"])
                .first()
                .drop(["EVALID_YEAR", "EVALID_STATE", "EVALID_TYPE"])
            )

        # Combine Texas and other states
        df_list = []
        if not df_texas.is_empty():
            df_list.append(df_texas)
        if not df_other.is_empty():
            df_list.append(df_other)

        if df_list:
            df = pl.concat(df_list)

    # Extract unique EVALIDs
    evalids = df.select("EVALID").unique()["EVALID"].to_list()

    # If we still need to sort the final list, use the robust parser
    if evalids and most_recent:
        from .evalid_parser import sort_evalids_by_year
        evalids = sort_evalids_by_year(evalids, descending=True)

    return evalids


# Alternative: Even simpler approach that always uses robust parsing
def find_evalid_simple(
    self,
    most_recent: bool = True,
    state: Optional[Union[int, List[int]]] = None,
    year: Optional[Union[int, List[int]]] = None,
    eval_type: Optional[str] = None,
) -> List[int]:
    """
    Simplified version that always uses EVALID parsing for robustness.

    This version prioritizes correctness and clarity over minor performance.
    """
    from .evalid_parser import ParsedEvalid, parse_evalid
    import polars as pl
    import warnings

    # Load and join tables (same as before)
    try:
        if "POP_EVAL" not in self.tables:
            self.load_table("POP_EVAL")
        if "POP_EVAL_TYP" not in self.tables:
            self.load_table("POP_EVAL_TYP")

        pop_eval = self.tables["POP_EVAL"].collect()
        pop_eval_typ = self.tables["POP_EVAL_TYP"].collect()

        if "EVALID" not in pop_eval.columns:
            raise ValueError(f"EVALID column not found in POP_EVAL table")

        df = pop_eval.join(
            pop_eval_typ, left_on="CN", right_on="EVAL_CN", how="left"
        )
    except Exception as e:
        warnings.warn(f"Could not load evaluation tables: {e}")
        return []

    # Apply basic filters
    if state is not None:
        if isinstance(state, int):
            state = [state]
        df = df.filter(pl.col("STATECD").is_in(state))

    if year is not None:
        if isinstance(year, int):
            year = [year]
        df = df.filter(pl.col("END_INVYR").is_in(year))

    if eval_type is not None:
        if eval_type.upper() == "ALL":
            eval_type_full = "EXPALL"
        else:
            eval_type_full = f"EXP{eval_type}"
        df = df.filter(pl.col("EVAL_TYP") == eval_type_full)

    # Convert to list of records for processing
    records = df.to_dicts()

    if not records:
        return []

    # Parse all EVALIDs and attach to records
    for record in records:
        record["parsed"] = parse_evalid(record["EVALID"])

    if most_recent:
        # Group by state and eval type
        from collections import defaultdict
        groups = defaultdict(list)

        for record in records:
            # Special handling for Texas
            if record["STATECD"] == 48:
                # Prefer full state over regional
                is_full_state = record.get("LOCATION_NM") == "Texas"
                priority = 0 if is_full_state else 1
            else:
                priority = 0

            key = (record["STATECD"], record.get("EVAL_TYP", ""))
            groups[key].append((priority, record))

        # Get most recent from each group
        selected_evalids = []
        for key, group_records in groups.items():
            # Sort by priority (full state first), then by parsed year
            group_records.sort(
                key=lambda x: (x[0], -x[1]["parsed"].year_4digit, x[1]["parsed"].eval_type)
            )
            selected_evalids.append(group_records[0][1]["EVALID"])

        return selected_evalids
    else:
        # Return all EVALIDs, sorted by year
        records.sort(key=lambda x: (-x["parsed"].year_4digit, x["parsed"].eval_type))
        return [r["EVALID"] for r in records]
"""
EVALID selection and filtering functions for FIA data analysis.

This module provides functions for finding and selecting appropriate EVALIDs
(Evaluation IDs) that ensure statistically valid FIA estimates. It handles
prioritization of statewide over regional evaluations and selection of the
most appropriate evaluation type for different analysis needs.
"""

from typing import Dict, Optional, Tuple, Union

from ..database.query_interface import DuckDBQueryInterface


def find_best_evalid(
    query_interface: DuckDBQueryInterface,
    state_code: int,
    eval_type: str = "EXPVOL",
    prefer_statewide: bool = True,
    most_recent: bool = True,
) -> Optional[int]:
    """
    Find the best EVALID for a given state and evaluation type.

    This function implements intelligent EVALID selection that prioritizes
    the most recent evaluation by default, with optional preference for
    statewide evaluations over regional ones.

    Parameters
    ----------
    query_interface : DuckDBQueryInterface
        Database interface for executing queries
    state_code : int
        FIPS state code (e.g., 48 for Texas)
    eval_type : str, default "EXPVOL"
        Evaluation type: "EXPVOL" (volume), "EXPCURR" (current),
        "EXPGROW" (growth), "EXPMORT" (mortality), etc.
    prefer_statewide : bool, default False
        Prefer statewide evaluations over regional ones
    most_recent : bool, default True
        Select the most recent evaluation

    Returns
    -------
    int or None
        Best EVALID matching criteria, or None if none found
    """
    query = """
    SELECT
        pe.EVALID,
        pe.EVAL_DESCR,
        pe.STATECD,
        pe.START_INVYR,
        pe.END_INVYR,
        pet.EVAL_TYP,
        COUNT(DISTINCT ppsa.PLT_CN) as plot_count,
        CASE
            WHEN pe.EVAL_DESCR LIKE '%(%' THEN 1  -- Regional (contains parentheses)
            ELSE 0  -- Statewide
        END as is_regional
    FROM POP_EVAL pe
    LEFT JOIN POP_EVAL_TYP pet ON pe.CN = pet.EVAL_CN
    LEFT JOIN POP_PLOT_STRATUM_ASSGN ppsa ON pe.EVALID = ppsa.EVALID
    WHERE pe.STATECD = ?
      AND pet.EVAL_TYP = ?
    GROUP BY pe.EVALID, pe.EVAL_DESCR, pe.STATECD,
             pe.START_INVYR, pe.END_INVYR, pet.EVAL_TYP
    """

    # Build ORDER BY clause - prioritize statewide, then most recent
    if prefer_statewide and most_recent:
        query += "ORDER BY is_regional ASC, pe.END_INVYR DESC"
    elif prefer_statewide:
        query += "ORDER BY is_regional ASC, pe.EVALID DESC"
    elif most_recent:
        query += "ORDER BY pe.END_INVYR DESC, pe.EVALID DESC"
    else:
        query += "ORDER BY pe.EVALID DESC"

    query += " LIMIT 1"

    try:
        # Replace placeholders with actual values
        final_query = query.replace("pe.STATECD = ?", f"pe.STATECD = {state_code}")
        final_query = final_query.replace("pet.EVAL_TYP = ?", f"pet.EVAL_TYP = '{eval_type}'")

        result = query_interface.execute_query(final_query)

        if len(result) > 0:
            return result.row(0, named=True)["EVALID"]
        return None

    except Exception:
        return None


def get_evalid_info(
    query_interface: DuckDBQueryInterface,
    state_code: Optional[int] = None,
    eval_type: Optional[str] = None,
    limit: int = 20,
) -> str:
    """
    Get formatted information about available EVALIDs.

    Parameters
    ----------
    query_interface : DuckDBQueryInterface
        Database interface for executing queries
    state_code : int, optional
        FIPS state code to filter by
    eval_type : str, optional
        Evaluation type to filter by
    limit : int, default 20
        Maximum number of results to return

    Returns
    -------
    str
        Formatted EVALID information
    """
    query = """
    SELECT
        pe.EVALID,
        pe.EVAL_DESCR,
        pe.STATECD,
        pe.START_INVYR,
        pe.END_INVYR,
        pet.EVAL_TYP,
        COUNT(DISTINCT ppsa.PLT_CN) as plot_count,
        CASE
            WHEN pe.EVAL_DESCR LIKE '%(%' THEN 'Regional'
            ELSE 'Statewide'
        END as scope
    FROM POP_EVAL pe
    LEFT JOIN POP_EVAL_TYP pet ON pe.CN = pet.EVAL_CN
    LEFT JOIN POP_PLOT_STRATUM_ASSGN ppsa ON pe.EVALID = ppsa.EVALID
    WHERE 1=1
    """

    if state_code:
        query += f" AND pe.STATECD = {state_code}"
    if eval_type:
        query += f" AND pet.EVAL_TYP = '{eval_type}'"

    query += """
    GROUP BY pe.EVALID, pe.EVAL_DESCR, pe.STATECD,
             pe.START_INVYR, pe.END_INVYR, pet.EVAL_TYP
    ORDER BY pe.STATECD,
             pe.END_INVYR DESC,
             CASE WHEN pe.EVAL_DESCR LIKE '%(%' THEN 1 ELSE 0 END
    """

    if limit:
        query += f" LIMIT {limit}"

    try:
        result = query_interface.execute_query(query)

        if len(result) == 0:
            return "No evaluations found matching criteria"

        formatted = "FIA Evaluations:\n"
        for row in result.iter_rows(named=True):
            formatted += f"\n- EVALID {row['EVALID']}: {row['EVAL_DESCR']}"
            formatted += f"\n  State: {row['STATECD']}, Type: {row['EVAL_TYP']}, Scope: {row['scope']}"
            formatted += f"\n  Years: {row['START_INVYR']}-{row['END_INVYR']}"
            formatted += f"\n  Plots: {row['plot_count']:,}\n"

        return formatted

    except Exception as e:
        return f"Error getting EVALID info: {str(e)}"


def get_recommended_evalid(
    query_interface: DuckDBQueryInterface,
    state_code: int,
    analysis_type: str = "tree_count",
) -> Tuple[Optional[int], str]:
    """
    Get recommended EVALID for a specific analysis type with explanation.

    Parameters
    ----------
    query_interface : DuckDBQueryInterface
        Database interface for executing queries
    state_code : int
        FIPS state code
    analysis_type : str, default "tree_count"
        Type of analysis: "tree_count", "volume", "biomass", "area",
        "growth", "mortality"

    Returns
    -------
    tuple
        (EVALID, explanation) where EVALID is the recommended evaluation ID
        and explanation describes why this EVALID was chosen
    """
    # Map analysis types to evaluation types
    eval_type_map = {
        "tree_count": "EXPVOL",
        "volume": "EXPVOL",
        "biomass": "EXPVOL",
        "area": "EXPCURR",
        "growth": "EXPGROW",
        "mortality": "EXPMORT",
        "tpa": "EXPVOL",
    }

    eval_type = eval_type_map.get(analysis_type, "EXPVOL")

    # Find the best EVALID - prioritize statewide evaluations
    evalid = find_best_evalid(
        query_interface=query_interface,
        state_code=state_code,
        eval_type=eval_type,
        prefer_statewide=True,
        most_recent=True,
    )

    if evalid is None:
        return None, f"No suitable {eval_type} evaluation found for state {state_code}"

    # Get details about the selected EVALID
    try:
        detail_query = f"""
        SELECT
            pe.EVALID,
            pe.EVAL_DESCR,
            pe.START_INVYR,
            pe.END_INVYR,
            pet.EVAL_TYP,
            COUNT(DISTINCT ppsa.PLT_CN) as plot_count,
            CASE
                WHEN pe.EVAL_DESCR LIKE '%(%' THEN 'regional'
                ELSE 'statewide'
            END as scope
        FROM POP_EVAL pe
        LEFT JOIN POP_EVAL_TYP pet ON pe.CN = pet.EVAL_CN
        LEFT JOIN POP_PLOT_STRATUM_ASSGN ppsa ON pe.EVALID = ppsa.EVALID
        WHERE pe.EVALID = {evalid}
          AND pet.EVAL_TYP = '{eval_type}'
        GROUP BY pe.EVALID, pe.EVAL_DESCR, pe.START_INVYR, pe.END_INVYR, pet.EVAL_TYP
        """

        result = query_interface.execute_query(detail_query)
        if len(result) > 0:
            row = result.row(0, named=True)
            explanation = (
                f"Using EVALID {evalid} ({row['EVAL_DESCR']}) - "
                f"the most recent {row['scope']} {eval_type} evaluation "
                f"covering {row['START_INVYR']}-{row['END_INVYR']} "
                f"with {row['plot_count']:,} plots"
            )
        else:
            explanation = f"Using EVALID {evalid} for {analysis_type} analysis"

    except Exception:
        explanation = f"Using EVALID {evalid} for {analysis_type} analysis"

    return evalid, explanation


def validate_evalid(
    query_interface: DuckDBQueryInterface,
    evalid: int,
) -> Dict[str, Union[bool, str, int]]:
    """
    Validate an EVALID and return information about it.

    Parameters
    ----------
    query_interface : DuckDBQueryInterface
        Database interface for executing queries
    evalid : int
        EVALID to validate

    Returns
    -------
    dict
        Validation results with keys: 'valid', 'description', 'state_code',
        'eval_type', 'plot_count', 'years'
    """
    query = f"""
    SELECT
        pe.EVALID,
        pe.EVAL_DESCR,
        pe.STATECD,
        pe.START_INVYR,
        pe.END_INVYR,
        pet.EVAL_TYP,
        COUNT(DISTINCT ppsa.PLT_CN) as plot_count
    FROM POP_EVAL pe
    LEFT JOIN POP_EVAL_TYP pet ON pe.CN = pet.EVAL_CN
    LEFT JOIN POP_PLOT_STRATUM_ASSGN ppsa ON pe.EVALID = ppsa.EVALID
    WHERE pe.EVALID = {evalid}
    GROUP BY pe.EVALID, pe.EVAL_DESCR, pe.STATECD,
             pe.START_INVYR, pe.END_INVYR, pet.EVAL_TYP
    """

    try:
        result = query_interface.execute_query(query)

        if len(result) == 0:
            return {
                'valid': False,
                'description': f'EVALID {evalid} not found',
                'state_code': None,
                'eval_type': None,
                'plot_count': 0,
                'years': None,
            }

        row = result.row(0, named=True)
        return {
            'valid': True,
            'description': row['EVAL_DESCR'],
            'state_code': row['STATECD'],
            'eval_type': row['EVAL_TYP'],
            'plot_count': row['plot_count'],
            'years': f"{row['START_INVYR']}-{row['END_INVYR']}",
        }

    except Exception as e:
        return {
            'valid': False,
            'description': f'Error validating EVALID {evalid}: {str(e)}',
            'state_code': None,
            'eval_type': None,
            'plot_count': 0,
            'years': None,
        }

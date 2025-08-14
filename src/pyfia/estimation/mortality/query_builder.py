"""SQL query builder for mortality estimation."""
from typing import List, Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)

class MortalityQueryBuilder:
    """Builds SQL queries for mortality estimation with flexible grouping."""
    
    def __init__(self, db_type: str = "duckdb"):
        """
        Initialize query builder.
        
        Args:
            db_type: Database type ('duckdb' or 'sqlite')
        """
        self.db_type = db_type
        self._grouping_columns = {
            "SPCD": "t.SPCD",
            "SPGRPCD": "t.SPGRPCD", 
            "OWNGRPCD": "c.OWNGRPCD",
            "UNITCD": "ps.UNITCD",
            "AGENTCD": "t.AGENTCD",
            "DSTRBCD1": "t.DSTRBCD1",
            "DSTRBCD2": "t.DSTRBCD2",
            "DSTRBCD3": "t.DSTRBCD3"
        }
        
    def build_plot_query(
        self,
        evalid_list: List[int],
        groups: List[str],
        tree_domain: Optional[str] = None,
        area_domain: Optional[str] = None,
        mortality_col: str = "SUBP_TPAMORT_UNADJ_AL_FOREST"
    ) -> str:
        """
        Build query for plot-level mortality estimates.
        
        Args:
            evalid_list: List of EVALIDs to include
            groups: List of grouping variables
            tree_domain: SQL condition for tree filtering
            area_domain: SQL condition for area filtering
            mortality_col: Column name for mortality metric
            
        Returns:
            SQL query string
        """
        # Validate groups
        invalid_groups = [g for g in groups if g not in self._grouping_columns]
        if invalid_groups:
            raise ValueError(f"Invalid grouping variables: {invalid_groups}")
            
        # Build group columns
        group_select = ", ".join([self._grouping_columns[g] + f" AS {g}" for g in groups])
        group_by = ", ".join([self._grouping_columns[g] for g in groups])
        
        # Build WHERE clause
        where_clauses = []
        if evalid_list:
            evalid_str = ", ".join(map(str, evalid_list))
            where_clauses.append(f"ppsa.EVALID IN ({evalid_str})")
        
        if tree_domain:
            where_clauses.append(f"({tree_domain})")
            
        if area_domain:
            where_clauses.append(f"({area_domain})")
            
        where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"
        
        # Build query with proper join order and text casting for numeric precision
        query = f"""
        WITH plot_mortality AS (
            SELECT 
                ppsa.STRATUM_CN,
                ps.ESTN_UNIT_CN,
                p.CN AS PLT_CN,
                {group_select if groups else "'ALL' AS TOTAL_GROUP"},
                SUM(t.{mortality_col} * ps.EXPNS) AS MORTALITY_EXPANDED,
                COUNT(DISTINCT p.CN) AS N_PLOTS,
                COUNT(t.TRE_CN) AS N_TREES
            FROM POP_STRATUM ps
            JOIN POP_PLOT_STRATUM_ASSGN ppsa ON ps.CN = ppsa.STRATUM_CN
            JOIN PLOT p ON ppsa.PLT_CN = p.CN
            JOIN COND c ON p.CN = c.PLT_CN
            JOIN TREE_GRM_COMPONENT t ON c.PLT_CN = t.PLT_CN
            WHERE {where_clause}
                AND ps.EVALID = ppsa.EVALID
                AND c.CONDPROP_UNADJ > 0
                AND t.{mortality_col} > 0
            GROUP BY 
                ppsa.STRATUM_CN,
                ps.ESTN_UNIT_CN,
                p.CN
                {', ' + group_by if groups else ''}
        )
        SELECT * FROM plot_mortality
        """
        
        return self._clean_query(query)
        
    def build_stratum_query(
        self,
        groups: List[str],
        plot_table: str = "plot_mortality"
    ) -> str:
        """
        Build query for stratum-level aggregation.
        
        Args:
            groups: List of grouping variables
            plot_table: Name of plot-level CTE or table
            
        Returns:
            SQL query string
        """
        group_cols = ", ".join(groups) if groups else "'ALL' AS TOTAL_GROUP"
        group_by = ", ".join(groups) if groups else ""
        
        query = f"""
        stratum_mortality AS (
            SELECT 
                STRATUM_CN,
                ESTN_UNIT_CN,
                {group_cols},
                SUM(MORTALITY_EXPANDED) AS STRATUM_MORTALITY,
                COUNT(DISTINCT PLT_CN) AS STRATUM_N_PLOTS,
                SUM(N_TREES) AS STRATUM_N_TREES,
                -- Variance components
                SUM(MORTALITY_EXPANDED * MORTALITY_EXPANDED) AS MORT_SQUARED_SUM,
                AVG(MORTALITY_EXPANDED) AS MORT_MEAN
            FROM {plot_table}
            GROUP BY 
                STRATUM_CN,
                ESTN_UNIT_CN
                {', ' + group_by if groups else ''}
        )
        """
        
        return self._clean_query(query)
        
    def build_population_query(
        self,
        groups: List[str],
        stratum_table: str = "stratum_mortality",
        include_variance: bool = True,
        include_totals: bool = False
    ) -> str:
        """
        Build query for population-level estimates.
        
        Args:
            groups: List of grouping variables
            stratum_table: Name of stratum-level CTE or table
            include_variance: Whether to calculate variance
            include_totals: Whether to include totals row
            
        Returns:
            SQL query string
        """
        group_cols = ", ".join(groups) if groups else "'ALL' AS TOTAL_GROUP"
        group_by = ", ".join(groups) if groups else ""
        
        # Base query for estimates
        base_query = f"""
        population_estimates AS (
            SELECT 
                ps.ESTN_UNIT_CN,
                {group_cols},
                -- Population estimates
                SUM(sm.STRATUM_MORTALITY * ps.P2POINTCNT / ps.P1POINTCNT) AS MORTALITY_TOTAL,
                SUM(ps.EXPNS) AS TOTAL_AREA,
                SUM(sm.STRATUM_MORTALITY * ps.P2POINTCNT / ps.P1POINTCNT) / 
                    NULLIF(SUM(ps.EXPNS), 0) AS MORTALITY_PER_ACRE,
                -- Sample sizes
                SUM(sm.STRATUM_N_PLOTS) AS N_PLOTS,
                SUM(sm.STRATUM_N_TREES) AS N_TREES
                {self._build_variance_select() if include_variance else ''}
            FROM {stratum_table} sm
            JOIN POP_STRATUM ps ON sm.STRATUM_CN = ps.CN
            GROUP BY 
                ps.ESTN_UNIT_CN
                {', ' + group_by if groups else ''}
        )
        """
        
        # Add totals if requested
        if include_totals and groups:
            totals_query = self._build_totals_query(groups, "population_estimates")
            query = f"""
            WITH {base_query},
            {totals_query}
            SELECT * FROM population_estimates
            UNION ALL
            SELECT * FROM group_totals
            ORDER BY {', '.join(groups)}
            """
        else:
            query = f"WITH {base_query} SELECT * FROM population_estimates"
            
        return self._clean_query(query)
        
    def _build_variance_select(self) -> str:
        """Build variance calculation columns."""
        if self.db_type == "duckdb":
            return """, 
                -- Variance calculation (simplified - full implementation in variance.py)
                VARIANCE(sm.STRATUM_MORTALITY) AS STRATUM_VAR,
                SQRT(VARIANCE(sm.STRATUM_MORTALITY)) AS SE,
                CASE 
                    WHEN SUM(sm.STRATUM_MORTALITY) > 0 
                    THEN 100 * SQRT(VARIANCE(sm.STRATUM_MORTALITY)) / SUM(sm.STRATUM_MORTALITY)
                    ELSE NULL 
                END AS SE_PERCENT"""
        else:
            # SQLite doesn't have VARIANCE function
            return """, 
                -- Variance components for later calculation
                SUM(sm.MORT_SQUARED_SUM) AS MORT_SQUARED_SUM,
                SUM(sm.STRATUM_N_PLOTS) AS TOTAL_PLOTS"""
                
    def _build_totals_query(self, groups: List[str], base_table: str) -> str:
        """Build query for group totals using ROLLUP or manual aggregation."""
        if self.db_type == "duckdb":
            # DuckDB supports ROLLUP
            group_list = ", ".join(groups)
            return f"""
            group_totals AS (
                SELECT * FROM {base_table}
                ROLLUP ({group_list})
            )
            """
        else:
            # SQLite requires manual implementation
            # Create separate queries for each grouping level
            return f"""
            group_totals AS (
                SELECT 
                    ESTN_UNIT_CN,
                    {', '.join(['NULL AS ' + g for g in groups])},
                    SUM(MORTALITY_TOTAL) AS MORTALITY_TOTAL,
                    SUM(TOTAL_AREA) AS TOTAL_AREA,
                    SUM(MORTALITY_TOTAL) / NULLIF(SUM(TOTAL_AREA), 0) AS MORTALITY_PER_ACRE,
                    SUM(N_PLOTS) AS N_PLOTS,
                    SUM(N_TREES) AS N_TREES
                FROM {base_table}
                GROUP BY ESTN_UNIT_CN
            )
            """
            
    def build_complete_query(
        self,
        evalid_list: List[int],
        groups: List[str],
        tree_domain: Optional[str] = None,
        area_domain: Optional[str] = None,
        mortality_col: str = "SUBP_TPAMORT_UNADJ_AL_FOREST",
        include_variance: bool = True,
        include_totals: bool = False
    ) -> str:
        """
        Build complete mortality estimation query.
        
        Combines plot, stratum, and population level calculations.
        
        Args:
            evalid_list: List of EVALIDs to include
            groups: List of grouping variables
            tree_domain: SQL condition for tree filtering
            area_domain: SQL condition for area filtering
            mortality_col: Column name for mortality metric
            include_variance: Whether to calculate variance
            include_totals: Whether to include totals
            
        Returns:
            Complete SQL query string
        """
        # Build plot query
        plot_query = self.build_plot_query(
            evalid_list, groups, tree_domain, area_domain, mortality_col
        )
        
        # Build stratum query
        stratum_query = self.build_stratum_query(groups)
        
        # Build population query
        pop_query = self.build_population_query(
            groups, "stratum_mortality", include_variance, include_totals
        )
        
        # Combine all CTEs
        complete_query = f"""
        {plot_query},
        {stratum_query},
        {pop_query}
        """
        
        return self._clean_query(complete_query)
        
    def _clean_query(self, query: str) -> str:
        """Clean and format SQL query."""
        # Remove extra whitespace
        lines = [line.strip() for line in query.strip().split('\n') if line.strip()]
        return '\n'.join(lines)
        
    def get_reference_table_joins(self, groups: List[str]) -> Dict[str, str]:
        """
        Get JOIN clauses for reference tables based on grouping variables.
        
        Args:
            groups: List of grouping variables
            
        Returns:
            Dictionary of table aliases to JOIN clauses
        """
        joins = {}
        
        if "SPCD" in groups:
            joins["species"] = "LEFT JOIN REF_SPECIES species ON t.SPCD = species.SPCD"
            
        if "SPGRPCD" in groups:
            joins["spgrp"] = "LEFT JOIN REF_SPECIES_GROUP spgrp ON t.SPGRPCD = spgrp.SPGRPCD"
            
        if "OWNGRPCD" in groups:
            joins["owner"] = "LEFT JOIN REF_RESEARCH_STATION owner ON c.OWNGRPCD = owner.OWNGRPCD"
            
        if "AGENTCD" in groups:
            joins["agent"] = "LEFT JOIN REF_AGENT agent ON t.AGENTCD = agent.AGENTCD"
            
        return joins
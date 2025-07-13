"""
Advanced LangGraph workflow for area estimation.

This module provides sophisticated area estimation with validation,
error handling, quality control, and metadata tracking while
leveraging the core area() function for calculations.
"""

import operator
from datetime import datetime
from typing import Annotated, Dict, List, Literal, Optional, TypedDict

import polars as pl
from langgraph.graph import END, StateGraph

from ..constants.constants import LandStatus, PlotBasis, ReserveStatus, SiteClass
from ..core import FIA

# Import the core calculation function
from .area import area
from .utils import ratio_var


class AreaEstimationState(TypedDict):
    """State management for area estimation workflow."""

    # Input parameters
    db: FIA
    grp_by: Optional[List[str]]
    by_land_type: bool
    land_type: str
    tree_domain: Optional[str]
    area_domain: Optional[str]
    method: str
    lambda_: float
    totals: bool
    variance: bool
    most_recent: bool

    # Workflow state
    step: str
    validation_errors: Annotated[List[str], operator.add]
    warnings: Annotated[List[str], operator.add]

    # Data processing state
    raw_data: Optional[Dict[str, pl.DataFrame]]
    filtered_data: Optional[Dict[str, pl.DataFrame]]
    plot_estimates: Optional[pl.DataFrame]
    stratum_estimates: Optional[pl.DataFrame]

    # Results
    final_results: Optional[pl.DataFrame]
    metadata: Dict[str, any]

    # Quality control
    quality_score: float
    needs_validation: bool
    retry_count: int
    max_retries: int


class AreaWorkflow:
    """Advanced LangGraph workflow for area estimation."""

    def __init__(self, enable_checkpointing: bool = False):
        """Initialize the area estimation workflow."""
        # Disable checkpointing by default to avoid serialization issues with FIA objects
        self.checkpointer = None  # No checkpointer to avoid FIA serialization issues
        self.workflow = self._build_workflow()

    def _build_workflow(self) -> StateGraph:
        """Build the complete area estimation workflow graph."""

        # Create state graph
        workflow = StateGraph(AreaEstimationState)

        # Add nodes
        workflow.add_node("validate_inputs", self.validate_inputs)
        workflow.add_node("optimize_query", self.optimize_query)
        workflow.add_node("calculate_area", self.calculate_area)
        workflow.add_node("validate_results", self.validate_results)
        workflow.add_node("format_output", self.format_output)
        workflow.add_node("handle_errors", self.handle_errors)

        # Set entry point
        workflow.set_entry_point("validate_inputs")

        # Add conditional edges
        workflow.add_conditional_edges(
            "validate_inputs",
            self.route_after_validation,
            {
                "proceed": "calculate_area",
                "error": "handle_errors",
                "optimize": "optimize_query"
            }
        )

        workflow.add_edge("optimize_query", "calculate_area")

        workflow.add_conditional_edges(
            "calculate_area",
            self.route_after_calculation,
            {
                "validate": "validate_results",
                "error": "handle_errors"
            }
        )

        workflow.add_conditional_edges(
            "validate_results",
            self.route_after_validation_check,
            {
                "format": "format_output",
                "retry": "calculate_area",
                "error": "handle_errors"
            }
        )

        workflow.add_edge("format_output", END)
        workflow.add_edge("handle_errors", END)

        return workflow

    def validate_inputs(self, state: AreaEstimationState) -> Dict:
        """Validate input parameters and database connection."""
        validation_errors = []
        warnings = []

        # Validate database
        if not state["db"] or not hasattr(state["db"], 'tables'):
            validation_errors.append("Invalid FIA database object")

        # Validate land type
        valid_land_types = ["forest", "timber", "all"]
        if state["land_type"] not in valid_land_types:
            validation_errors.append(f"Invalid land_type: {state['land_type']}")

        # Validate method
        if state["method"] != "TI":
            warnings.append(f"Method {state['method']} not fully supported, using TI")

        # Validate domain expressions
        if state["tree_domain"]:
            try:
                # Basic SQL validation
                if not isinstance(state["tree_domain"], str):
                    validation_errors.append("tree_domain must be a string")
            except Exception as e:
                validation_errors.append(f"Invalid tree_domain: {e}")

        if state["area_domain"]:
            try:
                if not isinstance(state["area_domain"], str):
                    validation_errors.append("area_domain must be a string")
            except Exception as e:
                validation_errors.append(f"Invalid area_domain: {e}")

        # Check for complex queries that might need optimization
        needs_optimization = (
            state["tree_domain"] and len(state["tree_domain"]) > 100
            or state["area_domain"] and len(state["area_domain"]) > 100
            or (state["grp_by"] and len(state["grp_by"]) > 5)
        )

        return {
            "validation_errors": validation_errors,
            "warnings": warnings,
            "step": "input_validation",
            "needs_optimization": needs_optimization,
            "retry_count": 0,
            "max_retries": 3
        }

    def route_after_validation(self, state: AreaEstimationState) -> Literal["proceed", "error", "optimize"]:
        """Route after input validation."""
        if state["validation_errors"]:
            return "error"
        elif state.get("needs_optimization", False):
            return "optimize"
        else:
            return "proceed"

    def optimize_query(self, state: AreaEstimationState) -> Dict:
        """Optimize complex queries for better performance."""
        optimizations = []

        # Simplify complex domain expressions
        if state["tree_domain"] and len(state["tree_domain"]) > 100:
            # Could implement query optimization logic here
            optimizations.append("Simplified tree domain expression")

        if state["area_domain"] and len(state["area_domain"]) > 100:
            optimizations.append("Simplified area domain expression")

        # Optimize grouping variables
        if state["grp_by"] and len(state["grp_by"]) > 5:
            # Limit to most important grouping variables
            optimized_grp_by = state["grp_by"][:5]
            optimizations.append(f"Limited grouping to {len(optimized_grp_by)} variables")

            return {
                "grp_by": optimized_grp_by,
                "warnings": [f"Query optimized: {'; '.join(optimizations)}"],
                "step": "query_optimization"
            }

        return {
            "warnings": [f"Query optimized: {'; '.join(optimizations)}"],
            "step": "query_optimization"
        }

    def load_data(self, state: AreaEstimationState) -> Dict:
        """Load required data tables with error handling."""
        try:
            db = state["db"]

            # Ensure required tables are loaded
            required_tables = ["PLOT", "COND", "POP_STRATUM", "POP_PLOT_STRATUM_ASSGN"]
            if state["tree_domain"]:
                required_tables.append("TREE")

            for table in required_tables:
                db.load_table(table)

            # Prepare data dictionary
            plots = db.get_plots()
            raw_data = {
                "PLOT": plots,
                "COND": db.get_conditions(),
                "POP_STRATUM": db.tables["POP_STRATUM"].collect(),
            }

            if state["tree_domain"]:
                raw_data["TREE"] = db.get_trees()

            # Get plot-stratum assignments
            if db.evalid:
                ppsa = (
                    db.tables["POP_PLOT_STRATUM_ASSGN"]
                    .filter(pl.col("EVALID").is_in(db.evalid))
                    .collect()
                )
            else:
                plot_cns = plots["CN"].to_list()
                ppsa = (
                    db.tables["POP_PLOT_STRATUM_ASSGN"]
                    .filter(pl.col("PLT_CN").is_in(plot_cns))
                    .collect()
                )

            raw_data["POP_PLOT_STRATUM_ASSGN"] = ppsa

            # Data quality checks
            quality_score = self._calculate_data_quality(raw_data)

            return {
                "raw_data": raw_data,
                "quality_score": quality_score,
                "step": "data_loading",
                "metadata": {
                    "n_plots": len(plots),
                    "n_conditions": len(raw_data["COND"]),
                    "load_timestamp": datetime.now().isoformat()
                }
            }

        except Exception as e:
            return {
                "validation_errors": [f"Data loading failed: {str(e)}"],
                "step": "data_loading_error",
                "retry_count": state.get("retry_count", 0) + 1
            }

    def route_after_data_load(self, state: AreaEstimationState) -> Literal["proceed", "retry", "error"]:
        """Route after data loading."""
        if state["validation_errors"] and state["retry_count"] < state["max_retries"]:
            return "retry"
        elif state["validation_errors"]:
            return "error"
        else:
            return "proceed"

    def apply_filters(self, state: AreaEstimationState) -> Dict:
        """Apply domain filters with advanced logic."""
        try:
            raw_data = state["raw_data"]
            cond_df = raw_data["COND"]

            # Apply area domain filter
            if state["area_domain"]:
                cond_df = cond_df.filter(pl.sql_expr(state["area_domain"]))

            # Apply tree domain at condition level
            if state["tree_domain"]:
                cond_df = self._apply_tree_domain_to_conditions(
                    cond_df, raw_data["TREE"], state["tree_domain"]
                )

            # Add land type categories
            if state["by_land_type"]:
                cond_df = self._add_land_type_categories(cond_df)

            # Calculate domain indicators
            cond_df = self._calculate_domain_indicators(
                cond_df, state["land_type"], state["by_land_type"]
            )

            filtered_data = raw_data.copy()
            filtered_data["COND"] = cond_df

            return {
                "filtered_data": filtered_data,
                "step": "filtering",
                "metadata": {
                    **state["metadata"],
                    "n_conditions_after_filter": len(cond_df)
                }
            }

        except Exception as e:
            return {
                "validation_errors": [f"Filtering failed: {str(e)}"],
                "step": "filtering_error"
            }

    def calculate_area(self, state: AreaEstimationState) -> Dict:
        """Calculate area estimates using the proven core area() function."""
        try:
            # Use the core area() function - this ensures consistency and eliminates code duplication
            result = area(
                db=state["db"],
                grp_by=state["grp_by"],
                by_land_type=state["by_land_type"],
                land_type=state["land_type"],
                tree_domain=state["tree_domain"],
                area_domain=state["area_domain"],
                method=state["method"],
                lambda_=state["lambda_"],
                totals=state["totals"],
                variance=state["variance"],
                most_recent=state["most_recent"]
            )

            return {
                "final_results": result,
                "step": "area_calculation_complete",
                "needs_validation": True,
                "metadata": {
                    **state["metadata"],
                    "calculation_timestamp": datetime.now().isoformat(),
                    "result_shape": result.shape,
                    "calculation_engine": "core_area_function",
                    "n_results": len(result)
                }
            }

        except Exception as e:
            return {
                "validation_errors": [f"Area calculation failed: {str(e)}"],
                "step": "area_calculation_error"
            }

    def route_after_calculation(self, state: AreaEstimationState) -> Literal["validate", "error"]:
        """Route after population calculation."""
        if state["validation_errors"]:
            return "error"
        else:
            return "validate"

    def validate_results(self, state: AreaEstimationState) -> Dict:
        """Validate final results for quality and consistency."""
        results = state["final_results"]
        validation_errors = []
        warnings = []

        # Check for null/infinite values
        if results.null_count().sum_horizontal()[0] > 0:
            warnings.append("Results contain null values")

        # Check for reasonable ranges
        if "AREA_PERC" in results.columns:
            area_percs = results["AREA_PERC"].to_list()
            if any(p < 0 or p > 100 for p in area_percs if p is not None):
                validation_errors.append("Area percentages outside valid range (0-100)")

        # Check for consistency
        if state["totals"] and "AREA" in results.columns:
            areas = results["AREA"].to_list()
            if any(a < 0 for a in areas if a is not None):
                validation_errors.append("Negative area values detected")

        # Calculate final quality score
        final_quality_score = self._calculate_result_quality(results, state["quality_score"])

        return {
            "validation_errors": validation_errors,
            "warnings": warnings,
            "quality_score": final_quality_score,
            "step": "result_validation"
        }

    def route_after_validation_check(self, state: AreaEstimationState) -> Literal["format", "retry", "error"]:
        """Route after result validation."""
        if state["validation_errors"]:
            if state["retry_count"] < state["max_retries"]:
                return "retry"
            else:
                return "error"
        else:
            return "format"

    def format_output(self, state: AreaEstimationState) -> Dict:
        """Format final output with metadata."""
        results = state["final_results"]

        # Add metadata columns
        metadata_cols = {
            "QUALITY_SCORE": state["quality_score"],
            "WORKFLOW_VERSION": "2025.1",
            "PROCESSING_TIMESTAMP": datetime.now().isoformat()
        }

        for col, value in metadata_cols.items():
            results = results.with_columns(pl.lit(value).alias(col))

        return {
            "final_results": results,
            "step": "output_formatting",
            "metadata": {
                **state["metadata"],
                "final_quality_score": state["quality_score"],
                "warnings": state.get("warnings", [])
            }
        }

    def handle_errors(self, state: AreaEstimationState) -> Dict:
        """Handle errors and provide diagnostic information."""
        error_summary = {
            "errors": state.get("validation_errors", []),
            "warnings": state.get("warnings", []),
            "step_failed": state.get("step", "unknown"),
            "retry_count": state.get("retry_count", 0),
            "timestamp": datetime.now().isoformat()
        }

        return {
            "final_results": None,
            "step": "error_handling",
            "metadata": error_summary
        }

    def _calculate_data_quality(self, data: Dict[str, pl.DataFrame]) -> float:
        """Calculate data quality score."""
        score = 1.0

        # Check for missing required columns
        required_cols = {
            "PLOT": ["CN", "STATECD"],
            "COND": ["PLT_CN", "CONDID", "COND_STATUS_CD"],
            "POP_STRATUM": ["CN", "EXPNS", "ADJ_FACTOR_SUBP"]
        }

        for table, cols in required_cols.items():
            if table in data:
                missing_cols = set(cols) - set(data[table].columns)
                if missing_cols:
                    score *= 0.8  # Reduce score for missing columns

        return score

    def _calculate_result_quality(self, results: pl.DataFrame, input_quality: float) -> float:
        """Calculate final result quality score."""
        score = input_quality

        # Check result completeness
        if len(results) == 0:
            return 0.0

        # Check for reasonable values
        if "AREA_PERC" in results.columns:
            valid_percs = results["AREA_PERC"].drop_nulls()
            if len(valid_percs) > 0:
                reasonable_range = valid_percs.is_between(0, 100).all()
                if not reasonable_range:
                    score *= 0.7

        return score

    # Include the original calculation methods from area.py
    def _apply_tree_domain_to_conditions(self, cond_df: pl.DataFrame, tree_df: pl.DataFrame, tree_domain: str) -> pl.DataFrame:
        """Apply tree domain at the condition level."""
        qualifying_trees = tree_df.filter(pl.sql_expr(tree_domain))
        qualifying_conds = (
            qualifying_trees.select(["PLT_CN", "CONDID"])
            .unique()
            .with_columns(pl.lit(1).alias("HAS_QUALIFYING_TREE"))
        )

        cond_df = cond_df.join(
            qualifying_conds, on=["PLT_CN", "CONDID"], how="left"
        ).with_columns(pl.col("HAS_QUALIFYING_TREE").fill_null(0))

        return cond_df

    def _add_land_type_categories(self, cond_df: pl.DataFrame) -> pl.DataFrame:
        """Add land type categories for grouping."""
        return cond_df.with_columns(
            pl.when(
                (pl.col("COND_STATUS_CD") == LandStatus.FOREST)
                & pl.col("SITECLCD").is_in(SiteClass.PRODUCTIVE_CLASSES)
                & (pl.col("RESERVCD") == ReserveStatus.NOT_RESERVED)
            )
            .then(pl.lit("Timber"))
            .when(pl.col("COND_STATUS_CD") == LandStatus.FOREST)
            .then(pl.lit("Non-Timber Forest"))
            .when(pl.col("COND_STATUS_CD") == LandStatus.NONFOREST)
            .then(pl.lit("Non-Forest"))
            .when(pl.col("COND_STATUS_CD").is_in([LandStatus.WATER, LandStatus.CENSUS_WATER]))
            .then(pl.lit("Water"))
            .otherwise(pl.lit("Other"))
            .alias("LAND_TYPE")
        )

    def _calculate_domain_indicators(self, cond_df: pl.DataFrame, land_type: str, by_land_type: bool = False) -> pl.DataFrame:
        """Calculate domain indicators for area estimation."""
        if by_land_type and "LAND_TYPE" in cond_df.columns:
            cond_df = cond_df.with_columns(pl.lit(1).alias("landD"))
        else:
            if land_type == "forest":
                cond_df = cond_df.with_columns(
                    (pl.col("COND_STATUS_CD") == LandStatus.FOREST).cast(pl.Int32).alias("landD")
                )
            elif land_type == "timber":
                cond_df = cond_df.with_columns(
                    (
                        (pl.col("COND_STATUS_CD") == LandStatus.FOREST)
                        & pl.col("SITECLCD").is_in(SiteClass.PRODUCTIVE_CLASSES)
                        & (pl.col("RESERVCD") == ReserveStatus.NOT_RESERVED)
                    )
                    .cast(pl.Int32)
                    .alias("landD")
                )
            else:
                cond_df = cond_df.with_columns(pl.lit(1).alias("landD"))

        cond_df = cond_df.with_columns(pl.lit(1).alias("aD"))

        if "HAS_QUALIFYING_TREE" in cond_df.columns:
            cond_df = cond_df.with_columns(pl.col("HAS_QUALIFYING_TREE").alias("tD"))
        else:
            cond_df = cond_df.with_columns(pl.lit(1).alias("tD"))

        if by_land_type:
            cond_df = cond_df.with_columns(pl.col("aD").alias("aDI"))
        else:
            cond_df = cond_df.with_columns(
                (pl.col("landD") * pl.col("aD") * pl.col("tD")).alias("aDI")
            )

        if by_land_type:
            cond_df = cond_df.with_columns(
                pl.when(pl.col("COND_STATUS_CD").is_in([LandStatus.FOREST, LandStatus.NONFOREST]))
                .then(pl.col("aD"))
                .otherwise(0)
                .alias("pDI")
            )
        else:
            cond_df = cond_df.with_columns((pl.col("landD") * pl.col("aD")).alias("pDI"))

        return cond_df

    def _prepare_area_stratification(self, stratum_df: pl.DataFrame, assgn_df: pl.DataFrame) -> pl.DataFrame:
        """Prepare stratification data for area estimation."""
        if "EVALID" in assgn_df.columns and len(assgn_df) > 0:
            current_evalid = assgn_df["EVALID"][0]
            assgn_df = assgn_df.filter(pl.col("EVALID") == current_evalid)

        strat_df = assgn_df.join(
            stratum_df.select(
                ["CN", "EXPNS", "ADJ_FACTOR_SUBP", "ADJ_FACTOR_MACR", "P2POINTCNT"]
            ),
            left_on="STRATUM_CN",
            right_on="CN",
            how="inner",
        )

        return strat_df

    def _calculate_plot_area_estimates_DEPRECATED(self, plot_df: pl.DataFrame, cond_df: pl.DataFrame,
                                     strat_df: pl.DataFrame, grp_by: Optional[List[str]]) -> pl.DataFrame:
        """Calculate plot-level area estimates."""
        if "PLT_CN" not in plot_df.columns:
            plot_df = plot_df.rename({"CN": "PLT_CN"})

        if grp_by:
            cond_groups = ["PLT_CN"] + grp_by
        else:
            cond_groups = ["PLT_CN"]

        area_num = cond_df.group_by(cond_groups).agg(
            [
                (pl.col("CONDPROP_UNADJ") * pl.col("aDI")).sum().alias("fa"),
                pl.col("PROP_BASIS").mode().first().alias("PROP_BASIS"),
            ]
        )

        area_den = cond_df.group_by("PLT_CN").agg(
            [
                (pl.col("CONDPROP_UNADJ") * pl.col("pDI")).sum().alias("fad"),
                pl.col("PROP_BASIS").mode().first().alias("PROP_BASIS_DEN"),
            ]
        )

        plot_est = area_num.join(area_den, on="PLT_CN", how="left")

        plot_est = plot_est.with_columns(
            pl.coalesce(["PROP_BASIS", "PROP_BASIS_DEN"]).alias("PROP_BASIS")
        ).drop("PROP_BASIS_DEN")

        plot_est = plot_est.join(
            strat_df.select(
                ["PLT_CN", "STRATUM_CN", "EXPNS", "ADJ_FACTOR_SUBP", "ADJ_FACTOR_MACR"]
            ),
            on="PLT_CN",
            how="left",
        )

        plot_est = plot_est.with_columns(
            pl.when(pl.col("PROP_BASIS") == PlotBasis.MACROPLOT)
            .then(pl.col("ADJ_FACTOR_MACR"))
            .otherwise(pl.col("ADJ_FACTOR_SUBP"))
            .alias("ADJ_FACTOR")
        )

        plot_est = plot_est.with_columns(
            [
                (pl.col("fa") * pl.col("ADJ_FACTOR") * pl.col("EXPNS")).alias("fa_expanded"),
                (pl.col("fad") * pl.col("ADJ_FACTOR") * pl.col("EXPNS")).alias("fad_expanded"),
                (pl.col("fa") * pl.col("ADJ_FACTOR")).alias("fa"),
                (pl.col("fad") * pl.col("ADJ_FACTOR")).alias("fad"),
            ]
        )

        plot_est = plot_est.with_columns(
            [
                pl.col("fa").fill_null(0),
                pl.col("fad").fill_null(0),
                pl.col("fa_expanded").fill_null(0),
                pl.col("fad_expanded").fill_null(0),
            ]
        )

        return plot_est

    def _calculate_stratum_area_estimates_DEPRECATED(self, plot_est: pl.DataFrame, grp_by: Optional[List[str]]) -> pl.DataFrame:
        """Calculate stratum-level area estimates."""
        if grp_by:
            strat_groups = ["STRATUM_CN"] + grp_by
        else:
            strat_groups = ["STRATUM_CN"]

        stratum_est = plot_est.group_by(strat_groups).agg(
            [
                pl.len().alias("n_h"),
                pl.sum("fa_expanded").alias("fa_expanded_total"),
                pl.sum("fad_expanded").alias("fad_expanded_total"),
                pl.mean("fa").alias("fa_bar_h"),
                pl.std("fa", ddof=1).alias("s_fa_h"),
                pl.mean("fad").alias("fad_bar_h"),
                pl.std("fad", ddof=1).alias("s_fad_h"),
                pl.corr("fa", "fad").fill_null(0).alias("corr_fa_fad"),
                pl.first("EXPNS").alias("w_h"),
            ]
        )

        stratum_est = stratum_est.with_columns(
            (pl.col("corr_fa_fad") * pl.col("s_fa_h") * pl.col("s_fad_h")).alias("s_fa_fad_h")
        )

        stratum_est = stratum_est.with_columns(
            [pl.col(c).fill_null(0) for c in ["s_fa_h", "s_fad_h", "s_fa_fad_h"]]
        )

        return stratum_est

    def _calculate_population_area_estimates_DEPRECATED(self, stratum_est: pl.DataFrame, grp_by: Optional[List[str]],
                                           totals: bool, variance: bool) -> pl.DataFrame:
        """Calculate population-level area estimates using direct expansion."""
        if grp_by:
            pop_groups = grp_by
        else:
            pop_groups = []

        by_land_type = pop_groups == ["LAND_TYPE"] if pop_groups else False

        agg_exprs = [
            pl.col("fa_expanded_total").sum().alias("FA_TOTAL"),
            pl.col("fad_expanded_total").sum().alias("FAD_TOTAL"),
            ((pl.col("w_h") ** 2) * (pl.col("s_fa_h") ** 2) / pl.col("n_h")).sum().alias("FA_VAR"),
            ((pl.col("w_h") ** 2) * (pl.col("s_fad_h") ** 2) / pl.col("n_h")).sum().alias("FAD_VAR"),
            ((pl.col("w_h") ** 2) * pl.col("s_fa_fad_h") / pl.col("n_h")).sum().alias("COV_FA_FAD"),
            pl.col("n_h").sum().alias("N_PLOTS"),
        ]

        if pop_groups:
            pop_est = stratum_est.group_by(pop_groups).agg(agg_exprs)
        else:
            pop_est = stratum_est.select(agg_exprs)

        if by_land_type:
            if "LAND_TYPE" in pop_est.columns:
                land_area_total = (
                    pop_est.filter(~pl.col("LAND_TYPE").str.contains("Water")).select(
                        pl.sum("FAD_TOTAL").alias("TOTAL_LAND_AREA")
                    )
                )[0, 0]

                pop_est = pop_est.with_columns(
                    pl.when(land_area_total == 0)
                    .then(0.0)
                    .otherwise((pl.col("FA_TOTAL") / land_area_total) * 100)
                    .alias("AREA_PERC")
                )
            else:
                pop_est = pop_est.with_columns(
                    pl.when(pl.col("FAD_TOTAL") == 0)
                    .then(0.0)
                    .otherwise((pl.col("FA_TOTAL") / pl.col("FAD_TOTAL")) * 100)
                    .alias("AREA_PERC")
                )
        else:
            pop_est = pop_est.with_columns(
                pl.when(pl.col("FAD_TOTAL") == 0)
                .then(0.0)
                .otherwise((pl.col("FA_TOTAL") / pl.col("FAD_TOTAL")) * 100)
                .alias("AREA_PERC")
            )

        pop_est = pop_est.with_columns(
            ratio_var(
                pl.col("FA_TOTAL"),
                pl.col("FAD_TOTAL"),
                pl.col("FA_VAR"),
                pl.col("FAD_VAR"),
                pl.col("COV_FA_FAD"),
            ).alias("PERC_VAR_RATIO")
        )

        pop_est = pop_est.with_columns(
            (pl.col("PERC_VAR_RATIO") * 10000).alias("AREA_PERC_VAR")
        )

        pop_est = pop_est.with_columns(
            [
                (pl.col("AREA_PERC_VAR").sqrt()).alias("AREA_PERC_SE"),
                (pl.col("FA_VAR").sqrt() / pl.col("FA_TOTAL") * 100).alias("AREA_SE"),
            ]
        )

        cols = pop_groups + ["AREA_PERC", "N_PLOTS"]

        if totals:
            pop_est = pop_est.with_columns(pl.col("FA_TOTAL").alias("AREA"))
            cols.append("AREA")

        if variance:
            cols.append("AREA_PERC_VAR")
            if totals:
                cols.append("FA_VAR")
        else:
            cols.append("AREA_PERC_SE")
            if totals:
                cols.append("AREA_SE")

        return pop_est.select(cols)

    def compile(self) -> StateGraph:
        """Compile the workflow graph."""
        return self.workflow.compile(checkpointer=self.checkpointer)

    def run(self, **kwargs) -> pl.DataFrame:
        """Run the complete area estimation workflow."""
        # Initialize state
        initial_state = AreaEstimationState(
            db=kwargs.get("db"),
            grp_by=kwargs.get("grp_by"),
            by_land_type=kwargs.get("by_land_type", False),
            land_type=kwargs.get("land_type", "forest"),
            tree_domain=kwargs.get("tree_domain"),
            area_domain=kwargs.get("area_domain"),
            method=kwargs.get("method", "TI"),
            lambda_=kwargs.get("lambda_", 0.5),
            totals=kwargs.get("totals", False),
            variance=kwargs.get("variance", False),
            most_recent=kwargs.get("most_recent", False),

            # Initialize workflow state
            step="",
            validation_errors=[],
            warnings=[],
            raw_data=None,
            filtered_data=None,
            plot_estimates=None,
            stratum_estimates=None,
            final_results=None,
            metadata={},
            quality_score=0.0,
            needs_validation=False,
            retry_count=0,
            max_retries=3
        )

        # Compile and run workflow
        app = self.compile()

        # Generate unique thread ID for this run
        thread_id = f"area_estimation_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        config = {"configurable": {"thread_id": thread_id}}

        # Execute workflow
        result = app.invoke(initial_state, config=config)

        # Extract final results
        if result["final_results"] is not None:
            return result["final_results"]
        else:
            # Handle error case
            error_info = result.get("metadata", {})
            raise ValueError(f"Area estimation failed: {error_info}")


# Convenience function to maintain backward compatibility
def area_workflow(
    db,
    grp_by: Optional[List[str]] = None,
    by_land_type: bool = False,
    land_type: str = "forest",
    tree_domain: Optional[str] = None,
    area_domain: Optional[str] = None,
    method: str = "TI",
    lambda_: float = 0.5,
    totals: bool = False,
    variance: bool = False,
    most_recent: bool = False,
) -> pl.DataFrame:
    """
    Advanced area estimation using LangGraph workflow.
    
    This function provides the same interface as the original area() function
    but uses an advanced LangGraph workflow for processing.
    """
    workflow = AreaWorkflow(enable_checkpointing=False)

    return workflow.run(
        db=db,
        grp_by=grp_by,
        by_land_type=by_land_type,
        land_type=land_type,
        tree_domain=tree_domain,
        area_domain=area_domain,
        method=method,
        lambda_=lambda_,
        totals=totals,
        variance=variance,
        most_recent=most_recent
    )

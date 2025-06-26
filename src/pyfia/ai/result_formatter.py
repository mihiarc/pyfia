"""
Enhanced result formatting utilities for FIA analysis results.

This module provides comprehensive formatting functions for presenting
FIA estimation results with proper statistical context, confidence intervals,
and user-friendly explanations using Rich terminal formatting.
"""

import math
from typing import Dict, Any, Optional, Union, List
from datetime import datetime

try:
    import polars as pl
except ImportError:
    pl = None

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.text import Text
    from rich.tree import Tree
    from rich.columns import Columns
    from rich.progress import Progress, track
    from rich.syntax import Syntax
    from rich.markdown import Markdown
    from rich.live import Live
    from rich import print as rprint
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False


class FIAResultFormatter:
    """
    Enhanced formatter for FIA analysis results with statistical context.
    
    Provides methods for formatting different types of FIA estimates
    (tree counts, volume, biomass, etc.) with proper statistical presentation
    including confidence intervals, reliability assessments, and methodology notes.
    """
    
    def __init__(
        self, 
        include_emojis: bool = True, 
        confidence_level: float = 0.95,
        use_rich: bool = True,
        console_width: Optional[int] = None
    ):
        """
        Initialize the result formatter.
        
        Args:
            include_emojis: Whether to include emoji indicators in output
            confidence_level: Confidence level for intervals (default 0.95 for 95% CI)
            use_rich: Whether to use Rich formatting (falls back to plain text if False or Rich unavailable)
            console_width: Console width for formatting
        """
        self.include_emojis = include_emojis
        self.confidence_level = confidence_level
        self.z_score = self._get_z_score(confidence_level)
        self.use_rich = use_rich and RICH_AVAILABLE
        
        # Initialize Rich console if available
        if self.use_rich:
            self.console = Console(width=console_width, force_terminal=True)
        else:
            self.console = None
        
    def _get_z_score(self, confidence_level: float) -> float:
        """Get z-score for confidence level."""
        if confidence_level == 0.95:
            return 1.96
        elif confidence_level == 0.90:
            return 1.645
        elif confidence_level == 0.99:
            return 2.576
        else:
            # Approximate for other levels
            return 1.96
    
    def _get_emoji(self, category: str, value: Optional[str] = None) -> str:
        """Get emoji for category if enabled."""
        if not self.include_emojis:
            return ""
            
        emoji_map = {
            "tree": "🌳",
            "chart": "📊", 
            "target": "🎯",
            "number": "🔢",
            "location": "📍",
            "microscope": "🔬",
            "bulb": "💡",
            "summary": "📋",
            "trend": "📈",
            "excellent": "🟢",
            "good": "🟡", 
            "fair": "🟠",
            "poor": "🔴"
        }
        
        return emoji_map.get(category, "")
    
    def _assess_reliability(self, se_percent: float) -> Dict[str, str]:
        """Assess reliability based on standard error percentage."""
        if se_percent <= 5:
            return {
                "level": "Excellent",
                "range": "≤5%",
                "emoji": self._get_emoji("excellent"),
                "interpretation": "Very reliable estimate"
            }
        elif se_percent <= 10:
            return {
                "level": "Good", 
                "range": "5-10%",
                "emoji": self._get_emoji("good"),
                "interpretation": "Reliable estimate"
            }
        elif se_percent <= 20:
            return {
                "level": "Fair",
                "range": "10-20%", 
                "emoji": self._get_emoji("fair"),
                "interpretation": "Moderately reliable estimate"
            }
        else:
            return {
                "level": "Poor",
                "range": ">20%",
                "emoji": self._get_emoji("poor"),
                "interpretation": "Use with caution"
            }
    
    def _calculate_confidence_interval(self, estimate: float, se: float) -> Dict[str, float]:
        """Calculate confidence interval."""
        margin_error = self.z_score * se
        return {
            "lower": max(0, estimate - margin_error),
            "upper": estimate + margin_error,
            "margin_error": margin_error
        }
    
    def _create_rich_table(self, data: List[Dict[str, Any]], title: str = "Results", analysis_type: str = "tree") -> Table:
        """Create a Rich table from data."""
        if not self.use_rich:
            return None
            
        table = Table(title=title, show_header=True, header_style="bold magenta")
        
        # Determine columns based on data
        if not data:
            return table
            
        first_row = data[0]
        
        if analysis_type == "tree":
            # Tree analysis table structure
            if 'COMMON_NAME' in first_row:
                table.add_column("Species", style="cyan", no_wrap=True)
            if 'SIZE_CLASS' in first_row:
                table.add_column("Size Class", style="blue")
            
            table.add_column("Population", justify="right", style="green")
            table.add_column("Standard Error", justify="right", style="yellow")
            table.add_column("95% CI Lower", justify="right", style="dim")
            table.add_column("95% CI Upper", justify="right", style="dim")
            table.add_column("Reliability", justify="center", style="bold")
            
            # Add tree rows
            for row in data:
                table_row = []
                
                if 'COMMON_NAME' in first_row:
                    species_name = row.get('COMMON_NAME', 'Unknown')
                    if 'SCIENTIFIC_NAME' in row and row['SCIENTIFIC_NAME']:
                        species_name += f"\n[dim]({row['SCIENTIFIC_NAME']})[/dim]"
                    table_row.append(species_name)
                    
                if 'SIZE_CLASS' in first_row:
                    table_row.append(str(row.get('SIZE_CLASS', 'All')))
                
                # Population estimate
                tree_count = row.get('TREE_COUNT', 0)
                table_row.append(f"{tree_count:,.0f}")
                
                # Standard error
                se = row.get('SE', 0)
                se_percent = row.get('SE_PERCENT', (se / tree_count * 100) if tree_count > 0 else 0)
                table_row.append(f"±{se:,.0f}\n[dim]({se_percent:.1f}%)[/dim]")
                
                # Confidence interval
                ci = self._calculate_confidence_interval(tree_count, se)
                table_row.append(f"{ci['lower']:,.0f}")
                table_row.append(f"{ci['upper']:,.0f}")
                
                # Reliability
                reliability = self._assess_reliability(se_percent)
                table_row.append(f"{reliability['emoji']} {reliability['level']}")
                
                table.add_row(*table_row)
        
        else:  # area analysis
            # Area analysis table structure
            if 'LAND_TYPE' in first_row:
                table.add_column("Land Type", style="cyan", no_wrap=True)
            
            # Add grouping columns dynamically
            for col in first_row.keys():
                if col not in ['AREA_PERC', 'AREA', 'AREA_SE', 'AREA_PERC_SE', 'N_PLOTS', 'LAND_TYPE', 'QUALITY_SCORE', 'WORKFLOW_VERSION', 'PROCESSING_TIMESTAMP']:
                    if first_row[col] is not None:
                        table.add_column(col.replace('_', ' ').title(), style="blue")
            
            table.add_column("Area (acres)", justify="right", style="green")
            table.add_column("Area %", justify="right", style="cyan")
            table.add_column("Standard Error", justify="right", style="yellow")
            table.add_column("Sample Plots", justify="right", style="dim")
            table.add_column("Reliability", justify="center", style="bold")
            
            # Add area rows
            for row in data:
                table_row = []
                
                if 'LAND_TYPE' in first_row:
                    table_row.append(str(row.get('LAND_TYPE', 'All')))
                
                # Add grouping values
                for col in first_row.keys():
                    if col not in ['AREA_PERC', 'AREA', 'AREA_SE', 'AREA_PERC_SE', 'N_PLOTS', 'LAND_TYPE', 'QUALITY_SCORE', 'WORKFLOW_VERSION', 'PROCESSING_TIMESTAMP']:
                        if first_row[col] is not None:
                            table_row.append(str(row.get(col, '-')))
                
                # Area estimate
                area = row.get('AREA', 0)
                table_row.append(f"{area:,.0f}")
                
                # Area percentage
                area_perc = row.get('AREA_PERC', 0)
                table_row.append(f"{area_perc:.2f}%")
                
                # Standard error
                area_se = row.get('AREA_SE', 0)
                se_percent = (area_se / area * 100) if area > 0 and area_se else 0
                if area_se:
                    table_row.append(f"±{area_se:,.0f}\n[dim]({se_percent:.1f}%)[/dim]")
                else:
                    table_row.append("-")
                
                # Sample plots
                n_plots = row.get('N_PLOTS', 0)
                table_row.append(f"{n_plots:,}" if n_plots else "-")
                
                # Reliability (based on SE percentage)
                if se_percent > 0:
                    reliability = self._assess_reliability(se_percent)
                    table_row.append(f"{reliability['emoji']} {reliability['level']}")
                else:
                    table_row.append("-")
                
                table.add_row(*table_row)
        
        return table
    
    def _create_summary_panel(self, query_params: Dict[str, Any], evalid_info: Optional[Dict[str, Any]] = None, analysis_type: str = "tree", calculation_method: Optional[str] = None) -> Panel:
        """Create a Rich panel with query summary."""
        if not self.use_rich:
            return None
            
        summary_lines = []
        
        # Query parameters based on analysis type
        if analysis_type == "tree":
            tree_type = query_params.get('tree_type', 'live').title()
            summary_lines.append(f"[bold cyan]Tree Status:[/bold cyan] {tree_type} trees")
        
        land_type = query_params.get('land_type', 'forest').title()
        summary_lines.append(f"[bold cyan]Land Type:[/bold cyan] {land_type} land")
        
        if query_params.get('tree_domain'):
            summary_lines.append(f"[bold cyan]Tree Filter:[/bold cyan] {query_params['tree_domain']}")
        if query_params.get('area_domain'):
            summary_lines.append(f"[bold cyan]Area Filter:[/bold cyan] {query_params['area_domain']}")
        
        # Analysis-specific parameters
        if analysis_type == "area":
            if query_params.get('by_land_type'):
                summary_lines.append(f"[bold cyan]Grouping:[/bold cyan] By land type")
            if query_params.get('totals'):
                summary_lines.append(f"[bold cyan]Output:[/bold cyan] Total estimates")
            if query_params.get('variance'):
                summary_lines.append(f"[bold cyan]Statistics:[/bold cyan] With variance estimates")
            if calculation_method:
                summary_lines.append(f"[bold cyan]Method:[/bold cyan] {calculation_method}")
        
        # EVALID context
        if evalid_info:
            summary_lines.append(f"[bold cyan]Evaluation ID:[/bold cyan] {evalid_info.get('evalid', 'Unknown')}")
            if evalid_info.get('description'):
                summary_lines.append(f"[bold cyan]Evaluation:[/bold cyan] {evalid_info['description']}")
        
        summary_lines.append(f"[bold cyan]Analysis Date:[/bold cyan] {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        
        summary_text = "\n".join(summary_lines)
        
        # Choose title and icon based on analysis type
        if analysis_type == "area":
            title = "🌍 Area Analysis Summary"
        else:
            title = "🌳 Query Summary"
        
        return Panel(
            summary_text,
            title=title,
            title_align="left",
            border_style="green",
            padding=(1, 2)
        )
    
    def _create_methodology_panel(self, analysis_type: str = "tree") -> Panel:
        """Create a Rich panel with methodology information."""
        if not self.use_rich:
            return None
        
        if analysis_type == "tree":
            methodology_text = """[bold]Statistical Methods:[/bold]
• Population estimates use FIA EVALIDator methodology
• Expansion factors account for plot sampling design  
• Standard errors reflect sampling uncertainty
• {ci_level}% confidence intervals assume normal distribution

[bold]Interpretation Guide:[/bold]
• Population estimates represent total trees across the area
• Standard errors indicate precision of estimates
• Lower SE% = more precise estimate
• Confidence intervals show plausible range of true values
• Reliability ratings help assess estimate quality

[bold]Reliability Scale:[/bold]
🟢 Excellent (≤5% SE) - Very reliable estimate
🟡 Good (5-10% SE) - Reliable estimate  
🟠 Fair (10-20% SE) - Moderately reliable estimate
🔴 Poor (>20% SE) - Use with caution""".format(
                ci_level=int(self.confidence_level * 100)
            )
        else:  # area analysis
            methodology_text = """[bold]Statistical Methods:[/bold]
• Area estimates use FIA EVALIDator methodology
• Plot-level expansion factors scale to population level
• Standard errors account for sampling design complexity
• {ci_level}% confidence intervals assume normal distribution

[bold]Interpretation Guide:[/bold]
• Area estimates represent total land area across the region
• Percentages show proportion of total area by category
• Standard errors indicate precision of area estimates
• Lower SE% = more precise estimate
• Sample plots provide the basis for population inference

[bold]Reliability Scale:[/bold]
🟢 Excellent (≤5% SE) - Very reliable estimate
🟡 Good (5-10% SE) - Reliable estimate  
🟠 Fair (10-20% SE) - Moderately reliable estimate
🔴 Poor (>20% SE) - Use with caution

[bold]Area Analysis Notes:[/bold]
• Land type classifications follow FIA standards
• Forest area includes all forest land regardless of ownership
• Timber area is subset of forest land available for harvest
• Estimates may include confidence intervals when available""".format(
                ci_level=int(self.confidence_level * 100)
            )
        
        return Panel(
            methodology_text,
            title="🔬 Methodology & Interpretation",
            title_align="left", 
            border_style="blue",
            padding=(1, 2)
        )
    
    def format_tree_count_results(
        self, 
        result: 'pl.DataFrame', 
        query_params: Dict[str, Any],
        evalid_info: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Format tree count results with enhanced presentation using Rich.
        
        Args:
            result: Polars DataFrame with tree count results
            query_params: Original query parameters for context
            evalid_info: Optional EVALID information for context
            
        Returns:
            Formatted string with comprehensive result presentation
        """
        if self.use_rich:
            return self._format_with_rich(result, query_params, evalid_info, "tree")
        else:
            return self._format_plain_text(result, query_params, evalid_info, "tree")
    
    def format_area_estimation_results(
        self, 
        result: 'pl.DataFrame', 
        query_params: Dict[str, Any],
        evalid_info: Optional[Dict[str, Any]] = None,
        calculation_method: str = "Core Function"
    ) -> str:
        """
        Format area estimation results with enhanced presentation using Rich.
        
        Args:
            result: Polars DataFrame with area estimation results
            query_params: Original query parameters for context
            evalid_info: Optional EVALID information for context
            calculation_method: Which calculation method was used
            
        Returns:
            Formatted string with comprehensive result presentation
        """
        if self.use_rich:
            return self._format_with_rich(result, query_params, evalid_info, "area", calculation_method)
        else:
            return self._format_plain_text(result, query_params, evalid_info, "area", calculation_method)
    
    def format_mortality_estimation_results(
        self, 
        result: 'pl.DataFrame', 
        query_params: Dict[str, Any],
        evalid_info: Optional[Dict[str, Any]] = None,
        calculation_method: str = "Core Function"
    ) -> str:
        """
        Format mortality estimation results with enhanced presentation.
        
        Args:
            result: Polars DataFrame with mortality estimation results
            query_params: Original query parameters for context
            evalid_info: Optional EVALID information for context
            calculation_method: Which calculation method was used
            
        Returns:
            Formatted string with comprehensive result presentation
        """
        if self.use_rich:
            return self._format_with_rich(result, query_params, evalid_info, "mortality", calculation_method)
        else:
            return self._format_plain_text(result, query_params, evalid_info, "mortality", calculation_method)
    
    def _format_with_rich(
        self, 
        result: 'pl.DataFrame', 
        query_params: Dict[str, Any],
        evalid_info: Optional[Dict[str, Any]] = None,
        analysis_type: str = "tree",
        calculation_method: Optional[str] = None
    ) -> str:
        """Format results using Rich components."""
        # Convert DataFrame to list of dicts for easier processing
        data = [dict(row) for row in result.iter_rows(named=True)]
        
        # Create components
        summary_panel = self._create_summary_panel(query_params, evalid_info, analysis_type, calculation_method)
        
        # Determine if we need a table or single result display
        if analysis_type == "tree":
            is_grouped = any(col in result.columns for col in ['COMMON_NAME', 'SIZE_CLASS'])
            title = "🌳 Tree Population Analysis"
        elif analysis_type == "mortality":
            is_grouped = any(col in result.columns for col in ['COMMON_NAME', 'SIZE_CLASS', 'TREE_CLASS'])
            title = "💀 Forest Mortality Analysis"
        else:  # area
            is_grouped = any(col in result.columns for col in ['LAND_TYPE', 'FORTYPCD', 'OWNGRPCD'])
            title = "🌍 Area Estimation Analysis"
        
        if is_grouped and len(data) > 1:
            # Create Rich table for grouped results
            results_table = self._create_rich_table(data, title, analysis_type)
        else:
            # Create Rich panel for single result
            results_table = self._create_single_result_panel(data[0] if data else {}, analysis_type)
        
        methodology_panel = self._create_methodology_panel(analysis_type)
        
        # Render all components to string
        output_parts = []
        
        if summary_panel:
            with self.console.capture() as capture:
                self.console.print(summary_panel)
            output_parts.append(capture.get())
        
        if results_table:
            with self.console.capture() as capture:
                self.console.print(results_table)
            output_parts.append(capture.get())
        
        if methodology_panel:
            with self.console.capture() as capture:
                self.console.print(methodology_panel)
            output_parts.append(capture.get())
        
        return "\n".join(output_parts)
    
    def _create_single_result_panel(self, data: Dict[str, Any], analysis_type: str = "tree") -> Panel:
        """Create a Rich panel for single result display."""
        if not self.use_rich or not data:
            return None
        
        if analysis_type == "tree":
            tree_count = data.get('TREE_COUNT', 0)
            se = data.get('SE', 0)
            se_percent = data.get('SE_PERCENT', (se / tree_count * 100) if tree_count > 0 else 0)
            nplots = data.get('nPlots', 0)
            
            # Calculate statistics
            ci = self._calculate_confidence_interval(tree_count, se)
            reliability = self._assess_reliability(se_percent)
            avg_trees_per_plot = tree_count / nplots if nplots > 0 else 0
            
            result_text = f"""[bold green]Population Estimate:[/bold green] {tree_count:,.0f} trees

[bold yellow]Statistical Precision:[/bold yellow]
• Standard Error: ±{se:,.0f} trees ({se_percent:.1f}%)
• {int(self.confidence_level * 100)}% Confidence Interval: {ci['lower']:,.0f} - {ci['upper']:,.0f}
• Reliability: {reliability['emoji']} {reliability['level']} ({reliability['interpretation']})

[bold cyan]Sample Information:[/bold cyan]
• Field Plots Used: {nplots:,.0f}
• Average Trees per Plot: {avg_trees_per_plot:,.1f}"""
            
            title = "📊 Population Analysis Results"
        
        elif analysis_type == "mortality":
            # Mortality analysis panel
            mort_tpa = data.get('MORT_TPA_AC', data.get('MORT_TPA_TOTAL', 0))
            mort_vol = data.get('MORT_VOL_AC', data.get('MORT_VOL_TOTAL', 0))
            mort_bio = data.get('MORT_BIO_AC', data.get('MORT_BIO_TOTAL', 0))
            
            se_tpa = data.get('MORT_TPA_SE', 0)
            cv_tpa = data.get('MORT_TPA_CV', 0)
            nplots = data.get('nPlots', 0)
            forest_area = data.get('AREA_TOTAL', 0)
            
            # Determine units
            is_per_acre = 'MORT_TPA_AC' in data
            unit_suffix = "/acre/year" if is_per_acre else "/year"
            
            result_text = f"""[bold red]Annual Mortality Estimates:[/bold red]
• Trees: {mort_tpa:,.3f} trees{unit_suffix}"""
            
            if mort_vol > 0:
                vol_unit = "cu.ft." + unit_suffix
                result_text += f"\n• Volume: {mort_vol:,.2f} {vol_unit}"
            
            if mort_bio > 0:
                bio_unit = "tons" + unit_suffix
                result_text += f"\n• Biomass: {mort_bio:,.3f} {bio_unit}"
            
            if se_tpa > 0 and cv_tpa > 0:
                ci = self._calculate_confidence_interval(mort_tpa, se_tpa)
                reliability = self._assess_reliability(cv_tpa)
                
                result_text += f"""

[bold yellow]Statistical Precision:[/bold yellow]
• Standard Error: ±{se_tpa:,.3f} trees{unit_suffix} ({cv_tpa:.1f}%)
• {int(self.confidence_level * 100)}% Confidence Interval: {ci['lower']:,.3f} - {ci['upper']:,.3f}
• Reliability: {reliability['emoji']} {reliability['level']} ({reliability['interpretation']})"""
            
            if nplots > 0:
                result_text += f"""

[bold cyan]Sample Information:[/bold cyan]
• Field Plots Used: {nplots:,}"""
                
                if forest_area > 0:
                    result_text += f"\n• Forest Area: {forest_area:,.0f} acres"
                    total_mortality = mort_tpa * forest_area if is_per_acre else mort_tpa
                    result_text += f"\n• Total Annual Mortality: {total_mortality:,.0f} trees/year"
            
            title = "💀 Annual Mortality Analysis"
            
        else:  # area analysis
            area = data.get('AREA', 0)
            area_perc = data.get('AREA_PERC', 0)
            area_se = data.get('AREA_SE', 0)
            se_percent = (area_se / area * 100) if area > 0 and area_se else 0
            nplots = data.get('N_PLOTS', 0)
            
            result_text = f"""[bold green]Area Estimate:[/bold green] {area:,.0f} acres"""
            
            if area_perc > 0:
                result_text += f" ({area_perc:.2f}% of total)"
            
            if area_se > 0:
                # Calculate statistics
                ci = self._calculate_confidence_interval(area, area_se)
                reliability = self._assess_reliability(se_percent)
                
                result_text += f"""

[bold yellow]Statistical Precision:[/bold yellow]
• Standard Error: ±{area_se:,.0f} acres ({se_percent:.1f}%)
• {int(self.confidence_level * 100)}% Confidence Interval: {ci['lower']:,.0f} - {ci['upper']:,.0f}
• Reliability: {reliability['emoji']} {reliability['level']} ({reliability['interpretation']})"""
            
            if nplots > 0:
                avg_area_per_plot = area / nplots if nplots > 0 else 0
                result_text += f"""

[bold cyan]Sample Information:[/bold cyan]
• Field Plots Used: {nplots:,.0f}
• Average Area per Plot: {avg_area_per_plot:,.1f} acres"""
            
            # Add workflow metadata if available
            if data.get('QUALITY_SCORE'):
                result_text += f"\n• Quality Score: {data['QUALITY_SCORE']:.3f}"
            if data.get('WORKFLOW_VERSION'):
                result_text += f"\n• Workflow: {data['WORKFLOW_VERSION']}"
            
            title = "🌍 Area Analysis Results"
        
        return Panel(
            result_text,
            title=title,
            title_align="left",
            border_style="magenta",
            padding=(1, 2)
        )
    
    def _format_plain_text(
        self, 
        result: 'pl.DataFrame', 
        query_params: Dict[str, Any],
        evalid_info: Optional[Dict[str, Any]] = None,
        analysis_type: str = "tree",
        calculation_method: Optional[str] = None
    ) -> str:
        """Fallback to plain text formatting when Rich is not available."""
        # Header with query context
        if analysis_type == "area":
            header_emoji = self._get_emoji("chart")
            formatted = f"{header_emoji} **FIA Area Estimation Results**\n"
        elif analysis_type == "mortality":
            skull_emoji = self._get_emoji("warning")  # Use warning emoji for mortality
            formatted = f"{skull_emoji} **FIA Mortality Analysis Results**\n"
        else:
            tree_emoji = self._get_emoji("tree")
            formatted = f"{tree_emoji} **FIA Tree Count Analysis Results**\n"
        formatted += "=" * 50 + "\n\n"
        
        # Query context section
        chart_emoji = self._get_emoji("chart")
        formatted += f"{chart_emoji} **Query Summary:**\n"
        
        if analysis_type == "tree":
            tree_type = query_params.get('tree_type', 'live').title()
            formatted += f"   • Tree Status: {tree_type} trees\n"
        
        land_type = query_params.get('land_type', 'forest').title()
        formatted += f"   • Land Type: {land_type} land\n"
        
        if query_params.get('tree_domain'):
            formatted += f"   • Tree Filter: {query_params['tree_domain']}\n"
        if query_params.get('area_domain'):
            formatted += f"   • Area Filter: {query_params['area_domain']}\n"
        
        # Analysis-specific parameters
        if analysis_type == "area":
            if query_params.get('by_land_type'):
                formatted += f"   • Grouping: By land type\n"
            if query_params.get('totals'):
                formatted += f"   • Output: Total estimates\n"
            if query_params.get('variance'):
                formatted += f"   • Statistics: With variance estimates\n"
            if calculation_method:
                formatted += f"   • Method: {calculation_method}\n"
        
        # Add EVALID context
        if evalid_info:
            formatted += f"   • Evaluation ID: {evalid_info.get('evalid', 'Unknown')}\n"
            if evalid_info.get('description'):
                formatted += f"   • Evaluation: {evalid_info['description']}\n"
        
        formatted += f"   • Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
        
        # Results section
        if analysis_type == "tree":
            is_grouped = any(col in result.columns for col in ['COMMON_NAME', 'SIZE_CLASS'])
            
            if is_grouped:
                formatted += self._format_grouped_results(result, analysis_type)
            else:
                formatted += self._format_single_result(result, analysis_type)
        elif analysis_type == "mortality":
            is_grouped = any(col in result.columns for col in ['COMMON_NAME', 'SIZE_CLASS', 'TREE_CLASS'])
            
            if is_grouped:
                formatted += self._format_grouped_results(result, analysis_type)
            else:
                formatted += self._format_single_result(result, analysis_type)
        else:  # area
            is_grouped = any(col in result.columns for col in ['LAND_TYPE', 'FORTYPCD', 'OWNGRPCD'])
            
            if is_grouped:
                formatted += self._format_grouped_results(result, analysis_type)
            else:
                formatted += self._format_single_result(result, analysis_type)
        
        # Add methodology and interpretation
        formatted += self._add_methodology_footer(analysis_type)
        
        return formatted
    
    def _format_grouped_results(self, result: 'pl.DataFrame', analysis_type: str = "tree") -> str:
        """Format grouped results (by species, size class, etc.)."""
        trend_emoji = self._get_emoji("trend")
        formatted = f"{trend_emoji} **Detailed Results:**\n\n"
        
        if analysis_type == "tree":
            total_trees = 0
            total_plots = 0
            
            for i, row in enumerate(result.iter_rows(named=True)):
                # Species information
                if 'COMMON_NAME' in row and row['COMMON_NAME']:
                    formatted += f"**{i+1}. {row['COMMON_NAME']}**"
                    if 'SCIENTIFIC_NAME' in row and row['SCIENTIFIC_NAME']:
                        formatted += f" (*{row['SCIENTIFIC_NAME']}*)"
                    formatted += "\n"
                    
                    if 'SPCD' in row and row['SPCD']:
                        formatted += f"   Species Code: {row['SPCD']}\n"
                
                # Size class information
                if 'SIZE_CLASS' in row and row['SIZE_CLASS']:
                    formatted += f"**Size Class: {row['SIZE_CLASS']}**\n"
                
                # Main estimate with statistics
                if 'TREE_COUNT' in row:
                    tree_count = row['TREE_COUNT']
                    total_trees += tree_count
                    
                    number_emoji = self._get_emoji("number")
                    formatted += f"   {number_emoji} **Population Estimate:** {tree_count:,.0f} trees\n"
                    
                    # Statistical precision
                    if 'SE' in row and row['SE'] and not math.isnan(row['SE']):
                        se = row['SE']
                        se_percent = row.get('SE_PERCENT', (se / tree_count * 100) if tree_count > 0 else 0)
                        
                        # Confidence interval
                        ci = self._calculate_confidence_interval(tree_count, se)
                        
                        chart_emoji = self._get_emoji("chart")
                        target_emoji = self._get_emoji("target")
                        formatted += f"   {chart_emoji} **Standard Error:** ±{se:,.0f} trees ({se_percent:.1f}%)\n"
                        formatted += f"   {target_emoji} **{int(self.confidence_level*100)}% Confidence Interval:** {ci['lower']:,.0f} - {ci['upper']:,.0f} trees\n"
                        
                        # Reliability assessment
                        reliability = self._assess_reliability(se_percent)
                        formatted += f"   {reliability['emoji']} **Reliability:** {reliability['level']} ({reliability['range']})\n"
                
                # Sample size
                if 'nPlots' in row and row['nPlots']:
                    plots = row['nPlots']
                    total_plots = max(total_plots, plots)
                    location_emoji = self._get_emoji("location")
                    formatted += f"   {location_emoji} **Sample Size:** {plots:,} plots\n"
                
                formatted += "\n"
            
            # Summary statistics for grouped results
            if len(result) > 1:
                summary_emoji = self._get_emoji("summary")
                formatted += f"{summary_emoji} **Summary Statistics:**\n"
                formatted += f"   • Total Entries: {len(result):,}\n"
                formatted += f"   • Combined Population: {total_trees:,.0f} trees\n"
                if total_plots > 0:
                    formatted += f"   • Plot Sample Size: {total_plots:,} plots\n"
                formatted += "\n"
        
        else:  # area analysis
            total_area = 0
            total_plots = 0
            
            for i, row in enumerate(result.iter_rows(named=True)):
                # Land type information
                if 'LAND_TYPE' in row and row['LAND_TYPE']:
                    formatted += f"**{i+1}. {row['LAND_TYPE']} Land**\n"
                
                # Main estimate with statistics
                if 'AREA' in row:
                    area = row['AREA']
                    total_area += area
                    
                    number_emoji = self._get_emoji("number")
                    formatted += f"   {number_emoji} **Area Estimate:** {area:,.0f} acres"
                    
                    if 'AREA_PERC' in row and row['AREA_PERC']:
                        formatted += f" ({row['AREA_PERC']:.2f}% of total)"
                    formatted += "\n"
                    
                    # Statistical precision
                    if 'AREA_SE' in row and row['AREA_SE'] and row['AREA_SE'] > 0:
                        area_se = row['AREA_SE']
                        se_percent = (area_se / area * 100) if area > 0 else 0
                        
                        # Confidence interval
                        ci = self._calculate_confidence_interval(area, area_se)
                        
                        chart_emoji = self._get_emoji("chart")
                        target_emoji = self._get_emoji("target")
                        formatted += f"   {chart_emoji} **Standard Error:** ±{area_se:,.0f} acres ({se_percent:.1f}%)\n"
                        formatted += f"   {target_emoji} **{int(self.confidence_level*100)}% Confidence Interval:** {ci['lower']:,.0f} - {ci['upper']:,.0f} acres\n"
                        
                        # Reliability assessment
                        reliability = self._assess_reliability(se_percent)
                        formatted += f"   {reliability['emoji']} **Reliability:** {reliability['level']} ({reliability['range']})\n"
                
                # Sample size
                if 'N_PLOTS' in row and row['N_PLOTS']:
                    plots = row['N_PLOTS']
                    total_plots = max(total_plots, plots)
                    location_emoji = self._get_emoji("location")
                    formatted += f"   {location_emoji} **Sample Size:** {plots:,} plots\n"
                
                formatted += "\n"
            
            # Summary statistics for grouped results
            if len(result) > 1:
                summary_emoji = self._get_emoji("summary")
                formatted += f"{summary_emoji} **Summary Statistics:**\n"
                formatted += f"   • Total Categories: {len(result):,}\n"
                formatted += f"   • Combined Area: {total_area:,.0f} acres\n"
                if total_plots > 0:
                    formatted += f"   • Plot Sample Size: {total_plots:,} plots\n"
                formatted += "\n"
        
        elif analysis_type == "mortality":
            total_mort_tpa = 0
            total_mort_vol = 0  
            total_mort_bio = 0
            total_plots = 0
            
            for i, row in enumerate(result.iter_rows(named=True)):
                # Tree class or species information
                if 'TREE_CLASS' in row and row['TREE_CLASS']:
                    formatted += f"**{i+1}. {row['TREE_CLASS']} Trees**\n"
                elif 'COMMON_NAME' in row and row['COMMON_NAME']:
                    formatted += f"**{i+1}. {row['COMMON_NAME']}**\n"
                    if 'SCIENTIFIC_NAME' in row and row['SCIENTIFIC_NAME']:
                        formatted += f"   *{row['SCIENTIFIC_NAME']}*\n"
                else:
                    formatted += f"**{i+1}. Annual Mortality**\n"
                
                # Main mortality estimates
                mort_tpa = row.get('MORT_TPA_AC', row.get('MORT_TPA_TOTAL', 0))
                mort_vol = row.get('MORT_VOL_AC', row.get('MORT_VOL_TOTAL', 0))
                mort_bio = row.get('MORT_BIO_AC', row.get('MORT_BIO_TOTAL', 0))
                
                total_mort_tpa += mort_tpa
                total_mort_vol += mort_vol
                total_mort_bio += mort_bio
                
                # Determine units
                is_per_acre = 'MORT_TPA_AC' in row
                unit_suffix = "/acre/year" if is_per_acre else "/year"
                
                number_emoji = self._get_emoji("number")
                formatted += f"   {number_emoji} **Tree Mortality:** {mort_tpa:,.3f} trees{unit_suffix}\n"
                
                if mort_vol > 0:
                    formatted += f"   {number_emoji} **Volume Mortality:** {mort_vol:,.2f} cu.ft.{unit_suffix}\n"
                
                if mort_bio > 0:
                    formatted += f"   {number_emoji} **Biomass Mortality:** {mort_bio:,.3f} tons{unit_suffix}\n"
                
                # Statistical precision for trees
                se_tpa = row.get('MORT_TPA_SE', 0)
                cv_tpa = row.get('MORT_TPA_CV', 0)
                
                if se_tpa > 0 and cv_tpa > 0:
                    ci = self._calculate_confidence_interval(mort_tpa, se_tpa)
                    
                    chart_emoji = self._get_emoji("chart")
                    target_emoji = self._get_emoji("target")
                    formatted += f"   {chart_emoji} **Standard Error:** ±{se_tpa:,.3f} trees{unit_suffix} ({cv_tpa:.1f}%)\n"
                    formatted += f"   {target_emoji} **{int(self.confidence_level*100)}% Confidence Interval:** {ci['lower']:,.3f} - {ci['upper']:,.3f} trees{unit_suffix}\n"
                    
                    # Reliability assessment
                    reliability = self._assess_reliability(cv_tpa)
                    formatted += f"   {reliability['emoji']} **Reliability:** {reliability['level']} ({reliability['range']})\n"
                
                # Sample size
                if 'nPlots' in row and row['nPlots']:
                    plots = row['nPlots']
                    total_plots = max(total_plots, plots)
                    location_emoji = self._get_emoji("location")
                    formatted += f"   {location_emoji} **Sample Size:** {plots:,} plots\n"
                
                formatted += "\n"
            
            # Summary statistics for grouped mortality results
            if len(result) > 1:
                summary_emoji = self._get_emoji("summary")
                formatted += f"{summary_emoji} **Summary Statistics:**\n"
                formatted += f"   • Total Categories: {len(result):,}\n"
                formatted += f"   • Combined Tree Mortality: {total_mort_tpa:,.3f} trees/acre/year\n"
                if total_mort_vol > 0:
                    formatted += f"   • Combined Volume Mortality: {total_mort_vol:,.2f} cu.ft./acre/year\n"
                if total_mort_bio > 0:
                    formatted += f"   • Combined Biomass Mortality: {total_mort_bio:,.3f} tons/acre/year\n"
                if total_plots > 0:
                    formatted += f"   • Plot Sample Size: {total_plots:,} plots\n"
                formatted += "\n"
        
        return formatted
    
    def _format_single_result(self, result: 'pl.DataFrame', analysis_type: str = "tree") -> str:
        """Format single result (total estimate)."""
        row = result.row(0, named=True)
        
        trend_emoji = self._get_emoji("trend")
        
        if analysis_type == "tree":
            formatted = f"{trend_emoji} **Population Estimate:**\n\n"
            
            if 'TREE_COUNT' in row:
                tree_count = row['TREE_COUNT']
                number_emoji = self._get_emoji("number")
                formatted += f"{number_emoji} **Total Trees:** {tree_count:,.0f}\n\n"
                
                # Statistical precision
                if 'SE' in row and row['SE'] and not math.isnan(row['SE']):
                    se = row['SE']
                    se_percent = row.get('SE_PERCENT', (se / tree_count * 100) if tree_count > 0 else 0)
                    
                    # Confidence interval
                    ci = self._calculate_confidence_interval(tree_count, se)
                    
                    chart_emoji = self._get_emoji("chart")
                    formatted += f"{chart_emoji} **Statistical Precision:**\n"
                    formatted += f"   • Standard Error: ±{se:,.0f} trees ({se_percent:.1f}%)\n"
                    formatted += f"   • {int(self.confidence_level*100)}% Confidence Interval: {ci['lower']:,.0f} - {ci['upper']:,.0f}\n"
                    
                    # Reliability assessment
                    reliability = self._assess_reliability(se_percent)
                    formatted += f"   • Reliability: {reliability['emoji']} {reliability['level']} ({reliability['interpretation']})\n\n"
            
            # Sample information
            if 'nPlots' in row and row['nPlots']:
                plots = row['nPlots']
                location_emoji = self._get_emoji("location")
                formatted += f"{location_emoji} **Sample Information:**\n"
                formatted += f"   • Field Plots Used: {plots:,}\n"
                
                # Plot density context
                if 'TREE_COUNT' in row and row['TREE_COUNT'] and plots:
                    trees_per_plot = row['TREE_COUNT'] / plots
                    formatted += f"   • Average Trees per Plot: {trees_per_plot:,.1f}\n"
                formatted += "\n"
        
        elif analysis_type == "mortality":
            formatted = f"{trend_emoji} **Annual Mortality Analysis:**\n\n"
            
            # Main mortality estimates
            mort_tpa = row.get('MORT_TPA_AC', row.get('MORT_TPA_TOTAL', 0))
            mort_vol = row.get('MORT_VOL_AC', row.get('MORT_VOL_TOTAL', 0))
            mort_bio = row.get('MORT_BIO_AC', row.get('MORT_BIO_TOTAL', 0))
            
            # Determine units
            is_per_acre = 'MORT_TPA_AC' in row
            unit_suffix = "/acre/year" if is_per_acre else "/year"
            
            number_emoji = self._get_emoji("number")
            formatted += f"{number_emoji} **Tree Mortality:** {mort_tpa:,.3f} trees{unit_suffix}\n"
            
            if mort_vol > 0:
                formatted += f"{number_emoji} **Volume Mortality:** {mort_vol:,.2f} cu.ft.{unit_suffix}\n"
                
            if mort_bio > 0:
                formatted += f"{number_emoji} **Biomass Mortality:** {mort_bio:,.3f} tons{unit_suffix}\n"
            
            formatted += "\n"
            
            # Statistical precision for trees
            se_tpa = row.get('MORT_TPA_SE', 0)
            cv_tpa = row.get('MORT_TPA_CV', 0)
            
            if se_tpa > 0 and cv_tpa > 0:
                ci = self._calculate_confidence_interval(mort_tpa, se_tpa)
                
                chart_emoji = self._get_emoji("chart")
                formatted += f"{chart_emoji} **Statistical Precision:**\n"
                formatted += f"   • Standard Error: ±{se_tpa:,.3f} trees{unit_suffix} ({cv_tpa:.1f}%)\n"
                formatted += f"   • {int(self.confidence_level*100)}% Confidence Interval: {ci['lower']:,.3f} - {ci['upper']:,.3f}\n"
                
                # Reliability assessment
                reliability = self._assess_reliability(cv_tpa)
                formatted += f"   • Reliability: {reliability['emoji']} {reliability['level']} ({reliability['interpretation']})\n\n"
            
            # Sample information
            if 'nPlots' in row and row['nPlots']:
                plots = row['nPlots']
                location_emoji = self._get_emoji("location")
                formatted += f"{location_emoji} **Sample Information:**\n"
                formatted += f"   • Field Plots Used: {plots:,}\n"
                
                # Forest area and total mortality if available
                if 'AREA_TOTAL' in row and row['AREA_TOTAL']:
                    forest_area = row['AREA_TOTAL']
                    formatted += f"   • Forest Area: {forest_area:,.0f} acres\n"
                    
                    if is_per_acre:
                        total_mortality = mort_tpa * forest_area
                        formatted += f"   • Total Annual Mortality: {total_mortality:,.0f} trees/year\n"
                
                formatted += "\n"
        
        else:  # area analysis
            formatted = f"{trend_emoji} **Area Estimate:**\n\n"
            
            if 'AREA' in row:
                area = row['AREA']
                number_emoji = self._get_emoji("number")
                formatted += f"{number_emoji} **Total Area:** {area:,.0f} acres"
                
                if 'AREA_PERC' in row and row['AREA_PERC']:
                    formatted += f" ({row['AREA_PERC']:.2f}% of total)"
                formatted += "\n\n"
                
                # Statistical precision
                if 'AREA_SE' in row and row['AREA_SE'] and row['AREA_SE'] > 0:
                    area_se = row['AREA_SE']
                    se_percent = (area_se / area * 100) if area > 0 else 0
                    
                    # Confidence interval
                    ci = self._calculate_confidence_interval(area, area_se)
                    
                    chart_emoji = self._get_emoji("chart")
                    formatted += f"{chart_emoji} **Statistical Precision:**\n"
                    formatted += f"   • Standard Error: ±{area_se:,.0f} acres ({se_percent:.1f}%)\n"
                    formatted += f"   • {int(self.confidence_level*100)}% Confidence Interval: {ci['lower']:,.0f} - {ci['upper']:,.0f}\n"
                    
                    # Reliability assessment
                    reliability = self._assess_reliability(se_percent)
                    formatted += f"   • Reliability: {reliability['emoji']} {reliability['level']} ({reliability['interpretation']})\n\n"
            
            # Sample information
            if 'N_PLOTS' in row and row['N_PLOTS']:
                plots = row['N_PLOTS']
                location_emoji = self._get_emoji("location")
                formatted += f"{location_emoji} **Sample Information:**\n"
                formatted += f"   • Field Plots Used: {plots:,}\n"
                
                # Plot density context
                if 'AREA' in row and row['AREA'] and plots:
                    area_per_plot = row['AREA'] / plots
                    formatted += f"   • Average Area per Plot: {area_per_plot:,.1f} acres\n"
                formatted += "\n"
            
            # Workflow metadata if available
            if 'QUALITY_SCORE' in row and row['QUALITY_SCORE']:
                bulb_emoji = self._get_emoji("bulb")
                formatted += f"{bulb_emoji} **Quality Assessment:**\n"
                formatted += f"   • Quality Score: {row['QUALITY_SCORE']:.3f}\n"
                if 'WORKFLOW_VERSION' in row and row['WORKFLOW_VERSION']:
                    formatted += f"   • Workflow: {row['WORKFLOW_VERSION']}\n"
                formatted += "\n"
        
        return formatted
    
    def _add_methodology_footer(self, analysis_type: str = "tree") -> str:
        """Add methodology and interpretation guidance."""
        microscope_emoji = self._get_emoji("microscope")
        bulb_emoji = self._get_emoji("bulb")
        
        formatted = f"{microscope_emoji} **Methodology Notes:**\n"
        if analysis_type == "tree":
            formatted += "   • Population estimates use FIA EVALIDator methodology\n"
            formatted += "   • Expansion factors account for plot sampling design\n"
            formatted += "   • Standard errors reflect sampling uncertainty\n"
            formatted += f"   • {int(self.confidence_level*100)}% confidence intervals assume normal distribution\n"
            
            formatted += f"\n{bulb_emoji} **Interpretation Guide:**\n"
            formatted += "   • Population estimates represent total trees across the area\n"
            formatted += "   • Standard errors indicate precision of estimates\n"
            formatted += "   • Lower SE% = more precise estimate\n"
            formatted += "   • Confidence intervals show plausible range of true values\n"
            formatted += "   • Reliability ratings help assess estimate quality\n"
        elif analysis_type == "mortality":
            formatted += "   • Mortality estimates use FIA GRM (Growth/Removal/Mortality) methodology\n"
            formatted += "   • Annual rates calculated from remeasurement periods\n"
            formatted += "   • Values represent trees dying per year\n"
            formatted += "   • Standard errors reflect sampling uncertainty\n"
            formatted += f"   • {int(self.confidence_level*100)}% confidence intervals assume normal distribution\n"
            
            formatted += f"\n{bulb_emoji} **Interpretation Guide:**\n"
            formatted += "   • Mortality rates show annual tree loss per unit area\n"
            formatted += "   • Volume mortality represents merchantable wood loss\n"
            formatted += "   • Biomass mortality includes above-ground dry weight\n"
            formatted += "   • Growing stock focuses on merchantable timber\n"
            formatted += "   • Rates are annualized from multi-year remeasurement periods\n"
        else:  # area analysis
            formatted += "   • Area estimates use FIA EVALIDator methodology\n"
            formatted += "   • Plot-level expansion factors scale to population level\n"
            formatted += "   • Standard errors account for sampling design complexity\n"
            formatted += f"   • {int(self.confidence_level*100)}% confidence intervals assume normal distribution\n"
            
            formatted += f"\n{bulb_emoji} **Interpretation Guide:**\n"
            formatted += "   • Area estimates represent total land area across the region\n"
            formatted += "   • Percentages show proportion of total area by category\n"
            formatted += "   • Standard errors indicate precision of area estimates\n"
            formatted += "   • Lower SE% = more precise estimate\n"
            formatted += "   • Sample plots provide the basis for population inference\n"
            
            formatted += f"\n{bulb_emoji} **Area Analysis Notes:**\n"
            formatted += "   • Land type classifications follow FIA standards\n"
            formatted += "   • Forest area includes all forest land regardless of ownership\n"
            formatted += "   • Timber area is subset of forest land available for harvest\n"
            formatted += "   • Estimates may include confidence intervals when available\n"
        
        return formatted
    
    def format_comparison_results(
        self,
        results: List[Dict[str, Any]],
        comparison_type: str = "temporal"
    ) -> str:
        """
        Format comparison results (temporal, spatial, etc.).
        
        Args:
            results: List of result dictionaries with estimates and metadata
            comparison_type: Type of comparison ("temporal", "spatial", "species")
            
        Returns:
            Formatted comparison analysis
        """
        # Implementation for comparison formatting
        # This would be used for queries like "Compare oak vs pine trees"
        # or "Show tree counts over time"
        pass
    
    def format_summary_statistics(self, result: 'pl.DataFrame') -> str:
        """
        Format summary statistics for numeric columns.
        
        Args:
            result: DataFrame with numeric results
            
        Returns:
            Formatted summary statistics
        """
        # Implementation for summary statistics
        # Mean, median, std dev, min, max for numeric columns
        pass


def create_result_formatter(
    style: str = "enhanced", 
    use_rich: bool = True,
    console_width: Optional[int] = None
) -> FIAResultFormatter:
    """
    Factory function to create result formatter with different styles.
    
    Args:
        style: Formatting style ("enhanced", "simple", "scientific")
        use_rich: Whether to use Rich formatting
        console_width: Console width for Rich formatting
        
    Returns:
        Configured FIAResultFormatter instance
    """
    if style == "enhanced":
        return FIAResultFormatter(
            include_emojis=True, 
            confidence_level=0.95,
            use_rich=use_rich,
            console_width=console_width
        )
    elif style == "simple":
        return FIAResultFormatter(
            include_emojis=False, 
            confidence_level=0.95,
            use_rich=use_rich,
            console_width=console_width
        )
    elif style == "scientific":
        return FIAResultFormatter(
            include_emojis=False, 
            confidence_level=0.99,
            use_rich=use_rich,
            console_width=console_width
        )
    else:
        return FIAResultFormatter(
            use_rich=use_rich,
            console_width=console_width
        ) 
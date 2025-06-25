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
    
    def _create_rich_table(self, data: List[Dict[str, Any]], title: str = "Results") -> Table:
        """Create a Rich table from data."""
        if not self.use_rich:
            return None
            
        table = Table(title=title, show_header=True, header_style="bold magenta")
        
        # Determine columns based on data
        if not data:
            return table
            
        first_row = data[0]
        
        # Add columns based on available data
        if 'COMMON_NAME' in first_row:
            table.add_column("Species", style="cyan", no_wrap=True)
        if 'SIZE_CLASS' in first_row:
            table.add_column("Size Class", style="blue")
        
        table.add_column("Population", justify="right", style="green")
        table.add_column("Standard Error", justify="right", style="yellow")
        table.add_column("95% CI Lower", justify="right", style="dim")
        table.add_column("95% CI Upper", justify="right", style="dim")
        table.add_column("Reliability", justify="center", style="bold")
        
        # Add rows
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
        
        return table
    
    def _create_summary_panel(self, query_params: Dict[str, Any], evalid_info: Optional[Dict[str, Any]] = None) -> Panel:
        """Create a Rich panel with query summary."""
        if not self.use_rich:
            return None
            
        summary_lines = []
        
        # Query parameters
        tree_type = query_params.get('tree_type', 'live').title()
        land_type = query_params.get('land_type', 'forest').title()
        summary_lines.append(f"[bold cyan]Tree Status:[/bold cyan] {tree_type} trees")
        summary_lines.append(f"[bold cyan]Land Type:[/bold cyan] {land_type} land")
        
        if query_params.get('tree_domain'):
            summary_lines.append(f"[bold cyan]Tree Filter:[/bold cyan] {query_params['tree_domain']}")
        if query_params.get('area_domain'):
            summary_lines.append(f"[bold cyan]Area Filter:[/bold cyan] {query_params['area_domain']}")
        
        # EVALID context
        if evalid_info:
            summary_lines.append(f"[bold cyan]Evaluation ID:[/bold cyan] {evalid_info.get('evalid', 'Unknown')}")
            if evalid_info.get('description'):
                summary_lines.append(f"[bold cyan]Evaluation:[/bold cyan] {evalid_info['description']}")
        
        summary_lines.append(f"[bold cyan]Analysis Date:[/bold cyan] {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        
        summary_text = "\n".join(summary_lines)
        
        return Panel(
            summary_text,
            title="🌳 Query Summary",
            title_align="left",
            border_style="green",
            padding=(1, 2)
        )
    
    def _create_methodology_panel(self) -> Panel:
        """Create a Rich panel with methodology information."""
        if not self.use_rich:
            return None
            
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
            return self._format_with_rich(result, query_params, evalid_info)
        else:
            return self._format_plain_text(result, query_params, evalid_info)
    
    def _format_with_rich(
        self, 
        result: 'pl.DataFrame', 
        query_params: Dict[str, Any],
        evalid_info: Optional[Dict[str, Any]] = None
    ) -> str:
        """Format results using Rich components."""
        # Convert DataFrame to list of dicts for easier processing
        data = [dict(row) for row in result.iter_rows(named=True)]
        
        # Create components
        summary_panel = self._create_summary_panel(query_params, evalid_info)
        
        # Determine if we need a table or single result display
        is_grouped = any(col in result.columns for col in ['COMMON_NAME', 'SIZE_CLASS'])
        
        if is_grouped and len(data) > 1:
            # Create Rich table for grouped results
            results_table = self._create_rich_table(data, "🌳 Tree Population Analysis")
        else:
            # Create Rich panel for single result
            results_table = self._create_single_result_panel(data[0] if data else {})
        
        methodology_panel = self._create_methodology_panel()
        
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
    
    def _create_single_result_panel(self, data: Dict[str, Any]) -> Panel:
        """Create a Rich panel for single result display."""
        if not self.use_rich or not data:
            return None
            
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
        
        return Panel(
            result_text,
            title="📊 Population Analysis Results",
            title_align="left",
            border_style="magenta",
            padding=(1, 2)
        )
    
    def _format_plain_text(
        self, 
        result: 'pl.DataFrame', 
        query_params: Dict[str, Any],
        evalid_info: Optional[Dict[str, Any]] = None
    ) -> str:
        """Fallback to plain text formatting when Rich is not available."""
        # Header with query context
        tree_emoji = self._get_emoji("tree")
        formatted = f"{tree_emoji} **FIA Tree Count Analysis Results**\n"
        formatted += "=" * 50 + "\n\n"
        
        # Query context section
        chart_emoji = self._get_emoji("chart")
        formatted += f"{chart_emoji} **Query Summary:**\n"
        tree_type = query_params.get('tree_type', 'live').title()
        land_type = query_params.get('land_type', 'forest').title()
        formatted += f"   • Tree Status: {tree_type} trees\n"
        formatted += f"   • Land Type: {land_type} land\n"
        
        if query_params.get('tree_domain'):
            formatted += f"   • Tree Filter: {query_params['tree_domain']}\n"
        if query_params.get('area_domain'):
            formatted += f"   • Area Filter: {query_params['area_domain']}\n"
        
        # Add EVALID context
        if evalid_info:
            formatted += f"   • Evaluation ID: {evalid_info.get('evalid', 'Unknown')}\n"
            if evalid_info.get('description'):
                formatted += f"   • Evaluation: {evalid_info['description']}\n"
        
        formatted += f"   • Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
        
        # Results section
        is_grouped = any(col in result.columns for col in ['COMMON_NAME', 'SIZE_CLASS'])
        
        if is_grouped:
            formatted += self._format_grouped_results(result)
        else:
            formatted += self._format_single_result(result)
        
        # Add methodology and interpretation
        formatted += self._add_methodology_footer()
        
        return formatted
    
    def _format_grouped_results(self, result: 'pl.DataFrame') -> str:
        """Format grouped results (by species, size class, etc.)."""
        trend_emoji = self._get_emoji("trend")
        formatted = f"{trend_emoji} **Detailed Results:**\n\n"
        
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
        
        return formatted
    
    def _format_single_result(self, result: 'pl.DataFrame') -> str:
        """Format single result (total estimate)."""
        row = result.row(0, named=True)
        
        trend_emoji = self._get_emoji("trend")
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
        
        return formatted
    
    def _add_methodology_footer(self) -> str:
        """Add methodology and interpretation guidance."""
        microscope_emoji = self._get_emoji("microscope")
        bulb_emoji = self._get_emoji("bulb")
        
        formatted = f"{microscope_emoji} **Methodology Notes:**\n"
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
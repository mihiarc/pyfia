"""
Utility functions for pyFIA CLI interfaces.

This module contains shared utilities used by the direct CLI,
including state parsing, validation helpers, and display formatting functions.
"""

from pathlib import Path
from typing import Dict, List, Optional, Union

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

# State code mappings for CLI parsing
STATE_ABBR_TO_CODE = {
    "AL": 1, "AK": 2, "AZ": 4, "AR": 5, "CA": 6, "CO": 8, "CT": 9, "DE": 10,
    "FL": 12, "GA": 13, "HI": 15, "ID": 16, "IL": 17, "IN": 18, "IA": 19,
    "KS": 20, "KY": 21, "LA": 22, "ME": 23, "MD": 24, "MA": 25, "MI": 26,
    "MN": 27, "MS": 28, "MO": 29, "MT": 30, "NE": 31, "NV": 32, "NH": 33,
    "NJ": 34, "NM": 35, "NY": 36, "NC": 37, "ND": 38, "OH": 39, "OK": 40,
    "OR": 41, "PA": 42, "RI": 44, "SC": 45, "SD": 46, "TN": 47, "TX": 48,
    "UT": 49, "VT": 50, "VA": 51, "WA": 53, "WV": 54, "WI": 55, "WY": 56,
}

STATE_NAME_TO_CODE = {
    "alabama": 1, "alaska": 2, "arizona": 4, "arkansas": 5, "california": 6,
    "colorado": 8, "connecticut": 9, "delaware": 10, "florida": 12, "georgia": 13,
    "hawaii": 15, "idaho": 16, "illinois": 17, "indiana": 18, "iowa": 19,
    "kansas": 20, "kentucky": 21, "louisiana": 22, "maine": 23, "maryland": 24,
    "massachusetts": 25, "michigan": 26, "minnesota": 27, "mississippi": 28,
    "missouri": 29, "montana": 30, "nebraska": 31, "nevada": 32, "new hampshire": 33,
    "new jersey": 34, "new mexico": 35, "new york": 36, "north carolina": 37,
    "north dakota": 38, "ohio": 39, "oklahoma": 40, "oregon": 41, "pennsylvania": 42,
    "rhode island": 44, "south carolina": 45, "south dakota": 46, "tennessee": 47,
    "texas": 48, "utah": 49, "vermont": 50, "virginia": 51, "washington": 53,
    "west virginia": 54, "wisconsin": 55, "wyoming": 56,
}

STATE_CODE_TO_NAME = {v: k.title() for k, v in STATE_NAME_TO_CODE.items()}
STATE_CODE_TO_ABBR = {v: k for k, v in STATE_ABBR_TO_CODE.items()}


def parse_state_identifier(identifier: str) -> Optional[int]:
    """Parse state identifier (code, abbreviation, or name) to state code.

    Args:
        identifier: State code (e.g., "37"), abbreviation (e.g., "NC"),
                   or name (e.g., "North Carolina")

    Returns:
        State code as integer, or None if not found
    """
    if not identifier:
        return None

    identifier = identifier.strip()

    # Try as direct state code
    try:
        code = int(identifier)
        if code in STATE_CODE_TO_NAME:
            return code
    except ValueError:
        pass

    # Try as abbreviation
    abbr = identifier.upper()
    if abbr in STATE_ABBR_TO_CODE:
        return STATE_ABBR_TO_CODE[abbr]

    # Try as full name
    name = identifier.lower()
    if name in STATE_NAME_TO_CODE:
        return STATE_NAME_TO_CODE[name]

    return None


def get_state_name(state_code: int) -> Optional[str]:
    """Get state name from state code."""
    return STATE_CODE_TO_NAME.get(state_code)


def get_state_abbreviation(state_code: int) -> Optional[str]:
    """Get state abbreviation from state code."""
    return STATE_CODE_TO_ABBR.get(state_code)


def validate_evalid(evalid: str) -> bool:
    """Validate EVALID format (6-digit code: SSYYTT)."""
    if not evalid or len(evalid) != 6:
        return False

    try:
        int(evalid)
        return True
    except ValueError:
        return False


def format_file_size(size_bytes: int) -> str:
    """Format file size in human readable format."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} TB"


def create_database_info_panel(db_path: Path, console: Console) -> Panel:
    """Create a rich panel with database information."""
    try:
        size = format_file_size(db_path.stat().st_size)
        content = (
            f"📁 **Path:** {db_path}\n"
            f"📊 **Size:** {size}\n"
            f"🔗 **Type:** DuckDB Database"
        )

        return Panel(
            content,
            title=f"🗄️ {db_path.name}",
            border_style="blue",
        )
    except Exception:
        return Panel(
            f"📁 **Path:** {db_path}",
            title=f"🗄️ {db_path.name}",
            border_style="blue",
        )


def create_help_table(commands: Dict[str, str], title: str = "Available Commands") -> Table:
    """Create a formatted help table for CLI commands."""
    table = Table(title=title, show_header=True, header_style="bold magenta")
    table.add_column("Command", style="cyan", no_wrap=True)
    table.add_column("Description", style="white")

    for command, description in commands.items():
        table.add_row(command, description)

    return table


def parse_area_arguments(args: List[str]) -> Dict[str, Union[str, bool]]:
    """Parse arguments for area estimation commands."""
    parsed = {
        "land_type": "forest",
        "by_land_type": False,
        "totals": False,
        "tree_domain": None,
        "area_domain": None,
    }

    i = 0
    while i < len(args):
        arg = args[i].lower()

        if arg in ["forest", "timber", "all"]:
            parsed["land_type"] = arg
        elif arg == "by_land_type":
            parsed["by_land_type"] = True
        elif arg == "totals":
            parsed["totals"] = True
        elif arg == "tree_domain" and i + 1 < len(args):
            parsed["tree_domain"] = args[i + 1]
            i += 1
        elif arg == "area_domain" and i + 1 < len(args):
            parsed["area_domain"] = args[i + 1]
            i += 1

        i += 1

    return parsed


def parse_biomass_arguments(args: List[str]) -> Dict[str, Union[str, bool]]:
    """Parse arguments for biomass estimation commands."""
    parsed = {
        "component": "AG",
        "tree_type": "live",
        "land_type": "forest",
        "by_species": False,
        "by_size_class": False,
        "totals": False,
        "tree_domain": None,
        "area_domain": None,
    }

    i = 0
    while i < len(args):
        arg = args[i].upper() if args[i].upper() in ["AG", "BG", "TOTAL"] else args[i].lower()

        if arg in ["AG", "BG", "TOTAL"]:
            parsed["component"] = arg
        elif arg in ["live", "dead", "gs", "all"]:
            parsed["tree_type"] = arg
        elif arg in ["forest", "timber"]:
            parsed["land_type"] = arg
        elif arg == "by_species":
            parsed["by_species"] = True
        elif arg == "by_size_class":
            parsed["by_size_class"] = True
        elif arg == "totals":
            parsed["totals"] = True
        elif arg == "tree_domain" and i + 1 < len(args):
            parsed["tree_domain"] = args[i + 1]
            i += 1
        elif arg == "area_domain" and i + 1 < len(args):
            parsed["area_domain"] = args[i + 1]
            i += 1

        i += 1

    return parsed


def parse_volume_arguments(args: List[str]) -> Dict[str, Union[str, bool]]:
    """Parse arguments for volume estimation commands."""
    parsed = {
        "vol_type": "net",
        "tree_type": "live",
        "land_type": "forest",
        "by_species": False,
        "by_size_class": False,
        "totals": False,
        "tree_domain": None,
        "area_domain": None,
    }

    i = 0
    while i < len(args):
        arg = args[i].lower()

        if arg in ["net", "gross", "sound", "sawlog"]:
            parsed["vol_type"] = arg
        elif arg in ["live", "dead", "gs", "all"]:
            parsed["tree_type"] = arg
        elif arg in ["forest", "timber"]:
            parsed["land_type"] = arg
        elif arg == "by_species":
            parsed["by_species"] = True
        elif arg == "by_size_class":
            parsed["by_size_class"] = True
        elif arg == "totals":
            parsed["totals"] = True
        elif arg == "tree_domain" and i + 1 < len(args):
            parsed["tree_domain"] = args[i + 1]
            i += 1
        elif arg == "area_domain" and i + 1 < len(args):
            parsed["area_domain"] = args[i + 1]
            i += 1

        i += 1

    return parsed


def parse_tpa_arguments(args: List[str]) -> Dict[str, Union[str, bool]]:
    """Parse arguments for TPA estimation commands."""
    parsed = {
        "tree_type": "live",
        "land_type": "forest",
        "by_species": False,
        "by_size_class": False,
        "totals": False,
        "tree_domain": None,
        "area_domain": None,
    }

    i = 0
    while i < len(args):
        arg = args[i].lower()

        if arg in ["live", "dead", "gs", "all"]:
            parsed["tree_type"] = arg
        elif arg in ["forest", "timber"]:
            parsed["land_type"] = arg
        elif arg == "by_species":
            parsed["by_species"] = True
        elif arg == "by_size_class":
            parsed["by_size_class"] = True
        elif arg == "totals":
            parsed["totals"] = True
        elif arg == "tree_domain" and i + 1 < len(args):
            parsed["tree_domain"] = args[i + 1]
            i += 1
        elif arg == "area_domain" and i + 1 < len(args):
            parsed["area_domain"] = args[i + 1]
            i += 1

        i += 1

    return parsed


def create_estimation_help_text() -> str:
    """Create standardized help text for estimation commands."""
    return """
**Common Parameters:**
• `forest` or `timber` - Land type filter
• `live`, `dead`, `gs`, `all` - Tree type filter
• `by_species` - Group results by species
• `by_size_class` - Group by diameter size classes
• `totals` - Include population totals
• `tree_domain "DIA > 10"` - Custom tree filter
• `area_domain "OWNGRPCD == 10"` - Custom area filter

**Examples:**
• `area forest by_land_type` - Forest area by land type
• `biomass AG live forest by_species` - Live tree biomass by species
• `volume net timber by_size_class` - Net volume on timber land by size
• `tpa live tree_domain "DIA >= 5"` - Live trees ≥5" diameter
"""


def format_estimation_results_help(result_type: str) -> List[str]:
    """Format help text for estimation result columns."""
    explanations = []

    if result_type == "area":
        explanations.extend([
            "AREA = Total acres",
            "AREA_PERC = Percentage of total area",
            "SE = Standard Error",
            "N_PLOTS = Number of plots used"
        ])
    elif result_type == "biomass":
        explanations.extend([
            "BIO_ACRE = Biomass per acre (tons/acre)",
            "SE = Standard Error",
            "SE_PERCENT = Standard Error as % of estimate",
            "N_PLOTS = Number of plots used"
        ])
    elif result_type == "volume":
        explanations.extend([
            "VOL_ACRE = Volume per acre (cubic feet/acre)",
            "SE = Standard Error",
            "SE_PERCENT = Standard Error as % of estimate",
            "N_PLOTS = Number of plots used"
        ])
    elif result_type == "tpa":
        explanations.extend([
            "TPA = Trees per acre",
            "BAA = Basal area per acre (sq ft/acre)",
            "SE = Standard Error",
            "N_PLOTS = Number of plots used"
        ])

    return explanations

"""
Location parsing utilities for extracting geographic information from user queries.
"""

import re
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional


class LocationType(Enum):
    """Types of locations that can be parsed."""
    STATE = "state"
    COUNTY = "county"
    REGION = "region"
    FOREST = "forest"
    UNKNOWN = "unknown"


@dataclass
class ParsedLocation:
    """Represents a parsed location with type and identifiers."""

    location_type: LocationType
    raw_text: str
    normalized_name: str
    state_code: Optional[int] = None
    county_code: Optional[int] = None
    region_code: Optional[str] = None
    confidence: float = 1.0

    def to_domain_filter(self) -> str:
        """Convert to an area_domain filter string."""
        if self.location_type == LocationType.STATE and self.state_code:
            return f"STATECD == {self.state_code}"
        elif self.location_type == LocationType.COUNTY and self.state_code and self.county_code:
            return f"STATECD == {self.state_code} AND COUNTYCD == {self.county_code}"
        else:
            raise ValueError(f"Cannot convert {self.location_type} to domain filter")


class LocationParser:
    """Parser for extracting and identifying locations from natural language."""

    def __init__(self):
        # Common state name patterns
        self.state_patterns = [
            # Full state names (most specific first)
            r"\b(north\s+carolina|south\s+carolina|north\s+dakota|south\s+dakota|new\s+hampshire|new\s+jersey|new\s+mexico|new\s+york|west\s+virginia|rhode\s+island)\b",
            r"\b(alabama|alaska|arizona|arkansas|california|colorado|connecticut|delaware|florida|georgia|hawaii|idaho|illinois|indiana|iowa|kansas|kentucky|louisiana|maine|maryland|massachusetts|michigan|minnesota|mississippi|missouri|montana|nebraska|nevada|ohio|oklahoma|oregon|pennsylvania|tennessee|texas|utah|vermont|virginia|washington|wisconsin|wyoming)\b",

            # State abbreviations (only when clearly separated)
            r"(?<!\w)(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)(?!\w)",
        ]

        # Common location indicators
        self.location_indicators = [
            r"\bin\s+",
            r"\bfrom\s+",
            r"\bacross\s+",
            r"\bthroughout\s+",
            r"\bwithin\s+",
        ]

        # Compile patterns
        self.compiled_state_patterns = [re.compile(pattern, re.IGNORECASE) for pattern in self.state_patterns]
        self.compiled_location_indicators = [re.compile(pattern, re.IGNORECASE) for pattern in self.location_indicators]

    def parse(self, text: str) -> List[ParsedLocation]:
        """
        Parse locations from text.

        Args:
            text: Natural language text to parse

        Returns:
            List of parsed locations found in the text
        """
        locations = []

        # Find state references
        state_locations = self._extract_states(text)
        locations.extend(state_locations)

        # TODO: Add county, region, forest parsing

        return locations

    def _extract_states(self, text: str) -> List[ParsedLocation]:
        """Extract state references from text."""
        locations = []
        found_spans = set()  # Track found text spans to avoid duplicates

        for pattern in self.compiled_state_patterns:
            for match in pattern.finditer(text):
                # Check for overlap with already found locations
                if any(match.start() < end and match.end() > start for start, end in found_spans):
                    continue

                raw_text = match.group(1)
                normalized_name = self._normalize_state_name(raw_text)

                # Skip short matches that might be false positives
                if len(raw_text) <= 2 and raw_text.lower() in ['in', 'on', 'at', 'of', 'or', 'is', 'to', 'it']:
                    continue

                location = ParsedLocation(
                    location_type=LocationType.STATE,
                    raw_text=raw_text,
                    normalized_name=normalized_name,
                    confidence=0.9  # High confidence for explicit state matches
                )

                locations.append(location)
                found_spans.add((match.start(), match.end()))

        return locations

    def _normalize_state_name(self, state_text: str) -> str:
        """Normalize state name to standardized form."""
        # Convert to lowercase and normalize spacing
        normalized = re.sub(r'\s+', ' ', state_text.lower().strip())

        # Common abbreviation expansions
        abbreviation_map = {
            'nc': 'north carolina',
            'sc': 'south carolina',
            'nd': 'north dakota',
            'sd': 'south dakota',
            'nh': 'new hampshire',
            'nj': 'new jersey',
            'nm': 'new mexico',
            'ny': 'new york',
            'wv': 'west virginia',
            'ri': 'rhode island'
        }

        return abbreviation_map.get(normalized, normalized)

    def find_primary_location(self, text: str) -> Optional[ParsedLocation]:
        """
        Find the primary/most relevant location in text.

        Args:
            text: Text to parse

        Returns:
            The most relevant location found, or None if no locations detected
        """
        locations = self.parse(text)

        if not locations:
            return None

        # For now, return the first state found
        # TODO: Implement more sophisticated primary location detection
        state_locations = [loc for loc in locations if loc.location_type == LocationType.STATE]

        if state_locations:
            return state_locations[0]

        return locations[0] if locations else None

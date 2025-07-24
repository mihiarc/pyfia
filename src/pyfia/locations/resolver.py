"""
Location resolution utilities for converting parsed locations to database identifiers.
"""

from typing import Optional

from ..constants import StateCodes
from .parser import LocationType, ParsedLocation


class LocationResolver:
    """Resolves parsed locations to database identifiers (FIPS codes, etc.)."""

    def __init__(self):
        # Use state mappings from constants module
        self.state_name_to_code = StateCodes.NAME_TO_CODE
        self.state_code_to_name = StateCodes.CODE_TO_NAME

    def resolve(self, location: ParsedLocation) -> ParsedLocation:
        """
        Resolve a parsed location to include database identifiers.

        Args:
            location: ParsedLocation to resolve

        Returns:
            Updated ParsedLocation with resolved identifiers
        """
        if location.location_type == LocationType.STATE:
            return self._resolve_state(location)
        elif location.location_type == LocationType.COUNTY:
            return self._resolve_county(location)
        else:
            # For now, return as-is for other location types
            return location

    def _resolve_state(self, location: ParsedLocation) -> ParsedLocation:
        """Resolve state location to FIPS state code."""
        state_code = self.state_name_to_code.get(location.normalized_name)

        if state_code:
            location.state_code = state_code
            location.confidence = min(location.confidence, 0.95)  # High confidence for successful resolution
        else:
            # Try partial matching
            state_code = self._fuzzy_match_state(location.normalized_name)
            if state_code:
                location.state_code = state_code
                location.confidence = min(location.confidence, 0.8)  # Lower confidence for fuzzy match

        return location

    def _resolve_county(self, location: ParsedLocation) -> ParsedLocation:
        """Resolve county location to FIPS codes."""
        # TODO: Implement county resolution
        # This would require county name to FIPS mappings
        return location

    def _fuzzy_match_state(self, state_name: str) -> Optional[int]:
        """Attempt fuzzy matching for state names."""
        # Simple fuzzy matching - check if state_name is contained in any key
        for name, code in self.state_name_to_code.items():
            if state_name in name or name in state_name:
                return code
        return None

    def get_state_name(self, state_code: int) -> Optional[str]:
        """Get state name from FIPS code."""
        return self.state_code_to_name.get(state_code)

    def get_state_code(self, state_identifier: str) -> Optional[int]:
        """
        Get FIPS state code from name or abbreviation.

        Args:
            state_identifier: State name, abbreviation, or already a code

        Returns:
            FIPS state code or None if not found
        """
        # If it's already a number, return it
        try:
            code = int(state_identifier)
            if 1 <= code <= 56:  # Valid FIPS state code range
                return code
        except ValueError:
            pass

        # Try direct lookup
        normalized = state_identifier.lower().strip()
        code = self.state_name_to_code.get(normalized)
        if code:
            return code

        # Try fuzzy matching
        return self._fuzzy_match_state(normalized)

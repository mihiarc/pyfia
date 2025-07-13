"""
AI and machine learning components for pyFIA.

This module provides:
- AI agents for natural language queries
- FIA domain knowledge for enhanced analysis
- Prompt templates and AI utilities

Note: This module requires optional AI dependencies (langchain, openai, etc.)
Install with: pip install pyfia[ai]
"""

try:
    from .agent import FIAAgent
    from .domain_knowledge import *

    __all__ = [
        "FIAAgent",
    ]

except ImportError:
    # AI dependencies not installed
    def _create_import_error(name):
        def _missing(*args, **kwargs):
            raise ImportError(
                "AI functionality requires optional dependencies. "
                "Install with: pip install pyfia[ai]"
            ) from e
        return _missing

    FIAAgent = _create_import_error("FIAAgent")

    __all__ = ["FIAAgent"]

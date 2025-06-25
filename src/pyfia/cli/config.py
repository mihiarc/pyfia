"""
Configuration management for FIA CLI.
"""

import json
from pathlib import Path
from typing import Any, Dict, Optional


class CLIConfig:
    """Manage CLI configuration."""

    def __init__(self):
        self.config_dir = Path.home() / ".fia"
        self.config_file = self.config_dir / "config.json"
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from file."""
        if self.config_file.exists():
            try:
                with open(self.config_file, "r") as f:
                    return json.load(f)
            except:
                pass
        return {}

    def save_config(self):
        """Save configuration to file."""
        self.config_dir.mkdir(exist_ok=True)
        with open(self.config_file, "w") as f:
            json.dump(self.config, f, indent=2)

    @property
    def default_database(self) -> Optional[str]:
        """Get default database path."""
        return self.config.get("default_database")

    @default_database.setter
    def default_database(self, path: str):
        """Set default database path."""
        self.config["default_database"] = path
        self.save_config()

    @property
    def recent_databases(self) -> list:
        """Get list of recently used databases."""
        return self.config.get("recent_databases", [])

    def add_recent_database(self, path: str):
        """Add database to recent list."""
        recent = self.recent_databases
        if path in recent:
            recent.remove(path)
        recent.insert(0, path)
        # Keep only last 5
        self.config["recent_databases"] = recent[:5]
        self.save_config()

    @property
    def state_shortcuts(self) -> Dict[str, str]:
        """Get state database shortcuts."""
        return self.config.get("state_shortcuts", {})

    def add_state_shortcut(self, state: str, path: str):
        """Add a state shortcut."""
        if "state_shortcuts" not in self.config:
            self.config["state_shortcuts"] = {}
        self.config["state_shortcuts"][state.upper()] = path
        self.save_config()

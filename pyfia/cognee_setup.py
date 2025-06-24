"""Setup module for Cognee - must be imported before cognee."""

import os
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up LLM API key for Cognee
if "OPENAI_API_KEY" in os.environ:
    # Cognee expects these environment variables
    os.environ["LLM_API_KEY"] = os.environ["OPENAI_API_KEY"]
    os.environ["LLM_PROVIDER"] = "openai"
    os.environ["LLM_MODEL"] = "gpt-4o-mini"
    os.environ["EMBEDDING_PROVIDER"] = "openai"
    os.environ["EMBEDDING_API_KEY"] = os.environ["OPENAI_API_KEY"]
    print("Configured Cognee with OpenAI settings")

# Configure data directories
data_dir = Path.cwd() / ".cognee_data" / "data_storage"
system_dir = Path.cwd() / ".cognee_data" / "cognee_system"

# Create directories
data_dir.mkdir(parents=True, exist_ok=True)
system_dir.mkdir(parents=True, exist_ok=True)

# Store paths for later configuration
COGNEE_DATA_DIR = str(data_dir)
COGNEE_SYSTEM_DIR = str(system_dir)


def configure_cognee():
    """Configure Cognee with our directories.

    This must be called after importing cognee.
    """
    try:
        from cognee import config
        
        config.data_root_directory(COGNEE_DATA_DIR)
        config.system_root_directory(COGNEE_SYSTEM_DIR)
        
        # Also set environment variables as backup
        os.environ["COGNEE_DATA_DIR"] = COGNEE_DATA_DIR
        os.environ["COGNEE_SYSTEM_DIR"] = COGNEE_SYSTEM_DIR
        
        print(f"Cognee configured with data dir: {COGNEE_DATA_DIR}")
        
    except Exception as e:
        print(f"Warning: Could not configure Cognee: {e}")
        # Set environment variables as fallback
        os.environ["COGNEE_DATA_DIR"] = COGNEE_DATA_DIR
        os.environ["COGNEE_SYSTEM_DIR"] = COGNEE_SYSTEM_DIR

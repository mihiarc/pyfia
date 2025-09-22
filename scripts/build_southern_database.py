#!/usr/bin/env python3
"""
Build a Southern US FIA database in DuckDB format.

This script creates a multi-state FIA database for the Southern US region
by either:
1. Converting existing SQLite files downloaded from FIA DataMart
2. Extracting and converting ZIP files from FIA DataMart
3. Providing instructions for downloading the required files

Southern states included:
- Alabama (01)
- Arkansas (05)
- Florida (12)
- Georgia (13)
- Louisiana (22)
- Mississippi (28)
- North Carolina (37)
- Oklahoma (40)
- South Carolina (45)
- Tennessee (47)
- Texas (48)
- Virginia (51)
"""

from pathlib import Path
import sys
import argparse
import zipfile
import tempfile
import shutil
from typing import List, Dict, Optional

# Add parent directory to path to import pyfia
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyfia.converter import (
    convert_sqlite_to_duckdb,
    append_state,
    get_database_info
)
from pyfia.constants import StateCodes


SOUTHERN_STATES = {
    "Alabama": 1,
    "Arkansas": 5,
    "Florida": 12,
    "Georgia": 13,
    "Louisiana": 22,
    "Mississippi": 28,
    "North Carolina": 37,
    "Oklahoma": 40,
    "South Carolina": 45,
    "Tennessee": 47,
    "Texas": 48,
    "Virginia": 51,
}

FIA_DATAMART_URL = "https://apps.fs.usda.gov/fia/datamart/datamart.html"


def find_data_files(data_dir: Path) -> Dict[int, tuple[Path, str]]:
    """Find SQLite or ZIP files for Southern states in the data directory.

    Returns:
        Dict mapping state code to (file_path, file_type) where file_type is 'db' or 'zip'
    """
    found_files = {}

    for state_name, state_code in SOUTHERN_STATES.items():
        state_abbr = StateCodes.CODE_TO_ABBR.get(state_code, '')

        # Try different naming patterns for both .db and .zip files
        patterns = [
            (f"SQLite_FIADB_{state_abbr}.db", 'db'),
            (f"SQLite_FIADB_{state_abbr}.zip", 'zip'),
            (f"SQLite_FIADB_{state_name.replace(' ', '_')}.db", 'db'),
            (f"SQLite_FIADB_{state_name.replace(' ', '_')}.zip", 'zip'),
            (f"*{state_abbr}*.db", 'db'),
            (f"*{state_abbr}*.zip", 'zip'),
        ]

        for pattern, file_type in patterns:
            matches = list(data_dir.glob(pattern))
            if matches:
                found_files[state_code] = (matches[0], file_type)
                break

    return found_files


def extract_sqlite_from_zip(zip_path: Path, temp_dir: Path) -> Path:
    """Extract SQLite database from ZIP file to temporary directory.

    Returns:
        Path to extracted .db file
    """
    print(f"  Extracting {zip_path.name}...")
    with zipfile.ZipFile(zip_path, 'r') as zf:
        # Find the .db file in the archive
        db_files = [f for f in zf.namelist() if f.endswith('.db')]
        if not db_files:
            raise ValueError(f"No .db file found in {zip_path}")

        # Extract the first .db file
        db_filename = db_files[0]
        extracted_path = temp_dir / Path(db_filename).name

        with zf.open(db_filename) as source, open(extracted_path, 'wb') as target:
            shutil.copyfileobj(source, target)

    return extracted_path


def download_instructions(missing_states: List[str]) -> None:
    """Print instructions for downloading missing SQLite files."""
    print("\n" + "="*60)
    print("DOWNLOAD INSTRUCTIONS")
    print("="*60)
    print(f"\nTo download the missing SQLite files, visit:")
    print(f"  {FIA_DATAMART_URL}")
    print("\nFor each missing state:")
    print("1. Click on the state name")
    print("2. Download the 'SQLite' version (not CSV)")
    print("3. Place the downloaded .db file in the 'data' directory")
    print("\nMissing states:")
    for state in missing_states:
        abbr = StateCodes.CODE_TO_ABBR.get(SOUTHERN_STATES[state], "??")
        print(f"  - {state} ({abbr})")
    print("\n" + "="*60 + "\n")


def build_database(
    data_dir: Path,
    output_path: Path,
    force: bool = False,
    yes: bool = False
) -> None:
    """Build the Southern US FIA database."""

    # Check if output exists
    if output_path.exists() and not force:
        print(f"Output file {output_path} already exists.")
        if not yes:
            response = input("Overwrite? (y/n): ")
            if response.lower() != 'y':
                print("Aborted.")
                return
        else:
            print("  Overwriting existing file (--yes flag)")
            output_path.unlink()

    # Find available data files (SQLite or ZIP)
    print("Searching for SQLite and ZIP files...")
    found_files = find_data_files(data_dir)

    if not found_files:
        print("\nNo SQLite or ZIP files found!")
        download_instructions(list(SOUTHERN_STATES.keys()))
        return

    # Check what's missing
    missing_codes = set(SOUTHERN_STATES.values()) - set(found_files.keys())
    if missing_codes:
        missing_names = [
            name for name, code in SOUTHERN_STATES.items()
            if code in missing_codes
        ]
        print(f"\nFound {len(found_files)} of {len(SOUTHERN_STATES)} states.")
        print("Missing states:", ", ".join(missing_names))

        if not yes:
            response = input("\nContinue with available states? (y/n): ")
            if response.lower() != 'y':
                download_instructions(missing_names)
                return
        else:
            print("  Continuing with available states (--yes flag)")
    else:
        print(f"\nFound all {len(SOUTHERN_STATES)} Southern states!")

    # Count file types
    zip_count = sum(1 for _, (_, ftype) in found_files.items() if ftype == 'zip')
    db_count = sum(1 for _, (_, ftype) in found_files.items() if ftype == 'db')
    if zip_count > 0:
        print(f"  {zip_count} ZIP files, {db_count} DB files")

    # Build the database
    print(f"\nBuilding database: {output_path}")
    print("-" * 40)

    # Create temporary directory for extracting ZIPs
    with tempfile.TemporaryDirectory(prefix="pyfia_extract_") as temp_dir:
        temp_path = Path(temp_dir)

        # Process first state
        first_state = list(found_files.items())[0]
        state_code, (file_path, file_type) = first_state
        state_name = [k for k, v in SOUTHERN_STATES.items() if v == state_code][0]

        print(f"\n[1/{len(found_files)}] Converting {state_name}...")

        # Extract if needed
        if file_type == 'zip':
            sqlite_path = extract_sqlite_from_zip(file_path, temp_path)
        else:
            sqlite_path = file_path

        row_counts = convert_sqlite_to_duckdb(
            source_path=sqlite_path,
            target_path=output_path,
            state_code=state_code,
            show_progress=True
        )
        print(f"  Converted {len(row_counts)} tables")

        # Clean up extracted file if it was a ZIP
        if file_type == 'zip' and sqlite_path.exists():
            sqlite_path.unlink()

        # Append remaining states
        for i, (state_code, (file_path, file_type)) in enumerate(list(found_files.items())[1:], 2):
            state_name = [k for k, v in SOUTHERN_STATES.items() if v == state_code][0]
            print(f"\n[{i}/{len(found_files)}] Appending {state_name}...")

            # Extract if needed
            if file_type == 'zip':
                sqlite_path = extract_sqlite_from_zip(file_path, temp_path)
            else:
                sqlite_path = file_path

            row_counts = append_state(
                source_path=sqlite_path,
                target_path=output_path,
                state_code=state_code,
                dedupe=False,  # No deduplication for new states
                show_progress=True
            )
            print(f"  Appended {sum(row_counts.values()):,} rows")

            # Clean up extracted file if it was a ZIP
            if file_type == 'zip' and sqlite_path.exists():
                sqlite_path.unlink()

    # Print summary
    print("\n" + "="*60)
    print("DATABASE BUILD COMPLETE")
    print("="*60)

    db_info = get_database_info(output_path)
    print(f"\nDatabase: {output_path}")
    print(f"Size: {db_info['file_size_mb']:.1f} MB")
    print(f"Tables: {db_info['total_tables']}")
    print(f"Total rows: {db_info['total_rows']:,}")

    # Show state summary
    print("\nStates included:")
    for state_code in sorted(found_files.keys()):
        state_name = [k for k, v in SOUTHERN_STATES.items() if v == state_code][0]
        abbr = StateCodes.CODE_TO_ABBR.get(state_code, "??")
        print(f"  {state_name:20s} ({abbr})")

    print("\n" + "="*60)
    print("\nExample usage:")
    print("  from pyfia import FIA, area")
    print(f"  db = FIA('{output_path}')")
    print("  results = area(db)")
    print("\n" + "="*60 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="Build a Southern US FIA database in DuckDB format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Build database from SQLite/ZIP files in data/
  python build_southern_database.py

  # Specify custom data directory
  python build_southern_database.py --data-dir ~/fia_downloads

  # Specify custom output location
  python build_southern_database.py --output ~/databases/southern.duckdb

  # Auto-confirm all prompts
  python build_southern_database.py --yes

  # Force overwrite existing database
  python build_southern_database.py --force
        """
    )

    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data"),
        help="Directory containing SQLite/ZIP files (default: data/)"
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/southern_states.duckdb"),
        help="Output DuckDB file path (default: data/southern_states.duckdb)"
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Force overwrite existing database"
    )

    parser.add_argument(
        "-y", "--yes",
        action="store_true",
        help="Automatically answer yes to all prompts"
    )

    args = parser.parse_args()

    # Ensure data directory exists
    args.data_dir.mkdir(parents=True, exist_ok=True)

    # Build the database
    build_database(
        data_dir=args.data_dir,
        output_path=args.output,
        force=args.force,
        yes=args.yes
    )


if __name__ == "__main__":
    main()
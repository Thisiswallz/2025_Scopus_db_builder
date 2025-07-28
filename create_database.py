#!/usr/bin/env python3
"""
Quick-start script for creating Scopus database using the package.
This demonstrates how to use the scopus_db package programmatically.

Data quality filtering is always enabled to ensure high-quality research data.

Usage:
  python create_database.py <scopus.csv>                    # Single CSV file
  python create_database.py <directory>                     # Directory with multiple CSV files
"""

import sys
from pathlib import Path
from scopus_db import OptimalScopusDatabase


def find_scopus_csv_files(directory_path: Path) -> list:
    """Find all Scopus CSV files in the given directory."""
    csv_files = []
    
    # Look for CSV files, excluding temporary and log files
    for csv_file in directory_path.glob("*.csv"):
        # Skip temporary files, backup files, and exclusion logs
        if (csv_file.name.startswith("~") or 
            csv_file.name.startswith(".") or
            "exclusion" in csv_file.name.lower() or
            csv_file.name.endswith("_backup.csv")):
            continue
        csv_files.append(csv_file)
    
    return sorted(csv_files)


def main():
    """Create Scopus database from CSV file(s) or directory with data quality filtering."""
    if len(sys.argv) != 2:
        print("Usage: python create_database.py <scopus.csv|directory>")
        print("")
        print("Examples:")
        print("  # Single CSV file")
        print("  python create_database.py data/scopus_exports/export_1/scopus.csv")
        print("  # Directory with multiple CSV files")
        print("  python create_database.py data/scopus_exports/export_1/")
        print("")
        print("Multi-CSV Processing:")
        print("  - Combines all CSV files in directory into single database")
        print("  - Automatically detects and removes duplicate records")
        print("  - Reports source files and deduplication statistics")
        print("")
        print("Data Quality Filtering (always enabled):")
        print("  - Excludes papers missing authors, abstracts, or publication dates")
        print("  - Filters out editorial content (corrections, errata)")
        print("  - Removes non-research content (conference announcements, TOC)")
        print("  - Generates detailed exclusion log with rationales")
        sys.exit(1)
    
    input_path = Path(sys.argv[1])
    print("✅ Data quality filtering ENABLED - ensuring high-quality research data")
    
    # Handle directory vs file input
    if input_path.is_dir():
        # Multi-CSV mode: Process directory
        csv_files = find_scopus_csv_files(input_path)
        
        if not csv_files:
            print(f"❌ Error: No CSV files found in directory '{input_path}'")
            print("   Make sure the directory contains Scopus export CSV files.")
            sys.exit(1)
        
        print(f"\n📁 Multi-CSV Processing Mode")
        print(f"   Directory: {input_path}")
        print(f"   CSV files found: {len(csv_files)}")
        for i, csv_file in enumerate(csv_files, 1):
            print(f"   {i}. {csv_file.name}")
        print()
        
        # Ask for user confirmation
        while True:
            response = input("❓ Proceed with processing these CSV files? [y/N]: ").strip().lower()
            if response in ['y', 'yes']:
                break
            elif response in ['n', 'no', '']:
                print("❌ Processing cancelled by user.")
                sys.exit(0)
            else:
                print("   Please enter 'y' for yes or 'n' for no.")
        
        # Create database using directory mode
        db_creator = OptimalScopusDatabase(
            csv_path=input_path, 
            enable_data_filtering=True,
            csv_files=csv_files
        )
        
    elif input_path.is_file() and input_path.suffix.lower() == '.csv':
        # Single CSV mode: Process individual file
        print(f"\n📄 Single CSV Processing Mode")
        print(f"   File: {input_path}")
        print()
        
        # Ask for user confirmation
        while True:
            response = input("❓ Proceed with processing this CSV file? [y/N]: ").strip().lower()
            if response in ['y', 'yes']:
                break
            elif response in ['n', 'no', '']:
                print("❌ Processing cancelled by user.")
                sys.exit(0)
            else:
                print("   Please enter 'y' for yes or 'n' for no.")
        
        db_creator = OptimalScopusDatabase(
            csv_path=input_path, 
            enable_data_filtering=True
        )
    else:
        print(f"❌ Error: '{input_path}' is not a valid CSV file or directory")
        print("   Please provide either:")
        print("   - A single CSV file path (e.g., scopus.csv)")
        print("   - A directory containing CSV files (e.g., export_1/)")
        sys.exit(1)
    
    # Create database schema and process data
    db_creator.create_optimal_schema()
    db_creator.process_csv_to_optimal_db()
    db_creator.conn.close()
    
    print(f"\n✅ Database created: {db_creator.db_path}")
    print(f"📝 Exclusion log: {db_creator.data_filter.log_path}")


if __name__ == "__main__":
    main()
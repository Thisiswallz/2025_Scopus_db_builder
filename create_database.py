#!/usr/bin/env python3
"""
Quick-start script for creating Scopus database using the package.
This demonstrates how to use the scopus_db package programmatically.
"""

import sys
from scopus_db import OptimalScopusDatabase


def main():
    """Create Scopus database from CSV file."""
    if len(sys.argv) != 2:
        print("Usage: python create_database.py <scopus.csv>")
        print("Example: python create_database.py data/scopus_exports/export_1/scopus.csv")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    
    # Create database using the package
    db_creator = OptimalScopusDatabase(csv_path)
    db_creator.create_optimal_schema()
    db_creator.process_csv_to_optimal_db()
    db_creator.conn.close()
    
    print(f"\nDatabase created: {db_creator.db_path}")


if __name__ == "__main__":
    main()
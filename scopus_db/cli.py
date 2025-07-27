#!/usr/bin/env python3
"""
Command-line interface for Scopus Database creation.
"""

import sys
import os
from pathlib import Path
from .database import OptimalScopusDatabase


def main():
    """Main CLI entry point for Scopus database creation."""
    if len(sys.argv) != 2:
        print("Usage: scopus-db <scopus.csv>")
        print("Example: scopus-db data/scopus_exports/export_1/scopus.csv")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    
    if not os.path.exists(csv_path):
        print(f"Error: CSV file not found: {csv_path}")
        sys.exit(1)
    
    try:
        # Create database
        print("🚀 Starting Scopus Database Creation")
        print(f"Input: {csv_path}")
        
        db_creator = OptimalScopusDatabase(csv_path)
        
        # Create schema
        db_creator.create_optimal_schema()
        
        # Process data
        db_creator.process_csv_to_optimal_db()
        
        print("\n🎉 Scopus database creation completed!")
        print(f"📁 Database location: {db_creator.db_path}")
        
        # Close connection
        db_creator.conn.close()
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
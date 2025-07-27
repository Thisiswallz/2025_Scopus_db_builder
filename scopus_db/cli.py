#!/usr/bin/env python3
"""
Command-line interface for Scopus Database creation and validation.
"""

import sys
import os
import argparse
from pathlib import Path
from .database import OptimalScopusDatabase
from .validator import DatabaseValidator


def create_database(args):
    """Handle 'create' subcommand."""
    csv_path = args.csv_file
    
    if not os.path.exists(csv_path):
        print(f"❌ Error: CSV file not found: {csv_path}")
        sys.exit(1)
    
    try:
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


def check_database(args):
    """Handle 'check' subcommand."""
    db_path = args.db_file
    csv_path = args.csv_file
    
    if not os.path.exists(db_path):
        print(f"❌ Error: Database file not found: {db_path}")
        sys.exit(1)
        
    if not os.path.exists(csv_path):
        print(f"❌ Error: CSV file not found: {csv_path}")
        sys.exit(1)
    
    try:
        print("🔍 Starting Database Integrity Check")
        print(f"Database: {db_path}")
        print(f"Source CSV: {csv_path}")
        
        validator = DatabaseValidator(csv_path, db_path)
        
        # Run all integrity tests
        results = validator.run_all_tests()
        
        if results['all_passed']:
            print("\n✅ All integrity tests passed!")
            print("Database accurately represents all CSV data.")
        else:
            print("\n❌ Some integrity tests failed!")
            print(f"Passed: {results['passed_tests']}")
            print(f"Failed: {results['failed_tests']}")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def main():
    """Main CLI entry point with subcommand support."""
    parser = argparse.ArgumentParser(
        prog='scopus-db',
        description='Create and validate Scopus databases from CSV exports'
    )
    
    subparsers = parser.add_subparsers(dest='command', required=True, help='Available commands')
    
    # Create subcommand
    create_parser = subparsers.add_parser('create', help='Create database from CSV file')
    create_parser.add_argument('csv_file', help='Path to Scopus CSV export file')
    create_parser.set_defaults(func=create_database)
    
    # Check subcommand
    check_parser = subparsers.add_parser('check', help='Check database integrity against CSV')
    check_parser.add_argument('db_file', help='Path to SQLite database file')
    check_parser.add_argument('--csv-file', required=True, help='Path to original CSV file')
    check_parser.set_defaults(func=check_database)
    
    # Parse arguments and run appropriate function
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
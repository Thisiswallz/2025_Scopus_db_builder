#!/usr/bin/env python3
"""
Scopus Database API

High-level API interface for creating and validating Scopus databases.
This module provides a clean, importable interface for external projects.

Author: Claude Code
Created: 2025-07-27
"""

import os
from pathlib import Path
from .database import OptimalScopusDatabase
from .validator import DatabaseValidator


class ScopusDB:
    """
    High-level API for Scopus database operations.
    
    This class provides a simple interface for creating and validating
    Scopus databases from CSV exports, suitable for use in external projects.
    """
    
    @staticmethod
    def create_database(csv_path: str, output_path: str = None, **options) -> str:
        """
        Create a Scopus database from a CSV export file.
        
        Args:
            csv_path (str): Path to the Scopus CSV export file
            output_path (str, optional): Path for the output database file.
                                       If None, places in same directory as CSV.
            **options: Additional options (reserved for future use)
            
        Returns:
            str: Path to the created database file
            
        Raises:
            FileNotFoundError: If CSV file doesn't exist
            Exception: If database creation fails
            
        Example:
            >>> from scopus_db import ScopusDB
            >>> db_path = ScopusDB.create_database("data/scopus_export.csv")
            >>> print(f"Database created: {db_path}")
        """
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
        
        # Create database using the existing optimal creator
        db_creator = OptimalScopusDatabase(csv_path)
        
        # Override output path if specified
        if output_path:
            db_creator.db_path = Path(output_path)
        
        # Create schema and process data
        db_creator.create_optimal_schema()
        db_creator.process_csv_to_optimal_db()
        
        # Close connection and return path
        db_path = str(db_creator.db_path)
        db_creator.conn.close()
        
        return db_path
    
    @staticmethod
    def validate_database(db_path: str, csv_path: str, **options) -> dict:
        """
        Validate database integrity against the original CSV file.
        
        Args:
            db_path (str): Path to the SQLite database file
            csv_path (str): Path to the original Scopus CSV export file
            **options: Additional validation options (reserved for future use)
            
        Returns:
            dict: Validation results with keys:
                - 'all_passed' (bool): Whether all tests passed
                - 'passed_tests' (int): Number of passed tests
                - 'failed_tests' (int): Number of failed tests
                - 'test_results' (list): Detailed test results
                
        Raises:
            FileNotFoundError: If database or CSV file doesn't exist
            Exception: If validation fails to run
            
        Example:
            >>> from scopus_db import ScopusDB
            >>> results = ScopusDB.validate_database("scopus.db", "scopus.csv")
            >>> if results['all_passed']:
            ...     print("✅ Database validation passed!")
            >>> else:
            ...     print(f"❌ {results['failed_tests']} tests failed")
        """
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"Database file not found: {db_path}")
        
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
        
        # Run validation using the validator
        validator = DatabaseValidator(csv_path, db_path)
        results = validator.run_all_tests()
        
        return results
    
    @staticmethod
    def get_database_info(db_path: str) -> dict:
        """
        Get basic information about a Scopus database.
        
        Args:
            db_path (str): Path to the SQLite database file
            
        Returns:
            dict: Database information including table counts and schema details
            
        Raises:
            FileNotFoundError: If database file doesn't exist
            
        Example:
            >>> from scopus_db import ScopusDB
            >>> info = ScopusDB.get_database_info("scopus.db")
            >>> print(f"Papers: {info['papers']}")
            >>> print(f"Authors: {info['authors']}")
        """
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"Database file not found: {db_path}")
        
        import sqlite3
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get table counts
        tables = {
            'papers': 'papers',
            'authors': 'authors_master', 
            'institutions': 'institutions_master',
            'keywords': 'keywords_master',
            'author_relationships': 'paper_authors',
            'keyword_relationships': 'paper_keywords',
            'institution_relationships': 'paper_institutions'
        }
        
        info = {}
        for key, table_name in tables.items():
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                info[key] = cursor.fetchone()[0]
            except sqlite3.OperationalError:
                info[key] = 0
        
        # Get database file size
        info['file_size_mb'] = round(os.path.getsize(db_path) / (1024 * 1024), 2)
        
        conn.close()
        return info
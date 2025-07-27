"""
Scopus Database Package

A Python package for creating and validating optimized SQLite databases from Scopus CSV exports.
Provides both CLI tools and programmatic API for external projects.

Features:
- CLI: `scopus-db create <csv_file>` - Create database from CSV
- CLI: `scopus-db check <db_file> --csv-file <csv_file>` - Validate database integrity  
- API: ScopusDB.create_database() - Programmatic database creation
- API: ScopusDB.validate_database() - Programmatic validation
"""

__version__ = "0.2.0"
__author__ = "Claude Code"

# Import main classes for external use
from .database.creator import OptimalScopusDatabase
from .api import ScopusDB
from .validator import DatabaseValidator

# Public API - what users should import
__all__ = [
    "ScopusDB",              # High-level API for external projects
    "OptimalScopusDatabase", # Lower-level database creator
    "DatabaseValidator"      # Database validation utility
]
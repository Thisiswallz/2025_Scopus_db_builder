"""
Scopus Database Package

A Python package for creating optimized SQLite databases from Scopus CSV exports.
Focuses on pure data extraction and structuring without pre-computed analytics.
"""

__version__ = "0.1.0"
__author__ = "Claude Code"

from .database.creator import OptimalScopusDatabase

__all__ = ["OptimalScopusDatabase"]
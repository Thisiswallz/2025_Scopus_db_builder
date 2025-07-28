"""
CrossRef API Integration Module

This module provides CrossRef REST API integration for recovering missing metadata
from Scopus records, particularly DOIs and other bibliographic information.

Components:
- CrossRefClient: HTTP client with polite pool compliance
- RecoveryEngine: Search and validation logic (future phases)
"""

from .crossref_client import CrossRefClient

__all__ = ['CrossRefClient']
"""
Lens API Integration Package

Provides patent data enrichment capabilities for Scopus databases
using the Lens Patent API.
"""

__version__ = "1.0.0"

from .client import LensClient
from .enricher import LensEnricher
from .analytics import InnovationAnalytics

__all__ = [
    'LensClient',
    'LensEnricher', 
    'InnovationAnalytics'
]
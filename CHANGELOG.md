# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2025-07-27

### Added
- Initial release of Scopus Database Creator
- Pure data extraction from Scopus CSV exports
- Comprehensive data parsing for:
  - Papers with metadata
  - Authors with normalization
  - Institutions extraction
  - Keywords (author and index)
  - Funding information
  - Chemical compounds
  - Trade names
  - Citations
- SQLite database creation with optimized schema
- Relationship tables for papers, authors, keywords, and institutions
- Database integrity test suite
- Proper Python package structure
- CLI tool for database creation

### Changed
- Removed all pre-computed analytics (h-index, TF-IDF, collaborations)
- Focus on pure data repository without calculations

### Technical Details
- Python 3.6+ compatible
- No external dependencies (uses only standard library)
- Comprehensive test coverage
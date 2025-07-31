# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

The Scopus Database Builder is a zero-dependency Python tool that converts Scopus CSV exports into optimized SQLite databases. It features automatic CrossRef DOI recovery, data quality filtering, and comprehensive reporting.

## Common Commands

### Running the Database Creator
```bash
# Process single CSV file
python 1_create_database.py data/scopus_export.csv

# Process directory with multiple CSV files
python 1_create_database.py data/export_1/

# With organized folder structure (default)
python 1_create_database.py data/export_1/  # Looks in data/export_1/raw_scopus/
```

### Running Tests
```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_crossref_phase1.py

# Run with coverage
pytest --cov=scopus_db

# Run tests matching pattern
pytest -k "crossref"
```

### Code Quality Tools
```bash
# Format code with black
black scopus_db/ tests/

# Sort imports
isort scopus_db/ tests/

# Lint with flake8
flake8 scopus_db/ tests/

# Type checking
mypy scopus_db/
```

### Package Installation
```bash
# Install in development mode
pip install -e .

# Install with dev dependencies
pip install -e ".[dev]"

# Install with visualization extras
pip install -e ".[viz]"
```

### CLI Commands (after installation)
```bash
# Create database
scopus-db create data/scopus.csv

# Validate database
scopus-db check database.db --csv-file data/scopus.csv
```

## Architecture Overview

### Zero-Dependency Design
The system uses only Python standard library modules - no external dependencies required for core functionality. This design choice ensures maximum compatibility and ease of deployment.

### Multi-Phase Processing Pipeline

1. **Data Quality Filtering** (`scopus_db/data_quality_filter.py`)
   - Removes low-quality records (missing essential fields)
   - Generates comprehensive exclusion reports
   - Triggers CrossRef recovery for missing DOIs

2. **CrossRef DOI Recovery** (`scopus_db/crossref/`)
   - Phase 1: PubMed ID lookup (95% success rate)
   - Phase 2a: Journal metadata matching (40-60% success)
   - Phase 2b: Title fuzzy matching (20-40% success)
   - Confidence scoring system for match validation

3. **Database Creation** (`scopus_db/database/creator.py`)
   - Creates optimized SQLite schema with proper relationships
   - Normalizes entities (authors, institutions, keywords)
   - Pre-computes analytics and collaboration networks
   - Maintains recovery attribution metadata

### Key Components

- **Entry Points**:
  - `1_create_database.py` - Main CLI script (database creation)
  - `2_enrich_database.py` - Enrichment script (CrossRef DOI recovery)
  - `3_Lens_database.py` - Lens enrichment script (patent/institution data)
  - `scopus_db.cli` - Package CLI interface
  - `scopus_db.api` - Python API (ScopusDB class)

- **Core Classes**:
  - `OptimalScopusDatabase` - Database creation and schema management
  - `ScopusDataQualityFilter` - Data filtering and CrossRef recovery
  - `CrossRefClient` - API client with rate limiting and polite pool
  - `DatabaseValidator` - Integrity checking against source CSV

- **Configuration**:
  - `config.json` - CrossRef email and processing options
  - `scopus_db/config_loader.py` - Configuration management

### Data Flow

**Step 1: Database Creation**
```
CSV Files → Quality Filter → Database Creator → SQLite DB
     ↓            ↓                               ↓
   Validation  Exclusion Logs              Analytics Tables
```

**Step 2: Optional Enrichment**
```
SQLite DB → CrossRef Recovery → Enhanced SQLite DB
     ↓              ↓                       ↓
Existing Data   Recovery Stats        Updated Records
```

### Output Structure
```
data/export_1/
├── raw_scopus/          # Original CSV files
├── output/              # All generated reports
│   ├── *.json/txt/html  # Quality reports
│   └── *_missing_doi.*  # DOI troubleshooting
└── *.db                 # Final database
```

## Configuration

### CrossRef Setup
Create `config.json` for DOI recovery:
```json
{
  "crossref": {
    "enabled": true,
    "email": "researcher@university.edu"
  }
}
```

### Testing Considerations
- Tests use real-world sample data in `tests/test_data/`
- CrossRef tests may make actual API calls (use sparingly)
- Database tests validate schema and data integrity

## Important Notes

- Always run quality filtering (enabled by default)
- CrossRef recovery requires user email and confirmation
- Database files use timestamps to prevent overwrites
- Recovery metadata preserved in `_recovery_method` and `_recovery_confidence` fields
- Processing speed: ~1000-5000 records/second (excluding API calls)
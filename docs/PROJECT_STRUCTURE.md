# Project Structure

This document describes the structure of the Scopus Database project, which follows Python packaging best practices.

## Directory Layout

```
scopus-db/
├── scopus_db/                 # Main package directory
│   ├── __init__.py           # Package initialization
│   ├── cli.py                # Command-line interface
│   ├── database/             # Database module
│   │   ├── __init__.py
│   │   └── creator.py        # Main database creation logic
│   ├── parsers/              # Data parsing utilities (future)
│   │   └── __init__.py
│   └── utils/                # Utility functions (future)
│       └── __init__.py
├── tests/                    # Test suite
│   ├── __init__.py
│   ├── test_database_integrity.py
│   └── test_data/            # Test data directory
├── scripts/                  # Standalone utility scripts
│   └── investigate_keyword_mismatch.py
├── docs/                     # Documentation
│   └── examples/             # Usage examples
├── data/                     # Data files
│   └── scopus_exports/       # Scopus CSV exports
├── .github/                  # GitHub specific files
│   └── workflows/            # CI/CD workflows (future)
├── setup.py                  # Package setup configuration
├── pyproject.toml            # Modern Python project config
├── requirements.txt          # Production dependencies
├── requirements-dev.txt      # Development dependencies
├── .gitignore               # Git ignore patterns
├── LICENSE                  # MIT License
├── README.md                # Project documentation
├── CHANGELOG.md             # Version history
├── CLAUDE.md                # Claude-specific documentation
├── MANIFEST.in              # Package manifest
└── create_database.py       # Example usage script
```

## Package Installation

### Development Mode
```bash
pip install -e .
```

### Production Mode
```bash
pip install .
```

### With Development Dependencies
```bash
pip install -e ".[dev]"
```

## Usage

### As a Package
```python
from scopus_db import OptimalScopusDatabase

db_creator = OptimalScopusDatabase("path/to/scopus.csv")
db_creator.create_optimal_schema()
db_creator.process_csv_to_optimal_db()
db_creator.conn.close()
```

### Command Line
```bash
# After installation
scopus-db data/scopus_exports/export_1/scopus.csv

# Or using the example script
python create_database.py data/scopus_exports/export_1/scopus.csv
```

## Testing

Run tests from the project root:
```bash
python -m pytest tests/
```

## Development

1. Install development dependencies:
   ```bash
   pip install -r requirements-dev.txt
   ```

2. Run code formatting:
   ```bash
   black scopus_db tests
   isort scopus_db tests
   ```

3. Run linting:
   ```bash
   flake8 scopus_db tests
   mypy scopus_db
   ```

## Future Enhancements

- Add more parser modules for specific data types
- Implement utility functions for common operations
- Add GitHub Actions for CI/CD
- Create comprehensive documentation with Sphinx
- Add more test coverage
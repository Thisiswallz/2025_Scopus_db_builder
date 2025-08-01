# Scopus Database Builder

A professional Python package for creating and validating optimized SQLite databases from Scopus CSV exports. Provides both CLI tools and programmatic API for seamless integration into research projects.

## Features

- **3-Step Sequential Workflow**: Separated database creation, CrossRef enrichment, and Lens enrichment
- **Fast Database Creation**: Step 1 creates optimized databases in ~30 seconds (offline, no API calls)
- **Optional CrossRef Enrichment**: Step 2 recovers missing DOIs with multi-phase recovery pipeline
- **Future Lens Integration**: Step 3 will add patent citations and enhanced institution data
- **Pure Python**: No external dependencies for core database creation
- **Research-Ready**: Optimized SQLite schema with proper relationships and indexing
- **Comprehensive Data Quality**: Built-in filtering with detailed exclusion reports
- **Organized Output**: Structured folder system with raw data, outputs, and detailed reports
- **Performance Optimized**: Separated concerns allow fast offline processing vs. slower network tasks
- **GitHub Installation**: Easy installation directly from GitHub repository

## Installation

### Install from GitHub Repository

```bash
# Install the package directly from GitHub
pip install git+https://github.com/Thisiswallz/2025_Scopus_db_builder.git

# For private repositories (if using personal access token)
pip install git+https://your_username:your_token@github.com/Thisiswallz/2025_Scopus_db_builder.git

# Install specific version or branch
pip install git+https://github.com/Thisiswallz/2025_Scopus_db_builder.git@main
```

### For Development

```bash
# Clone and install in development mode
git clone https://github.com/Thisiswallz/2025_Scopus_db_builder.git
cd 2025_Scopus_db_builder
pip install -e .
```

## Quick Start

### 3-Step Workflow

The Scopus Database Builder uses a 3-step sequential workflow for maximum flexibility and performance:

```bash
# Step 1: Create optimized database (fast, offline)
python 1_create_database.py data/my_scopus_export.csv

# Step 2: Enrich with CrossRef DOI recovery (optional, requires internet)
python 2_enrich_database.py path/to/created/database.db --email your@email.edu

# Step 3: Enrich with Lens patent/institution data (optional, future feature)
python 3_Lens_database.py path/to/database.db --api-key your_lens_key
```

### Why This Approach?
- **Step 1 is fast**: Database creation completes in ~30 seconds without network dependencies
- **Step 2 is optional**: Only run CrossRef enrichment if you need missing DOIs recovered
- **Step 3 is future**: Lens enrichment for patent citations and enhanced institution data

### Configuration Setup (Optional)

Create a `config.json` file to enable CrossRef DOI recovery:

```json
{
  "crossref": {
    "enabled": true,
    "email": "your.email@university.edu",
    "confidence_thresholds": {
      "phase1_pubmed": 0.8,
      "phase2a_journal": 0.75,
      "phase2b_title": 0.65
    }
  },
  "processing": {
    "enable_data_quality_filter": true,
    "folder_structure": "organized"
  }
}
```

### Python API Usage

```python
from scopus_db import ScopusDB

# Create database programmatically
db_path = ScopusDB.create_database("data/scopus_export.csv")
print(f"Database created: {db_path}")

# Validate database integrity
results = ScopusDB.validate_database(db_path, "data/scopus_export.csv")
if results['all_passed']:
    print("✅ All integrity tests passed!")
else:
    print(f"❌ {results['failed_tests']} tests failed")

# Get database information
info = ScopusDB.get_database_info(db_path)
print(f"Papers: {info['papers']}")
print(f"Authors: {info['authors']}")
print(f"File size: {info['file_size_mb']} MB")
```

## Step 2: CrossRef DOI Recovery

Optional enrichment step that recovers missing DOIs from existing databases:

```bash
# Run on database created by Step 1
python 2_enrich_database.py path/to/database.db --email your@university.edu
```

### Multi-Phase Recovery Pipeline
1. **Phase 1**: PubMed ID-based lookup (85-95% success rate)
2. **Phase 2a**: Journal metadata matching (40-60% success rate)
3. **Phase 2b**: Title fuzzy matching (20-40% success rate)

### Configuration
Enable CrossRef recovery in your `config.json`:
```json
{
  "crossref": {
    "enabled": true,
    "email": "your.email@university.edu"
  }
}
```

### Enrichment Reports
For papers where DOI recovery fails, enrichment generates:
- **Enrichment Report**: `enrichment_report_crossref_*.json` - Recovery statistics and analysis
- **Missing DOI Analysis**: `*_missing_doi_analysis.txt` - Troubleshooting recommendations

See `docs/MISSING_DOI_TROUBLESHOOTING.md` for detailed troubleshooting guide.

## Integration into Other Projects

### Step-by-Step Integration Guide

Follow these steps to add the Scopus Database Builder package to your existing Python project:

#### 1. **Install the Package**

In your project directory, install the package:

```bash
# Navigate to your project directory
cd /path/to/your/project

# Install from GitHub
pip install git+https://github.com/Thisiswallz/2025_Scopus_db_builder.git

# Or add to requirements.txt
echo "git+https://github.com/Thisiswallz/2025_Scopus_db_builder.git" >> requirements.txt
pip install -r requirements.txt
```

#### 2. **For Private Repositories**

If the repository is private, you'll need authentication:

```bash
# Option A: Using Personal Access Token
pip install git+https://your_username:ghp_your_token@github.com/Thisiswallz/2025_Scopus_db_builder.git

# Option B: Using SSH (if SSH keys configured)
pip install git+ssh://git@github.com/Thisiswallz/2025_Scopus_db_builder.git
```

#### 3. **Basic Integration Example**

Create a new Python file in your project:

```python
# my_scopus_processor.py
from scopus_db import ScopusDB
import os

def process_scopus_data(csv_file_path, output_dir="databases"):
    """Process Scopus CSV and create validated database."""
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Create database
    print(f"Creating database from: {csv_file_path}")
    db_path = ScopusDB.create_database(csv_file_path)
    
    # Validate the database
    print("Validating database integrity...")
    results = ScopusDB.validate_database(db_path, csv_file_path)
    
    if results['all_passed']:
        print(f"✅ Database created and validated: {db_path}")
        
        # Get database statistics
        info = ScopusDB.get_database_info(db_path)
        print(f"📊 Database contains:")
        print(f"   - Papers: {info['papers']:,}")
        print(f"   - Authors: {info['authors']:,}")
        print(f"   - Institutions: {info['institutions']:,}")
        print(f"   - Keywords: {info['keywords']:,}")
        print(f"   - File size: {info['file_size_mb']} MB")
        
        return db_path
    else:
        print(f"❌ Database validation failed:")
        print(f"   - Passed tests: {results['passed_tests']}")
        print(f"   - Failed tests: {results['failed_tests']}")
        return None

# Usage example
if __name__ == "__main__":
    db_path = process_scopus_data("data/my_scopus_export.csv")
    if db_path:
        print(f"Database ready for analysis: {db_path}")
```

#### 4. **Advanced Integration Example**

For more complex projects with error handling and logging:

```python
# advanced_scopus_integration.py
import logging
import sqlite3
from pathlib import Path
from typing import Optional, Dict, Any
from scopus_db import ScopusDB

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ScopusProcessor:
    """Advanced Scopus data processor with error handling and logging."""
    
    def __init__(self, base_output_dir: str = "scopus_databases"):
        self.base_output_dir = Path(base_output_dir)
        self.base_output_dir.mkdir(exist_ok=True)
        
    def create_and_validate_database(self, 
                                   csv_path: str, 
                                   custom_output_path: Optional[str] = None) -> Optional[str]:
        """Create and validate Scopus database with comprehensive error handling."""
        
        csv_path = Path(csv_path)
        if not csv_path.exists():
            logger.error(f"CSV file not found: {csv_path}")
            return None
            
        try:
            # Create database
            logger.info(f"Processing Scopus CSV: {csv_path.name}")
            
            if custom_output_path:
                db_path = ScopusDB.create_database(str(csv_path), custom_output_path)
            else:
                db_path = ScopusDB.create_database(str(csv_path))
            
            logger.info(f"Database created: {db_path}")
            
            # Validate database
            logger.info("Running integrity validation...")
            results = ScopusDB.validate_database(db_path, str(csv_path))
            
            if results['all_passed']:
                logger.info("✅ All validation tests passed")
                self._log_database_stats(db_path)
                return db_path
            else:
                logger.warning(f"⚠️ {results['failed_tests']} validation tests failed")
                self._log_validation_failures(results)
                return db_path  # Return even if some tests failed
                
        except Exception as e:
            logger.error(f"Failed to process Scopus data: {str(e)}")
            return None
    
    def _log_database_stats(self, db_path: str) -> None:
        """Log database statistics."""
        try:
            info = ScopusDB.get_database_info(db_path)
            logger.info("📊 Database Statistics:")
            logger.info(f"   Papers: {info['papers']:,}")
            logger.info(f"   Authors: {info['authors']:,}")
            logger.info(f"   Institutions: {info['institutions']:,}")
            logger.info(f"   Keywords: {info['keywords']:,}")
            logger.info(f"   File size: {info['file_size_mb']} MB")
        except Exception as e:
            logger.warning(f"Could not retrieve database stats: {e}")
    
    def _log_validation_failures(self, results: Dict[str, Any]) -> None:
        """Log detailed validation failure information."""
        for test_result in results.get('test_results', []):
            if not test_result['passed']:
                logger.warning(f"❌ {test_result['name']}: {test_result.get('details', 'No details')}")

    def query_database(self, db_path: str, query: str) -> list:
        """Execute SQL query on the database."""
        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(query)
            results = [dict(row) for row in cursor.fetchall()]
            conn.close()
            return results
        except Exception as e:
            logger.error(f"Query failed: {e}")
            return []

# Usage example
if __name__ == "__main__":
    processor = ScopusProcessor()
    
    # Process multiple CSV files
    csv_files = ["data/scopus_2023.csv", "data/scopus_2024.csv"]
    
    for csv_file in csv_files:
        db_path = processor.create_and_validate_database(csv_file)
        if db_path:
            # Example query
            papers = processor.query_database(db_path, 
                "SELECT title, year FROM papers WHERE year >= 2023 LIMIT 5")
            print(f"Sample papers from {csv_file}:")
            for paper in papers:
                print(f"  - {paper['title']} ({paper['year']})")
```

#### 5. **Virtual Environment Setup**

For isolated project environments:

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On macOS/Linux:
source venv/bin/activate
# On Windows:
venv\Scripts\activate

# Install the package
pip install git+https://github.com/Thisiswallz/2025_Scopus_db_builder.git

# Save dependencies
pip freeze > requirements.txt
```

#### 6. **Requirements.txt Integration**

Add to your `requirements.txt`:

```txt
# requirements.txt
git+https://github.com/Thisiswallz/2025_Scopus_db_builder.git

# Or for specific version
git+https://github.com/Thisiswallz/2025_Scopus_db_builder.git@v0.2.0

# Other dependencies
pandas>=1.3.0
matplotlib>=3.5.0
jupyter>=1.0.0
```

#### 7. **Docker Integration**

For containerized projects:

```dockerfile
# Dockerfile
FROM python:3.9-slim

WORKDIR /app

# Install git (required for pip install from GitHub)
RUN apt-get update && apt-get install -y git

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy application code
COPY . .

CMD ["python", "my_scopus_processor.py"]
```

#### 8. **Project Structure Example**

```
your-research-project/
├── data/
│   ├── scopus_export_2023.csv
│   └── scopus_export_2024.csv
├── databases/
│   ├── scopus_2023.db
│   └── scopus_2024.db
├── analysis/
│   ├── __init__.py
│   └── scopus_analyzer.py
├── my_scopus_processor.py
├── requirements.txt
├── README.md
└── .gitignore
```

## Data Quality Filtering

The package includes built-in data quality filtering to ensure high-quality research data:

### **Default Behavior (Recommended)**
By default, the database creation process filters out low-quality records:

```bash
# CLI with filtering (default)
scopus-db create data/scopus_export.csv

# Python API with filtering (default)
from scopus_db import ScopusDB
db_path = ScopusDB.create_database("data/scopus_export.csv")
```

### **Filtering Criteria**
Records are excluded if they meet any of these conditions:
- **Missing essential fields**: No authors, title, or abstract
- **Editorial content**: Corrections, errata, editorial notes
- **Non-research content**: Conference announcements, table of contents
- **Incomplete data**: Missing publication dates or journal information

### **Exclusion Logging**
A detailed exclusion log is automatically generated:
- **Location**: Same directory as the database file
- **Format**: `data_quality_exclusion_log_YYYYMMDD_HHMMSS.json`
- **Contents**: Excluded records with specific rationales

### **Note on Filtering**
Data quality filtering is always enabled in the standard scripts to ensure research data integrity. This helps maintain high-quality datasets suitable for academic research and analysis.

## Database Output Structure

### Organized Folder Structure (NEW)

The system now creates an organized folder structure for better data management:

```
data/export_1/
├── raw_scopus/
│   └── scopus.csv                                      # Original Scopus data
├── output/
│   ├── data_quality_exclusions_YYYYMMDD_HHMMSS.json   # Exclusion details
│   ├── data_quality_exclusions_YYYYMMDD_HHMMSS.html   # Visual report
│   ├── data_quality_exclusions_YYYYMMDD_HHMMSS.txt    # Human-readable summary
│   ├── data_quality_exclusions_YYYYMMDD_HHMMSS.csv    # Excluded records CSV
│   ├── data_quality_exclusions_YYYYMMDD_HHMMSS_missing_doi.csv         # Missing DOI details
│   └── data_quality_exclusions_YYYYMMDD_HHMMSS_missing_doi_analysis.txt # DOI troubleshooting
└── export_1_research_optimized_YYYYMMDD_HHMMSS.db     # Generated database
```

### Legacy Structure (if organized folders disabled)

```
data/scopus_exports/export_1/
├── scopus.csv                                          # Original Scopus data
├── scopus_research_optimized_YYYYMMDD_HHMMSS.db       # Generated database
└── data_quality_exclusion_log_YYYYMMDD_HHMMSS.json    # Filtering log
```

### Database Statistics (5,000 paper dataset)
- **Papers**: 5,000 research articles with enhanced metrics
- **DOI Recovery**: 30-50% of missing DOIs automatically recovered
- **Authors**: 22,648 unique authors with career analytics  
- **Institutions**: 6,870 institutions with collaboration metrics
- **Keywords**: 32,297 keywords with TF-IDF scoring
- **File Size**: ~62 MB (40% reduction through optimization)
- **Reports**: Comprehensive data quality and missing DOI analysis

## Architecture Overview

### Zero-Dependency Architecture
The system maintains a zero-dependency approach using only Python's standard library. This design choice eliminates dependency management issues while providing sophisticated features like CrossRef integration.

### 3-Step Sequential Pipeline

**Step 1: Database Creation** (`1_create_database.py`)
1. **Entity Normalization**: Extract and normalize authors, institutions, keywords
2. **Complex Data Parsing**: Parse funding, citations, chemicals, trade names
3. **Relationship Optimization**: Build optimized relationships with TF-IDF scoring
4. **Analytics Pre-computation**: Generate collaboration networks and trend analyses

**Step 2: CrossRef Enrichment** (`2_enrich_database.py`)
1. **DOI Recovery**: Multi-phase recovery pipeline for missing identifiers
2. **Confidence Scoring**: Validate recovered DOI matches
3. **Database Updates**: Update existing database with recovered metadata

**Step 3: Lens Enrichment** (`3_Lens_database.py` - Future)
1. **Patent Citations**: Link papers to citing patents
2. **Institution Enhancement**: Enrich with detailed institution metadata

## Requirements

- **Python**: 3.6 or higher
- **Dependencies**: None (uses only Python standard library)
- **Storage**: Approximately 10-15 MB per 1,000 papers processed

## Contributing

This is a research tool for analyzing Scopus bibliographic data. Contributions welcome for:
- Additional analysis metrics
- New visualization capabilities  
- Enhanced data quality checks
- Research methodology improvements

## License

MIT License - See LICENSE file for details.
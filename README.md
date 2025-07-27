# Scopus Database Builder

A professional Python package for creating and validating optimized SQLite databases from Scopus CSV exports. Provides both CLI tools and programmatic API for seamless integration into research projects.

## Features

- **CLI Commands**: Simple command-line interface for database creation and validation
- **Python API**: Clean programmatic interface for external project integration
- **Database Validation**: Comprehensive integrity checking against original CSV data
- **Pure Python**: No external dependencies - uses only standard library
- **Research-Ready**: Optimized SQLite schema with proper relationships and indexing
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

### CLI Usage

After installation, use the `scopus-db` command globally:

```bash
# Create database from Scopus CSV export
scopus-db create data/my_scopus_export.csv

# Validate database integrity against original CSV
scopus-db check my_database.db --csv-file data/my_scopus_export.csv

# Get help
scopus-db --help
scopus-db create --help
scopus-db check --help
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

## Output Structure

Each parsing run creates timestamped output folders:

```
output/
├── YYYYMMDD_HHMMSS_filename_authors/               # Author parsing results
│   ├── parsed_authors_YYYYMMDD_HHMMSS.csv          # Normalized author data (CSV)
│   ├── parsed_authors_YYYYMMDD_HHMMSS.json         # Normalized author data (JSON)
│   ├── parsing_stats_YYYYMMDD_HHMMSS.txt           # Parsing statistics
│   └── parsing_errors_YYYYMMDD_HHMMSS.txt          # Error report
└── YYYYMMDD_HHMMSS_filename_data/                  # Complete data parsing results
    ├── keywords_YYYYMMDD_HHMMSS.csv                # Author & Index keywords
    ├── funding_YYYYMMDD_HHMMSS.csv                 # Grant funding details
    ├── references_YYYYMMDD_HHMMSS.csv              # Citation references
    ├── chemicals_YYYYMMDD_HHMMSS.csv               # Chemical/CAS data
    ├── affiliations_YYYYMMDD_HHMMSS.csv            # Institutional affiliations
    ├── trade_names_YYYYMMDD_HHMMSS.csv             # Commercial products
    ├── correspondence_YYYYMMDD_HHMMSS.csv          # Author contact info
    ├── open_access_YYYYMMDD_HHMMSS.csv             # Publication access types
    ├── [dataset]_YYYYMMDD_HHMMSS.json              # JSON versions of all datasets
    └── parsing_stats_YYYYMMDD_HHMMSS.txt           # Comprehensive statistics

data/scopus_exports/export_1/
├── scopus.csv                                       # Original Scopus data
└── scopus_research_db_YYYYMMDD_HHMMSS.db           # Generated SQLite database
```

## Requirements

- Python 3.6+
- No external dependencies (uses only standard library)

## Project Structure

```
├── parse_scopus_authors.py              # Author data parsing script (for standard workflow)
├── parse_scopus_data.py                 # Comprehensive data parsing script (for standard workflow)  
├── create_scopus_database.py            # Standard SQLite database creation script
├── create_optimal_scopus_database.py    # ⭐ Optimal database creation script (RECOMMENDED)
├── data/                                # Sample datasets
│   └── scopus_exports/
│       └── export_1/
│           ├── scopus.csv               # Original Scopus data
│           └── *_research_optimized.db  # Generated optimal SQLite database
├── output/                              # Parsing results (for standard workflow only)
├── README.md                            # This file
└── requirements.txt                     # Python dependencies
```

## Database Capabilities

### Author Data Parsing
- **Positional Parsing**: Correctly maps authors across multiple columns using positional correspondence
- **Scopus ID Extraction**: Extracts author IDs from parentheses in full names
- **Affiliation Processing**: Parses complex institutional affiliation text
- **Normalized Output**: Creates one record per author with paper metadata
- **Error Handling**: Robust handling of missing/malformed data with detailed error reporting
- **Multiple Formats**: Exports data in both CSV and JSON formats

### Comprehensive Data Parsing
- **Keywords**: Author-chosen and database-assigned terms for enhanced topic modeling
- **Funding Details**: Grant agencies, numbers, and structured funding information
- **References**: Citation network construction with 316K+ individual citations parsed
- **Chemicals/CAS**: Chemical substances and CAS registry numbers for materials research
- **Affiliations**: Institutional relationships and geographic analysis
- **Commercial Data**: Trade names, manufacturers, and technology transfer research
- **Correspondence**: Author contact information and institutional networks
- **Open Access**: Publication access types and availability analysis

### Standard SQLite Database Creation
- **Relational Schema**: 15+ tables with foreign key relationships for data integrity
- **Basic Indexes**: Standard indexing for common queries
- **Simple Views**: Basic analysis views for collaboration and co-occurrence
- **Requires Pre-parsing**: Needs separate author and data parsing scripts

### Optimal SQLite Database Creation ⭐ **RECOMMENDED**
- **Three-Phase Architecture**: Entity normalization → Relationship optimization → Analytics pre-computation
- **Research-Optimized Schema**: Master tables for authors, institutions, keywords with enhanced metrics
- **Advanced Analytics**: Pre-computed collaboration networks, TF-IDF scores, temporal trends
- **Performance Indexes**: Composite indexes optimized for research query patterns
- **Single-Step Process**: Direct CSV-to-database conversion with all analytics included
- **Auto-Location**: Database automatically placed in same directory as original CSV file
- **Complete Metrics**: H-index calculations, citation velocities, collaboration strengths
- **Analysis Views**: 4+ pre-built views for immediate research queries

**Optimal Database Features:**
- **79K+ Collaboration Relationships** with strength metrics and temporal analysis
- **290K+ Keyword Co-occurrences** for semantic network analysis  
- **22K+ Normalized Authors** with research metrics and career timelines
- **32K+ TF-IDF Keyword Scores** for enhanced topic modeling
- **6K+ Institution Networks** with geographic and collaboration mapping

### Research Applications
- **Topic Modeling**: Enhanced with normalized keywords and TF-IDF scoring
- **Citation Networks**: Structured reference data for impact analysis
- **Collaboration Analysis**: Author co-occurrence and institutional partnerships  
- **Funding Research**: Grant agency patterns and geographic distribution

## Database Output

### Standard Database
**File**: `scopus_research_db_YYYYMMDD_HHMMSS.db` (~178 MB)
- Requires 3-step parsing process
- Basic relational structure
- Standard analysis capabilities

### Optimal Database ⭐ **RECOMMENDED**
**File**: `scopus_research_optimized_YYYYMMDD_HHMMSS.db` (~62 MB)

**Import Statistics**:
- **Papers**: 5,000 records with enhanced research metrics
- **Authors**: 22,648 normalized unique authors with career analytics
- **Institutions**: 6,870 normalized institutions with collaboration metrics
- **Keywords**: 32,297 unique keywords with TF-IDF scoring  
- **Collaboration Networks**: 79,687 pre-computed author relationships
- **Keyword Co-occurrences**: 290,367 semantic relationships
- **Temporal Trends**: 70,631 time-series records for trend analysis

**Performance Benefits**:
- **40% smaller file size** through optimized normalization
- **Pre-computed analytics** eliminate complex JOIN operations
- **Composite indexes** for sub-second query performance
- **Research-ready views** for immediate analysis

## Usage Examples

### Complete Database Generation
```bash
# Generate database from Scopus CSV file
python3 parse_scopus_authors.py "data/my_scopus_export.csv"
python3 parse_scopus_data.py "data/my_scopus_export.csv"  
python3 create_scopus_database.py "output/YYYYMMDD_HHMMSS_my_scopus_export_data/" "data/my_scopus_export.csv"

# Example: Complete workflow with sample data
python3 parse_scopus_authors.py "data/scopus_exports/export_1/scopus.csv"
python3 parse_scopus_data.py "data/scopus_exports/export_1/scopus.csv"
python3 create_scopus_database.py "output/20250727_163911_scopus_data/" "data/scopus_exports/export_1/scopus.csv"
```

### Author Parsing Results
The author parsing script extracts individual author records:

**Input** (Scopus format):
```
Authors: "Wang H.; Bing Y.; Liu X.; Chang Z."
Author full names: "Wang, Haibo (57200947046); Bing, Yuanqiang (59763725000)..."
```

**Output** (Normalized format):
```csv
paper_id,author_position,author_abbreviated,author_full_name,scopus_id,affiliation_raw,primary_institution
2,1,Wang H.,"Wang, Haibo",57200947046,"College of Engineering, Ocean University...",College of Engineering
2,2,Bing Y.,"Bing, Yuanqiang",59763725000,"College of Engineering, Ocean University...",College of Engineering
```

### Optimal Database Creation & Querying ⭐ **RECOMMENDED**
Create research-optimized database with advanced analytics:

```bash
# Create optimal database (single step)
python3 create_optimal_scopus_database.py data/scopus_exports/export_1/scopus.csv

# Example queries using pre-computed analytics
DB_PATH="data/scopus_exports/export_1/scopus _research_optimized_*.db"

# Top collaborating authors with strength metrics
sqlite3 $DB_PATH "SELECT * FROM top_collaborators LIMIT 10;"

# Most productive authors with career metrics  
sqlite3 $DB_PATH "SELECT * FROM productive_authors LIMIT 10;"

# Trending keywords by TF-IDF scores
sqlite3 $DB_PATH "SELECT * FROM trending_keywords LIMIT 10;"

# Institution collaboration networks
sqlite3 $DB_PATH "SELECT * FROM institution_networks LIMIT 10;"

# Advanced collaboration analysis
sqlite3 $DB_PATH "
SELECT am1.full_name, am2.full_name, 
       ac.collaboration_count, ac.collaboration_strength,
       ac.first_collaboration_year, ac.latest_collaboration_year
FROM author_collaborations ac
JOIN authors_master am1 ON ac.author1_id = am1.author_id  
JOIN authors_master am2 ON ac.author2_id = am2.author_id
WHERE ac.collaboration_strength > 2.0
ORDER BY ac.collaboration_strength DESC;"
```

## Contributing

This is a research tool for analyzing Scopus bibliographic data. Contributions welcome for:
- Additional analysis metrics
- New visualization capabilities  
- Enhanced data quality checks
- Research methodology improvements

## License

MIT License - See LICENSE file for details.
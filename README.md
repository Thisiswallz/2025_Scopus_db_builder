# Scopus Database Generation Toolkit

A comprehensive Python toolkit for parsing Scopus bibliographic datasets and generating research-ready SQLite databases with complete relationship modeling and optimized indexing.

## Features

- **Author Data Parsing**: Extract and normalize author-paper relationships from complex semicolon-separated formats
- **Complete Data Parsing**: Parse all complex fields including keywords, funding, references, chemicals, and more
- **SQLite Database Creation**: Generate comprehensive relational databases with optimized indexing
- **Research-Ready Structure**: Pre-built analysis views for collaboration networks, keyword co-occurrence, funding statistics
- **Auto-Location**: Database automatically placed in same directory as source CSV
- **Complete Integration**: All parsed data imported with full relationship mapping

## Database Generation Workflow

### Standard Database (Basic Structure)
```bash
# Step 1: Parse author data into normalized format
python3 parse_scopus_authors.py path/to/your/scopus.csv

# Step 2: Parse all other data fields comprehensively  
python3 parse_scopus_data.py path/to/your/scopus.csv

# Step 3: Create comprehensive SQLite database (placed in same folder as CSV)
python3 create_scopus_database.py output/YYYYMMDD_HHMMSS_filename_data/ path/to/your/scopus.csv
```

### Optimal Database (Research-Optimized Structure) ⭐ **RECOMMENDED**
```bash
# Single-step optimal database creation with advanced analytics
python3 create_optimal_scopus_database.py path/to/your/scopus.csv

# Example with sample data
python3 create_optimal_scopus_database.py data/scopus_exports/export_1/scopus.csv
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
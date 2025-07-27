# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Purpose

This is a specialized Python toolkit for processing Scopus bibliographic datasets and generating research-optimized SQLite databases. The codebase focuses specifically on 3D printing and advanced manufacturing research data from Scopus exports.

## Core Commands

### Database Generation (Primary Use Case)
```bash
# Generate optimal research database (RECOMMENDED)
python3 create_optimal_scopus_database.py data/scopus_exports/export_1/scopus.csv

# Alternative paths for different datasets
python3 create_optimal_scopus_database.py "path/to/your/scopus.csv"
```

### Database Querying
```bash
# Basic database exploration
sqlite3 "data/scopus_exports/export_1/scopus_research_optimized_*.db" "SELECT COUNT(*) FROM papers;"

# Research analytics queries
sqlite3 "data/scopus_exports/export_1/scopus_research_optimized_*.db" "SELECT * FROM top_collaborators LIMIT 10;"
sqlite3 "data/scopus_exports/export_1/scopus_research_optimized_*.db" "SELECT * FROM trending_keywords LIMIT 10;"
```

### File Handling Notes
- CSV files may contain spaces in filenames (e.g., "scopus .csv")
- Always use quotes when passing file paths as arguments
- Database files are auto-generated with timestamps in the same directory as source CSV

## Architecture Overview

### Single-File Architecture
The codebase is intentionally minimal with one primary script (`create_optimal_scopus_database.py`) that handles the complete pipeline. This design choice eliminates dependency management and simplifies deployment in research environments.

### Three-Phase Processing Architecture

**Phase 1: Entity Normalization**
- Extracts and normalizes authors, institutions, and keywords
- Creates master tables with unique registries to prevent duplicates
- Handles complex Scopus field formats (semicolon-separated values, nested data)

**Phase 1.5: Complex Data Parsing**
- Parses funding information with grant number extraction
- Processes citation references with year detection
- Extracts chemical substances and CAS numbers
- Handles trade names and manufacturer information
- Processes correspondence and open access data

**Phase 2: Relationship Optimization**
- Builds paper-author, paper-keyword, and paper-institution relationships
- Computes TF-IDF scores for enhanced keyword analysis
- Creates optimized indexes for research query patterns

**Phase 3: Analytics Pre-computation**
- Generates collaboration networks with strength metrics
- Computes keyword co-occurrence matrices
- Creates temporal trend analyses
- Updates author research metrics (h-index, citation velocity)

### Database Schema Design

**Master Tables (Normalized Entities)**
- `authors_master`: Unique authors with research metrics
- `institutions_master`: Normalized institutions with collaboration data
- `keywords_master`: Keywords with TF-IDF scoring
- `papers`: Enhanced paper metadata with citation velocities

**Relationship Tables (Optimized for Queries)**
- `paper_authors`: Author-paper relationships with positional data
- `paper_keywords`: Keyword-paper relationships with TF-IDF scores
- `paper_institutions`: Institution-paper relationships

**Analytics Tables (Pre-computed for Performance)**
- `author_collaborations`: Pre-computed collaboration networks
- `keyword_cooccurrence`: Semantic relationship matrices
- `temporal_trends`: Time-series data for trend analysis

**Additional Data Tables**
- `paper_funding`: Grant agencies and funding information
- `paper_citations`: Bibliography and reference data
- `paper_chemicals`: Chemical substances and CAS numbers
- `paper_trade_names`: Commercial products and manufacturers
- `paper_correspondence`: Author contact information
- `paper_open_access`: Publication access types

## Data Processing Specifics

### Scopus CSV Field Handling
The toolkit processes complex Scopus fields that contain semicolon-separated values:
- **Authors**: "Wang H.; Bing Y.; Liu X.; Chang Z."
- **Author IDs**: "57200947046; 59763725000; 59244986600; 7202170486"
- **Keywords**: Both "Author Keywords" and "Index Keywords"
- **Affiliations**: Complex institutional text requiring extraction
- **Funding**: Multi-agency funding with grant numbers
- **References**: Citation text requiring year extraction

### Data Quality Considerations
- Handles BOM characters in CSV headers (common in Scopus exports)
- Manages misaligned author data across multiple columns
- Processes incomplete or missing data gracefully
- Performs data normalization to reduce redundancy

### Performance Optimizations
- Uses entity registries to prevent duplicate processing
- Creates composite indexes optimized for research queries
- Pre-computes collaboration networks to avoid expensive JOINs
- Generates materialized views for immediate analysis

## Research Domain Context

This toolkit is specifically designed for **3D printing and advanced manufacturing research**. The sample query demonstrates the focus area:

```
TITLE-ABS-KEY("3D printing" OR "additive manufacturing" OR "three dimensional printing") 
AND TITLE-ABS-KEY("advanced manufacturing" OR "industry 4.0" OR "digital manufacturing" OR "precision manufacturing" OR "robotics" OR "automation")
```

Understanding this domain context helps when:
- Interpreting keyword analysis results
- Understanding collaboration patterns
- Analyzing funding agency distributions
- Processing chemical/materials data

## Output Specifications

### Database File Output
- **Standard Output**: ~178 MB basic relational database
- **Optimal Output**: ~119 MB research-optimized database (40% size reduction)
- **Location**: Automatically placed in same directory as source CSV
- **Naming**: `{csv_name}_research_optimized_{timestamp}.db`

### Expected Data Volumes (5,000 paper dataset)
- 22,648 unique authors with research metrics
- 32,297 unique keywords with TF-IDF scoring
- 6,870 unique institutions with collaboration metrics
- 79,687 pre-computed collaboration relationships
- 290,367 keyword co-occurrence relationships
- 316,976 citation references
- 12,447 funding records

## Development Dependencies

**Runtime Requirements**: Python 3.6+ with standard library only
**No External Dependencies**: The toolkit intentionally uses only standard library modules (csv, sqlite3, re, json, collections, datetime, pathlib) to ensure maximum compatibility in research environments.
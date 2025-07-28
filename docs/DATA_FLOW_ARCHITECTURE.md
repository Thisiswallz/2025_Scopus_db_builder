# Scopus Database Builder - Data Flow Architecture

## 📊 Complete Data Processing Pipeline

This document provides a comprehensive overview of how Scopus CSV data flows through the entire system to create optimized research databases, including all filtering, CrossRef recovery, and database creation processes.

---

## 🏗️ **System Architecture Overview**

```mermaid
graph TD
    A[📄 Scopus CSV Files] --> B[🔍 Data Quality Filter]
    B --> C[🔗 CrossRef Recovery]
    C --> D[🗄️ Database Creator]
    D --> E[📊 Research Database]
    
    B --> F[📋 Quality Reports]
    C --> G[📈 Recovery Statistics]
    D --> H[🔍 Analytics Tables]
```

---

## 📁 **File Structure and Responsibilities**

### **Entry Point**
- **`create_database.py`** - Main command-line interface
  - **Function**: `main()` - CLI argument parsing and workflow orchestration
  - **Function**: `find_scopus_csv_files()` - CSV file discovery
  - **Function**: `validate_csv_path()` - Input validation

### **Core Processing Modules**

#### **Data Quality & Filtering**
- **`scopus_db/data_quality_filter.py`** - Multi-phase quality control
  - **Class**: `ScopusDataQualityFilter`
  - **Function**: `filter_csv_data()` - Main filtering pipeline
  - **Function**: `should_exclude_record()` - Quality criteria evaluation
  - **Function**: `_attempt_crossref_recovery()` - Multi-phase DOI recovery coordinator

#### **CrossRef Integration**
- **`scopus_db/crossref/crossref_client.py`** - External API integration
  - **Class**: `CrossRefClient` - API client with polite pool compliance
  - **Function**: `search_by_pmid()` - Phase 1: PubMed ID recovery
  - **Function**: `search_by_journal_details()` - Phase 2a: Journal-based recovery
  - **Function**: `search_by_title()` - Phase 2b: Title-based recovery
  - **Function**: `calculate_match_confidence()` - Confidence scoring system

#### **Database Creation**
- **`scopus_db/database/creator.py`** - SQLite database generation
  - **Class**: `OptimalScopusDatabase` - Main database creator
  - **Function**: `create_database()` - Master database creation workflow
  - **Function**: `_create_tables()` - Database schema creation
  - **Function**: `_populate_tables()` - Data insertion and normalization

---

## 🔄 **Complete Data Flow Process**

### **Phase 1: Input Processing**

#### **1.1 CSV File Discovery**
```
📁 Input: Scopus CSV files or directory
    ↓
🔍 create_database.py::find_scopus_csv_files()
    ↓ 
📋 Output: List of validated CSV file paths
```

**Key Files:**
- Input: `*.csv` (Scopus export files)
- Script: `create_database.py`
- Function: `find_scopus_csv_files(directory_path: Path) -> list`

#### **1.2 Data Loading & Parsing**
```
📄 CSV Files
    ↓
🔍 OptimalScopusDatabase.__init__()
    ↓
📊 Raw CSV data in memory
```

**Process:**
- Creates timestamp-based database filename
- Initializes data quality filter
- Loads CSV data with UTF-8-BOM handling

---

### **Phase 2: Data Quality Control**

#### **2.1 Quality Filtering Pipeline**
```
📊 Raw CSV Data
    ↓
🔍 ScopusDataQualityFilter.filter_csv_data()
    ↓
✅ High-quality records + 📋 Exclusion reports
```

**Quality Criteria Checks:**
- **Function**: `should_exclude_record(row, row_index) -> Tuple[bool, str]`
- **Checks**:
  - Missing authors: `row.get('Authors', '').strip()`
  - Missing author IDs: `row.get('Author(s) ID', '').strip()`
  - Missing title: `row.get('Title', '').strip()`
  - Missing publication year: `row.get('Year', '').strip()`
  - Missing DOI: `row.get('DOI', '').strip()`
  - Missing affiliations: `row.get('Affiliations', '').strip()`
  - Missing abstract: `row.get('Abstract', '').strip()`

#### **2.2 Multi-Phase CrossRef DOI Recovery**

When records are missing DOIs, the system attempts recovery through multiple phases:

##### **Phase 1: PubMed ID Recovery (Highest Confidence ~95%)**
```
❌ Missing DOI Record
    ↓
🔍 ScopusDataQualityFilter._attempt_phase1_pubmed_recovery()
    ↓
🌐 CrossRefClient.search_by_pmid(pubmed_id)
    ↓
✅ DOI recovered (confidence: 0.8-1.0)
```

**Process:**
- **Input**: `row.get('PubMed ID', '').strip()`
- **API Call**: `GET https://api.crossref.org/works?filter=pmid:{pubmed_id}`
- **Validation**: `validate_publication_match()` with 0.8 confidence threshold
- **Output**: Enhanced record with DOI + recovery metadata

##### **Phase 2a: Journal-Based Recovery (High Confidence ~85%)**
```
❌ No PubMed ID available
    ↓
🔍 ScopusDataQualityFilter._attempt_phase2a_journal_recovery()
    ↓
🌐 CrossRefClient.search_by_journal_details()
    ↓
✅ DOI recovered (confidence: 0.75-0.9)
```

**Process:**
- **Input Fields**:
  - `row.get('Source title', '')` - Journal name
  - `row.get('Volume', '')` - Volume number
  - `row.get('Issue', '')` - Issue number
  - `row.get('Year', '')` - Publication year
  - `row.get('Page start', '')` + `row.get('Page end', '')` - Pages
- **API Call**: Complex journal search with publication details
- **Validation**: `validate_publication_match()` with 0.75 confidence threshold

##### **Phase 2b: Title-Based Recovery (Moderate Confidence ~70%)**
```
❌ Insufficient journal details
    ↓
🔍 ScopusDataQualityFilter._attempt_phase2b_title_recovery()
    ↓
🌐 CrossRefClient.search_by_title()
    ↓
✅ DOI recovered (confidence: 0.65-0.8)
```

**Process:**
- **Input Fields**:
  - `row.get('Title', '')` - Article title (minimum 10 characters)
  - `row.get('Authors', '')` - First author extraction
  - `row.get('Year', '')` - Publication year
- **API Call**: Title-based search with author/year filters
- **Validation**: `validate_publication_match()` with 0.65 confidence threshold

#### **2.3 Confidence Scoring System**
```
🌐 CrossRef API Response
    ↓
🎯 CrossRefClient.calculate_match_confidence()
    ↓
📊 Multi-factor confidence score (0.0-1.0)
```

**Confidence Factors:**
- **Year Match**: Exact year comparison (weight: 0.3)
- **Title Similarity**: Fuzzy string matching (weight: 0.4)
- **Author Overlap**: Name matching analysis (weight: 0.2)
- **Publication Details**: Journal/volume/issue matching (weight: 0.1)

**Confidence Thresholds:**
- **Phase 1 (PubMed)**: ≥ 0.8 (high confidence required)
- **Phase 2a (Journal)**: ≥ 0.75 (moderate-high confidence)
- **Phase 2b (Title)**: ≥ 0.65 (moderate confidence acceptable)

---

### **Phase 3: Database Creation**

#### **3.1 Schema Creation**
```
✅ Filtered & Enhanced Data
    ↓
🗄️ OptimalScopusDatabase._create_tables()
    ↓
📊 SQLite schema with research-optimized structure
```

**Database Schema:**

**Master Tables:**
- `papers` - Enhanced paper metadata with recovery attribution
- `authors_master` - Unique authors with research metrics
- `institutions_master` - Normalized institutions
- `keywords_master` - Keywords with TF-IDF scoring

**Relationship Tables:**
- `paper_authors` - Author-paper relationships
- `paper_keywords` - Keyword-paper relationships  
- `paper_institutions` - Institution-paper relationships

**CrossRef Recovery Tables:**
- Papers include recovery metadata:
  - `_recovery_method` - Phase used for DOI recovery
  - `_recovery_confidence` - Confidence score

#### **3.2 Data Population**
```
📊 SQLite Schema
    ↓
🔄 OptimalScopusDatabase._populate_tables()
    ↓
🗄️ Fully populated research database
```

**Population Process:**
1. **Entity Normalization**: Creates unique registries for authors, keywords, institutions
2. **Relationship Building**: Links papers to entities with optimized indexing
3. **Analytics Pre-computation**: Generates collaboration networks and metrics
4. **Recovery Attribution**: Preserves CrossRef recovery information

#### **3.3 Output Generation**
```
🗄️ Complete Database
    ↓
📁 Multiple output formats
    ↓
📊 Ready for research analysis
```

**Generated Files:**
- **`{csv_name}_research_optimized_{timestamp}.db`** - Main SQLite database
- **`{timestamp}.json`** - Technical processing log
- **`{timestamp}.txt`** - Human-readable quality report
- **`{timestamp}.html`** - Interactive quality dashboard
- **`{timestamp}.csv`** - Excluded records with original data

---

## 📈 **Data Quality Metrics & Reporting**

### **Quality Statistics Tracking**
```python
# In ScopusDataQualityFilter.stats
{
    "total_records": int,
    "excluded_records": int, 
    "included_records": int,
    "exclusion_reasons": {
        "MISSING_DOI": count,
        "MISSING_AUTHORS": count,
        # ... other exclusion categories
    },
    "crossref_recovery_stats": {
        "attempted": int,
        "successful": int,
        "phase1_successful": int,  # PubMed recovery
        "phase2a_successful": int, # Journal recovery  
        "phase2b_successful": int, # Title recovery
        # ... detailed phase statistics
    }
}
```

### **Recovery Attribution**
Each successfully recovered record includes:
- **`_recovery_method`**: `"Phase1_PubMed"`, `"Phase2a_Journal"`, or `"Phase2b_Title"`
- **`_recovery_confidence`**: Numerical confidence score (0.0-1.0)

---

## 🔧 **API Rate Limiting & Compliance**

### **CrossRef Polite Pool Requirements**
- **Email Header**: All requests include researcher email in User-Agent
- **Query Parameter**: `mailto={email}` in all API calls
- **Rate Limiting**: Conservative 45 requests/second (2.22ms between calls)
- **Error Handling**: Exponential backoff for failed requests
- **Gzip Support**: Automatic response compression handling

### **Network Configuration**
```python
# CrossRef API Settings
BASE_URL = "https://api.crossref.org"
RATE_LIMIT = 45.0  # requests per second
USER_AGENT = f"ScopusDBBuilder/1.0 (mailto:{email})"
```

---

## 🎯 **Performance Characteristics**

### **Processing Speeds**
- **Quality Filtering**: ~1,000-5,000 records/second
- **CrossRef Recovery**: ~45 records/minute (API rate limited)
- **Database Creation**: ~500-1,000 records/second

### **Expected Recovery Rates**
- **Phase 1 (PubMed)**: 95% success when PubMed IDs available
- **Phase 2a (Journal)**: 40-60% success with complete journal metadata
- **Phase 2b (Title)**: 20-40% success depending on title uniqueness
- **Overall**: 30-50% recovery rate for missing DOIs

### **Database Optimization**
- **Size Reduction**: ~40% smaller than basic relational design
- **Query Performance**: Optimized indexes for research patterns
- **Memory Usage**: Efficient entity registries prevent duplicates

---

## 🚀 **Usage Examples**

### **Basic Usage**
```bash
# Single CSV file
python create_database.py data/scopus_export.csv

# Directory with multiple CSV files  
python create_database.py data/scopus_exports/
```

### **CrossRef Recovery Process**
```python
# Automatic workflow when missing DOIs detected
filter_obj = ScopusDataQualityFilter(
    enable_crossref_recovery=True,
    crossref_email="researcher@university.edu"
)

# Multi-phase recovery attempts
filtered_data, report = filter_obj.filter_csv_data(csv_data)
```

### **Database Querying**
```sql
-- Check recovery statistics
SELECT 
    _recovery_method,
    COUNT(*) as recovered_count,
    AVG(CAST(_recovery_confidence AS FLOAT)) as avg_confidence
FROM papers 
WHERE _recovery_method IS NOT NULL
GROUP BY _recovery_method;

-- Research queries with recovered data
SELECT * FROM papers 
WHERE doi IS NOT NULL 
  AND _recovery_method = 'Phase2a_Journal';
```

---

## 📚 **Additional Resources**

### **Testing & Validation**
- **`tests/test_real_world_sample.py`** - Production data validation
- **`tests/test_crossref_phase1.py`** - PubMed ID recovery testing  
- **`tests/test_confidence_scoring.py`** - Confidence system validation

### **Analysis Tools**
- **`scripts/analyze_real_data.py`** - Dataset quality analysis
- **`scripts/demo_crossref_recovery.py`** - Recovery feature demonstration

### **Configuration**
- **Zero Dependencies**: Uses only Python standard library
- **Email Required**: CrossRef recovery requires valid researcher email
- **User Confirmation**: Interactive prompts for API usage (can be skipped for automation)

---

*This architecture enables high-quality research databases with significant improvements in DOI coverage and data completeness through intelligent recovery and validation systems.*
# 📁 Data Folder Organization Guide

## 🏗️ **Organized Structure (Recommended)**

The Scopus Database Builder now supports a clean, organized folder structure that separates raw data, outputs, and database files:

```
data/export_1/
├── raw_scopus/           # 📄 Raw Scopus CSV files and documentation
│   ├── scopus (1).csv
│   ├── scopus (2).csv
│   └── Scopus_query.md
├── output/               # 📊 Generated reports and logs
│   ├── data_quality_exclusions_20250728_143522.json
│   ├── data_quality_exclusions_20250728_143522.html
│   ├── data_quality_exclusions_20250728_143522.txt
│   └── data_quality_exclusions_20250728_143522.csv
└── export_1_research_optimized_20250728_143522.db  # 🗄️ Main database
```

---

## 📋 **Folder Structure Details**

### **📄 `raw_scopus/` - Source Data**
- **Purpose**: Contains original Scopus CSV exports and related documentation
- **Contents**:
  - `*.csv` - Scopus export files
  - `*.md` - Query documentation and metadata
  - Original files remain untouched and safe

### **📊 `output/` - Generated Reports**
- **Purpose**: All generated reports, logs, and analysis files
- **Contents**:
  - `*.json` - Technical processing logs
  - `*.html` - Interactive quality dashboards
  - `*.txt` - Human-readable quality reports
  - `*.csv` - Excluded records exports

### **🗄️ Database File (Main Directory)**
- **Purpose**: The final SQLite database for research use
- **Location**: Placed in the main export directory for easy access
- **Format**: `{export_name}_research_optimized_{timestamp}.db`

---

## 🔄 **Migration from Legacy Structure**

If you have existing data in the old format:

### **Legacy Structure (Still Supported)**
```
data/export_1/
├── scopus (1).csv
├── scopus (2).csv
├── Scopus_query.md
├── export_1_research_optimized_20250728_143522.db
├── data_quality_exclusions_20250728_143522.json
├── data_quality_exclusions_20250728_143522.html
├── data_quality_exclusions_20250728_143522.txt
└── data_quality_exclusions_20250728_143522.csv
```

### **Migration Steps**
```bash
cd data/export_1

# Create organized structure
mkdir -p raw_scopus output

# Move raw Scopus files
mv *.csv *.md raw_scopus/

# Move output files (optional - they'll be regenerated)
mv data_quality_exclusions_* output/ 2>/dev/null || true

# Database files stay in main directory
# (*.db files remain where they are)
```

---

## 🚀 **Usage Examples**

### **With Organized Structure**
```bash
# Process directory - automatically detects organized structure
python create_database.py data/export_1/

# Output:
# 📁 Found organized structure - looking in raw_scopus/ folder
# ✅ Processing: data/export_1/raw_scopus/scopus (1).csv
# ✅ Processing: data/export_1/raw_scopus/scopus (2).csv
# 🗄️ Database: data/export_1/export_1_research_optimized_20250729_101530.db
# 📊 Reports: data/export_1/output/
```

### **With Legacy Structure**
```bash
# Still works with old structure
python create_database.py data/legacy_export/

# Output:
# ✅ Processing: data/legacy_export/scopus.csv  
# 🗄️ Database: data/legacy_export/scopus_research_optimized_20250729_101530.db
```

### **Single File Processing**
```bash
# Process single CSV from organized structure
python create_database.py data/export_1/raw_scopus/scopus.csv

# Output:
# 🗄️ Database: data/export_1/export_1_research_optimized_20250729_101530.db
# 📊 Reports: data/export_1/output/
```

---

## 📊 **File Output Locations**

### **Database Files** (Main Directory)
- **Single CSV**: `{csv_name}_research_optimized_{timestamp}.db`
- **Multiple CSVs**: `{export_name}_combined_research_optimized_{timestamp}.db`
- **Location**: Main export directory (e.g., `data/export_1/`)

### **Quality Reports** (`output/` Folder)
- **JSON Log**: `data_quality_exclusions_{timestamp}.json`
- **HTML Dashboard**: `data_quality_exclusions_{timestamp}.html`
- **Text Report**: `data_quality_exclusions_{timestamp}.txt`
- **Excluded Records**: `data_quality_exclusions_{timestamp}.csv`

### **CrossRef Recovery Reports** (`output/` Folder)
- **Recovery Log**: Detailed recovery statistics and confidence scores
- **Missing DOI Export**: Records that couldn't be recovered
- **Attribution Data**: Included in main database with `_recovery_method` fields

---

## 🎯 **Benefits of Organized Structure**

### **✅ Clean Separation**
- **Raw data**: Safe and untouched in `raw_scopus/`
- **Generated files**: Organized in `output/`
- **Final database**: Easy to find in main directory

### **✅ Version Control Friendly**
- Easier to exclude generated files from git
- Clear distinction between source and derived data
- Simpler backup strategies

### **✅ Research Workflow**
- **Analysts**: Go straight to `.db` file in main directory
- **Data managers**: Check quality reports in `output/`
- **Researchers**: Reference original queries in `raw_scopus/`

### **✅ Automation Ready**
- Predictable file locations for scripts
- Clear separation for processing pipelines
- Consistent structure across projects

---

## 🔧 **Configuration Options**

You can control output generation in `config.json`:

```json
{
  "output": {
    "generate_html_report": true,     // Interactive dashboard
    "generate_csv_export": true,      // Excluded records
    "generate_text_report": true,     // Human-readable summary
    "generate_json_log": true,        // Technical log
    "timestamp_files": true           // Add timestamps to filenames
  }
}
```

---

## 📋 **Recommended Workflow**

### **1. Initial Setup**
```bash
mkdir -p data/my_project/raw_scopus
# Place your Scopus CSV files in raw_scopus/
```

### **2. Database Creation**
```bash
python create_database.py data/my_project/
```

### **3. Results**
- **Database**: `data/my_project/my_project_research_optimized_*.db`
- **Reports**: `data/my_project/output/`
- **Raw Data**: Safe in `data/my_project/raw_scopus/`

### **4. Analysis**
```bash
# Open database for research
sqlite3 data/my_project/my_project_research_optimized_*.db

# Review quality reports
open data/my_project/output/*.html
```

---

## 🚨 **Troubleshooting**

### **"No CSV files found"**
- Check that CSV files are in the correct location
- If using organized structure, ensure files are in `raw_scopus/`
- Verify file extensions are `.csv` (not `.CSV` or other variations)

### **"Permission denied writing to output/"**
- Ensure the export directory is writable
- Run with appropriate permissions
- Check disk space availability

### **"Database file already exists"**
- Database files include timestamps to prevent conflicts
- Old database files are preserved (not overwritten)
- Clean up old files manually if needed

---

*This organized structure provides better data management while maintaining full backward compatibility with existing workflows.*
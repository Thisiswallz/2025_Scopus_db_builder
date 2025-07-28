# Configuration Guide

## 📋 **Overview**

The Scopus Database Builder uses a flexible configuration system that allows you to customize all aspects of the database creation process through a simple JSON file.

## 🚀 **Quick Setup**

### **Step 1: Create Configuration File**
```bash
# Copy the example config file
cp config.example.json config.json
```

### **Step 2: Add Your Email**
Edit `config.json` and replace:
```json
"email": "your.email@university.edu"
```
With your actual email address.

### **Step 3: Run Database Creation**
```bash
python create_database.py your_data.csv
```

The system will automatically use your configuration!

---

## 📁 **Configuration File Location**

The system looks for `config.json` in the project root directory:
```
Scopus DB Builder/
├── config.json          ← Your configuration file
├── config.example.json  ← Example/template
├── create_database.py
└── ...
```

---

## ⚙️ **Configuration Sections**

### 🔗 **CrossRef DOI Recovery**
```json
"crossref": {
  "enabled": true,                    // Enable/disable DOI recovery
  "email": "researcher@uni.edu",      // Your email (REQUIRED)
  "skip_confirmation": true,          // Skip interactive prompts
  "rate_limit_requests_per_second": 45,  // API rate limiting
  "confidence_thresholds": {
    "phase1_pubmed": 0.8,            // PubMed ID recovery threshold
    "phase2a_journal": 0.75,         // Journal search threshold  
    "phase2b_title": 0.65            // Title search threshold
  },
  "timeout_seconds": 30,             // API request timeout
  "retry_attempts": 3                // Failed request retries
}
```

**Key Settings:**
- **`enabled`**: Turn CrossRef recovery on/off
- **`email`**: **REQUIRED** - Your institutional email for API access
- **`skip_confirmation`**: `true` = automatic, `false` = ask permission
- **`confidence_thresholds`**: Higher = stricter matching (0.0-1.0)

### 🔍 **Data Quality Filtering**
```json
"data_quality": {
  "filtering_enabled": true,          // Enable quality filtering
  "generate_reports": true,           // Create quality reports
  "export_excluded_records": true,    // Export rejected records
  "quality_criteria": {
    "require_authors": true,          // Must have author names
    "require_author_ids": true,       // Must have Scopus author IDs
    "require_title": true,            // Must have paper title
    "require_year": true,             // Must have publication year
    "require_doi": false,             // DOI not required (recoverable)
    "require_affiliations": true,     // Must have institution info
    "require_abstract": true          // Must have abstract text
  }
}
```

**Customization Tips:**
- Set `require_doi: true` to exclude papers without DOIs
- Set `require_abstract: false` for conference papers without abstracts
- Adjust criteria based on your research needs

### 🗄️ **Database Creation**
```json
"database": {
  "include_analytics_tables": true,   // Pre-compute research metrics
  "create_indexes": true,             // Optimize query performance
  "normalize_entities": true,         // Deduplicate authors/institutions
  "compute_collaborations": true,     // Author collaboration networks
  "compute_keyword_cooccurrence": true, // Keyword relationships
  "include_recovery_metadata": true   // Track CrossRef recoveries
}
```

### 📊 **Output Generation**
```json
"output": {
  "generate_html_report": true,       // Interactive quality dashboard
  "generate_csv_export": true,        // Excluded records CSV
  "generate_text_report": true,       // Human-readable summary
  "generate_json_log": true,          // Technical processing log
  "verbose_logging": true,            // Detailed console output
  "timestamp_files": true             // Add timestamps to filenames
}
```

### ⚡ **Performance Settings**
```json
"performance": {
  "batch_size": 1000,                 // Records processed per batch
  "memory_limit_mb": 2048,            // Memory usage limit
  "parallel_processing": false,       // Multi-threading (experimental)
  "cache_api_responses": true         // Cache CrossRef responses
}
```

### 📁 **File Handling**
```json
"file_handling": {
  "encoding": "utf-8-sig",            // CSV file encoding (handles BOM)
  "skip_empty_rows": true,            // Ignore blank CSV rows
  "handle_malformed_csv": true,       // Try to fix CSV format issues
  "backup_original_files": false      // Create backup copies
}
```

---

## 🌍 **Environment Variable Overrides**

You can override config settings with environment variables:

```bash
# CrossRef settings
export CROSSREF_EMAIL="researcher@university.edu"
export CROSSREF_ENABLED="true"
export CROSSREF_SKIP_CONFIRMATION="true"

# Other settings
export DATA_FILTERING_ENABLED="true"
export VERBOSE_LOGGING="false"
export BATCH_SIZE="500"
export MEMORY_LIMIT_MB="4096"

# Run with overrides
python create_database.py data.csv
```

**Priority Order:**
1. **Environment variables** (highest priority)
2. **config.json file** 
3. **Default values** (lowest priority)

---

## 📋 **Configuration Examples**

### **Minimal Setup (Just Add Email)**
```json
{
  "crossref": {
    "enabled": true,
    "email": "researcher@university.edu"
  }
}
```

### **Conservative Settings (Strict Quality)**
```json
{
  "crossref": {
    "enabled": true,
    "email": "researcher@university.edu",
    "confidence_thresholds": {
      "phase1_pubmed": 0.9,
      "phase2a_journal": 0.85,
      "phase2b_title": 0.8
    }
  },
  "data_quality": {
    "quality_criteria": {
      "require_authors": true,
      "require_author_ids": true,
      "require_title": true,
      "require_year": true,
      "require_doi": true,
      "require_affiliations": true,
      "require_abstract": true
    }
  }
}
```

### **Fast Processing (Minimal Reports)**
```json
{
  "crossref": {
    "enabled": false
  },
  "database": {
    "include_analytics_tables": false,
    "compute_collaborations": false,
    "compute_keyword_cooccurrence": false
  },
  "output": {
    "generate_html_report": false,
    "generate_csv_export": false,
    "verbose_logging": false
  },
  "performance": {
    "batch_size": 5000
  }
}
```

### **Development/Testing Setup**
```json
{
  "crossref": {
    "enabled": true,
    "email": "test@example.com",
    "skip_confirmation": true,
    "rate_limit_requests_per_second": 10
  },
  "data_quality": {
    "quality_criteria": {
      "require_doi": false,
      "require_affiliations": false,
      "require_abstract": false
    }
  },
  "output": {
    "verbose_logging": true
  },
  "performance": {
    "batch_size": 100
  }
}
```

---

## 🔧 **Configuration Validation**

The system automatically validates your configuration:

### **✅ Valid Configuration**
```
✅ Configuration loaded from: /path/to/config.json
📋 CONFIGURATION SUMMARY
==================================================
🔗 CrossRef Recovery: ✅ Enabled
   📧 Email: researcher@university.edu
   🤖 Auto-confirm: true
   ⚡ Rate limit: 45 req/sec
🔍 Data Quality: ✅ Enabled
   📋 Required fields: authors, author_ids, title, year, affiliations, abstract
📊 Output formats: html report, csv export, text report, json log
⚡ Performance: Batch size=1000, Memory=2048MB
==================================================
```

### **❌ Invalid Configuration**
```
❌ ConfigurationError: CrossRef is enabled but no email provided
❌ ConfigurationError: Invalid email format: not-an-email
❌ ConfigurationError: Confidence threshold for phase1_pubmed must be between 0.0 and 1.0
```

---

## 💡 **Tips & Best Practices**

### **Email Setup**
- **Use institutional email** when possible (`@university.edu`)
- **Real email required** - CrossRef tracks usage and may contact you
- **Professional context** - avoid personal/throwaway emails

### **Performance Tuning**
- **Large datasets (>10K records)**: Increase `batch_size` to 5000+
- **Limited memory**: Decrease `memory_limit_mb` to 1024 or less  
- **Fast processing**: Disable analytics tables and reports
- **High quality needs**: Enable all quality criteria

### **Quality vs Quantity**
- **Strict filtering**: Higher confidence thresholds, more required fields
- **Inclusive datasets**: Lower thresholds, fewer required fields
- **Balance**: Default settings work well for most research

### **Output Management**
- **Research analysis**: Enable all reports and analytics
- **Production pipelines**: Enable only JSON logs for automation
- **Storage constraints**: Disable CSV exports and HTML reports

---

## 🚨 **Troubleshooting**

### **Common Issues**

**"Configuration file not found"**
```bash
# Copy the example file
cp config.example.json config.json
# Edit with your settings
```

**"Invalid JSON in config file"**
- Check for missing commas, quotes, or brackets
- Use a JSON validator online
- Compare with `config.example.json`

**"CrossRef is enabled but no email provided"**
- Add your email to `crossref.email` field
- Ensure `crossref.enabled` is `true`

**"Invalid email format"**
- Email must contain `@` and valid domain
- Use format: `name@domain.com`

---

## 📚 **Advanced Usage**

### **Multiple Configurations**
```bash
# Use different config for different projects
python create_database.py --config /path/to/project1/config.json data1.csv
python create_database.py --config /path/to/project2/config.json data2.csv
```

### **Configuration in Scripts**
```python
from scopus_db.config_loader import get_config, reload_config

# Load specific config
config = reload_config('/path/to/custom/config.json')

# Check settings
if config.is_crossref_enabled():
    print(f"Using email: {config.get_crossref_email()}")
```

### **Environment Variable Automation**
```bash
#!/bin/bash
# automated_processing.sh

export CROSSREF_EMAIL="auto@processor.com"
export CROSSREF_SKIP_CONFIRMATION="true"
export VERBOSE_LOGGING="false"

for file in data/*.csv; do
    python create_database.py "$file"
done
```

---

*This configuration system provides complete control over the Scopus Database Builder while maintaining simple setup for basic usage.*
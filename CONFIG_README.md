# 📋 Configuration Setup Guide

## 🚀 **Quick Start**

### **1. Set Up Your Email (Required for CrossRef)**
```bash
# Copy the example configuration
cp config.example.json config.json

# Edit config.json and replace this line:
"email": "your.email@university.edu"

# With your actual email:  
"email": "researcher@myuniversity.edu"
```

### **2. Run Database Creation**
```bash
python create_database.py your_scopus_data.csv
```

**That's it!** The system will automatically:
- ✅ Enable CrossRef DOI recovery using your email
- ✅ Use all default settings optimized for research
- ✅ Show configuration summary before processing

---

## 📧 **Email Requirements**

Your email should be:
- **Institutional email preferred** (`@university.edu`, `@research-institute.org`)
- **Valid format** with proper domain
- **Real email address** (CrossRef may contact for compliance)

**Good Examples:**
```
john.smith@stanford.edu
researcher@mit.edu  
data.scientist@company.com
```

---

## ⚙️ **Key Configuration Options**

### **CrossRef DOI Recovery**
```json
"crossref": {
  "enabled": true,                    // Turn on/off DOI recovery
  "email": "your.email@university.edu", // YOUR EMAIL HERE
  "skip_confirmation": true           // Auto-enable (no prompts)
}
```

### **Data Quality Controls**
```json
"data_quality": {
  "quality_criteria": {
    "require_authors": true,          // Must have author names
    "require_author_ids": true,       // Must have Scopus author IDs  
    "require_title": true,            // Must have paper title
    "require_doi": false,             // DOI not required (recoverable)
    "require_abstract": true          // Must have abstract
  }
}
```

### **Output Control**
```json
"output": {
  "generate_html_report": true,       // Interactive quality dashboard
  "generate_csv_export": true,        // Export excluded records
  "verbose_logging": true             // Detailed console output
}
```

---

## 🌍 **Alternative: Environment Variable**

Instead of config file, you can use environment variables:

```bash
# Set your email
export CROSSREF_EMAIL="researcher@university.edu"

# Run database creation  
python create_database.py data.csv
```

---

## 📊 **What You'll See**

When you run the script, you'll see a configuration summary:

```
📋 CONFIGURATION SUMMARY
==================================================
🔗 CrossRef Recovery: ✅ Enabled
   📧 Email: researcher@university.edu
   🤖 Auto-confirm: True
   ⚡ Rate limit: 45 req/sec
🔍 Data Quality: ✅ Enabled
   📋 Required fields: authors, author_ids, title, year, affiliations, abstract
📊 Output formats: html report, csv export, text report, json log
⚡ Performance: Batch size=1000, Memory=2048MB
==================================================
```

---

## 🔧 **Need Help?**

- **📖 Full guide**: See `docs/CONFIGURATION_GUIDE.md`
- **🛠️ Example config**: See `config.example.json`
- **❌ Errors**: The system will show clear error messages for invalid settings

---

**🎯 Bottom Line**: Just add your email to `config.json` and you're ready to go!
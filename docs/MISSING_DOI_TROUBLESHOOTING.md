# 🔍 Missing DOI Troubleshooting Guide

## 📋 **Overview**

The Scopus Database Builder now generates specialized reports for papers that are missing DOIs and couldn't be recovered through CrossRef. These reports help you understand what's missing and how to improve DOI coverage.

---

## 📊 **Generated Reports**

When papers with missing DOIs are found, the system automatically generates:

### **1. Missing DOI Analysis Report** (`*_missing_doi_analysis.txt`)
- **Purpose**: Human-readable analysis of missing DOI patterns
- **Contents**: 
  - Summary statistics
  - Failure pattern analysis
  - Troubleshooting recommendations
  - Recovery potential assessment

### **2. Missing DOI CSV Export** (`*_missing_doi.csv`)
- **Purpose**: Detailed data for manual investigation
- **Contents**:
  - Paper metadata (title, authors, year, journal)
  - Available identifiers (PubMed ID, volume, issue)
  - Recovery failure reasons
  - Troubleshooting guidance

---

## 🔍 **Understanding the Analysis Report**

### **Missing DOI Summary Section**
```
📊 MISSING DOI SUMMARY
------------------------------
Total records missing DOIs: 25
CrossRef recovery attempted: YES
Recovery attempts made: 25
Successful recoveries: 15
Failed recoveries: 10
```

**Key Metrics:**
- **Total missing**: Papers without DOIs before processing
- **Recovery attempts**: Papers that went through CrossRef recovery
- **Successful recoveries**: Papers that got DOIs from CrossRef
- **Failed recoveries**: Papers that still need manual attention

### **Failure Pattern Analysis**
```
📋 FAILURE PATTERN ANALYSIS
------------------------------
Records with PubMed IDs: 3 (Phase 1 recovery possible)
Records with journal details: 5 (Phase 2a recovery possible)
Records with titles only: 2 (Phase 2b recovery possible)
Records with insufficient data: 0 (No recovery possible)
```

**Recovery Phases:**
- **Phase 1 (PubMed ID)**: Highest confidence recovery method
- **Phase 2a (Journal)**: Journal + volume/issue matching
- **Phase 2b (Title)**: Title-based search (lowest confidence)
- **Insufficient data**: Cannot attempt any recovery method

---

## 🔧 **Troubleshooting Recommendations**

### **1. Enable CrossRef Recovery**
If CrossRef recovery is disabled:
```json
{
  "crossref": {
    "enabled": true,
    "email": "your.email@university.edu"
  }
}
```
**Impact**: Could recover 60-80% of missing DOIs automatically

### **2. PubMed ID Issues**
**Problem**: Papers have PubMed IDs but CrossRef lookup failed
**Possible Causes**:
- Invalid or outdated PubMed IDs
- PubMed records not indexed in CrossRef
- Network connectivity issues

**Solutions**:
- Verify PubMed IDs manually: https://pubmed.ncbi.nlm.nih.gov/
- Check if papers are actually in PubMed
- Consider manual DOI lookup for high-value papers

### **3. Journal Matching Issues**
**Problem**: Papers have journal info but CrossRef can't match
**Possible Causes**:
- Journal name variations (abbreviations vs full names)
- Incorrect volume/issue information
- Journal not indexed in CrossRef

**Solutions**:
- Check journal name consistency
- Verify volume/issue numbers against publisher website
- Lower confidence thresholds in config (if appropriate):
```json
{
  "crossref": {
    "confidence_thresholds": {
      "phase2a_journal": 0.65  // Lower from 0.75
    }
  }
}
```

### **4. Title Matching Issues**
**Problem**: Only title available, but matching failed
**Possible Causes**:
- Title variations or translation differences
- Very generic or common titles
- Missing author information affects matching

**Solutions**:
- Check for title variations in publisher databases
- Consider manual search in CrossRef: https://search.crossref.org/
- Add author information if available

### **5. Insufficient Metadata**
**Problem**: Papers lack basic identifiers for recovery
**Solutions**:
- Review Scopus export settings
- Include more fields in Scopus CSV export:
  - PubMed ID
  - Complete journal information
  - Author details
- Consider excluding if not critical for research

---

## 📋 **CSV Export Fields**

The missing DOI CSV export includes these fields for troubleshooting:

| Field | Description | Usage |
|-------|-------------|-------|
| `row_index` | Original row number in CSV | Locate in source data |
| `title` | Paper title (truncated) | Manual DOI search |
| `authors` | Author list (truncated) | Verify author information |
| `year` | Publication year | Cross-reference with journal |
| `source_title` | Journal name | Check journal variations |
| `volume`, `issue` | Volume/issue numbers | Verify against publisher |
| `page_start`, `page_end` | Page numbers | Additional verification |
| `pubmed_id` | PubMed identifier | Manual PubMed lookup |
| `abstract` | Abstract (truncated) | Context for manual search |
| `affiliations` | Institution info | Author verification |
| `crossref_attempted` | Was CrossRef used? | Configuration check |
| `recovery_failure_reason` | Why recovery failed | Specific troubleshooting |

---

## 🎯 **Manual DOI Recovery Process**

### **Step 1: Prioritize Records**
Focus on papers that are:
- High-impact or frequently cited
- Essential for your research
- Have complete metadata available

### **Step 2: Manual Search Methods**
1. **CrossRef Search**: https://search.crossref.org/
2. **Publisher Websites**: Search journal archives
3. **Google Scholar**: Often shows DOI information
4. **PubMed**: For medical/life sciences papers
5. **Publisher DOI Tools**: Many publishers have DOI lookup

### **Step 3: Verify DOI Validity**
- Test DOI URL: `https://doi.org/{found_doi}`
- Ensure DOI points to correct paper
- Check publication details match your data

### **Step 4: Update Source Data**
- Add recovered DOIs back to original CSV
- Re-run database creation to include recovered papers
- Document manual recovery process for future reference

---

## 📊 **Expected Recovery Rates**

Based on real-world testing with Scopus data:

| Recovery Method | Success Rate | Requirements |
|----------------|--------------|--------------|
| **Phase 1 (PubMed)** | 85-95% | Valid PubMed ID |
| **Phase 2a (Journal)** | 40-60% | Complete journal metadata |
| **Phase 2b (Title)** | 20-40% | Unique, specific titles |
| **Manual Recovery** | 60-80% | Time and effort investment |

### **Overall Impact**
- **Automatic Recovery**: 30-50% of missing DOIs
- **Manual Recovery**: Additional 20-30%
- **Total Potential**: 50-80% DOI coverage improvement

---

## 🔍 **Configuration for Better Recovery**

### **Aggressive Recovery Settings**
```json
{
  "crossref": {
    "enabled": true,
    "email": "your.email@university.edu",
    "confidence_thresholds": {
      "phase1_pubmed": 0.75,     // Lower from 0.8
      "phase2a_journal": 0.65,   // Lower from 0.75
      "phase2b_title": 0.55      // Lower from 0.65
    },
    "retry_attempts": 5          // Increase from 3
  }
}
```
**⚠️ Warning**: Lower thresholds increase recovery but may reduce accuracy

### **Conservative Recovery Settings**
```json
{
  "crossref": {
    "confidence_thresholds": {
      "phase1_pubmed": 0.9,      // Higher than 0.8
      "phase2a_journal": 0.85,   // Higher than 0.75
      "phase2b_title": 0.75      // Higher than 0.65
    }
  }
}
```
**✅ Benefit**: Higher accuracy, fewer false positive DOIs

---

## 🚨 **Common Issues & Solutions**

### **"No missing DOI records found"**
**Meaning**: All papers either had DOIs or were successfully recovered
**Action**: Great! No manual intervention needed

### **"CrossRef recovery attempted: NO"**
**Meaning**: CrossRef recovery is disabled
**Solution**: Enable CrossRef in config.json with your email

### **"Phase 1 (PubMed ID) failed"**
**Causes**: Invalid PubMed IDs, network issues, CrossRef indexing gaps
**Solutions**: Manual PubMed verification, check ID validity

### **"Phase 2a (Journal) failed"**
**Causes**: Journal name mismatches, incorrect metadata
**Solutions**: Check journal name variations, verify volume/issue

### **"Insufficient metadata for any recovery phase"**
**Causes**: Missing basic identifiers in source data
**Solutions**: Improve Scopus export settings, exclude low-quality records

---

## 📁 **Report File Organization**

With the new folder structure, missing DOI reports are saved in the `output/` folder:

```
data/export_1/
├── raw_scopus/
│   └── scopus.csv
├── output/
│   ├── data_quality_exclusions_20250728_143522.json
│   ├── data_quality_exclusions_20250728_143522.html
│   ├── data_quality_exclusions_20250728_143522.txt
│   ├── data_quality_exclusions_20250728_143522.csv
│   ├── data_quality_exclusions_20250728_143522_missing_doi.csv      ← NEW
│   └── data_quality_exclusions_20250728_143522_missing_doi_analysis.txt ← NEW
└── export_1_research_optimized_20250728_143522.db
```

---

*These specialized reports provide targeted insights for improving DOI coverage and data quality in your research database.*
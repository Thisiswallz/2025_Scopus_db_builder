# Lens Patent API Data Structure Analysis

## Example Patent: WO 2013/121230 A1
**Title**: "APPARATUS AND METHODS FOR THE PREPARATION OF REACTION VESSELS WITH A 3D-PRINTER"  
**Lens ID**: 091-428-408-796-283  
**Published**: 2013-08-22

---

## 🏗️ **Top-Level Structure**

The Lens API returns patents with these main fields:

```json
{
  "lens_id": "091-428-408-796-283",
  "jurisdiction": "WO",
  "doc_number": "2013121230", 
  "kind": "A1",
  "date_published": "2013-08-22",
  "doc_key": "WO_2013121230_A1_20130822",
  "docdb_id": 297423021,
  "lang": "en",
  "publication_type": "PATENT_APPLICATION",
  "biblio": {...},
  "families": {...},
  "legal_status": {...},
  "abstract": [...],
  "claims": [...],
  "description": [...],
  "references_cited": {...}
}
```

---

## 📚 **Citations Structure** (KEY FOR REVERSE ENGINEERING!)

The `references_cited` field contains both patent and paper citations:

```json
"references_cited": {
  "citations": [
    {
      "sequence": 1,
      "patcit": {
        "document_id": {"jurisdiction": "US", "doc_number": "..."},
        "lens_id": "patent-lens-id"
      }
    },
    {
      "sequence": 2,
      "nplcit": {
        "text": "GOVIND V. KAIGALA ET AL: \"Rapid prototyping of microfluidic devices with a wax printer\", LAB ON A CHIP...",
        "lens_id": "105-639-743-003-304", 
        "external_ids": ["10.1039/b617764f", "17330171"]
      }
    }
  ],
  "patent_count": 3,
  "npl_count": 1,
  "npl_resolved_count": 1
}
```

### 🎯 **Key Citation Types**:
- **`patcit`**: Patent citations (references to other patents)
- **`nplcit`**: Non-patent literature (papers, books, etc.) - **THIS IS WHAT WE WANT!**

### 📖 **NPL Citation Fields**:
- `text`: Full formatted citation text
- `lens_id`: Lens ID of the paper (if resolved)
- `external_ids`: DOI, PubMed ID, etc.

---

## 👥 **Author/Inventor Structure**

```json
"biblio": {
  "parties": {
    "inventors": [
      {
        "name": {"name": "CRONIN, LEROY"},
        "residence": "GB"
      }
    ],
    "applicants": [
      {
        "name": {"name": "UNIV GLASGOW"},
        "residence": "GB",
        "type": "UNIVERSITY"
      }
    ]
  }
}
```

---

## 🏛️ **Institution/Applicant Matching**

For institution-based matching, we can use:
- `biblio.parties.applicants[].name.name`
- `biblio.parties.applicants[].residence` 
- `biblio.parties.applicants[].type`

---

## 🔗 **Reverse Engineering Strategy**

### **Phase 1: Patent → Paper Discovery**
1. Query patents by institution: `applicant.name:"University of Glasgow"`
2. Extract `references_cited.citations[]` where `nplcit` exists
3. Match `nplcit.lens_id` or `nplcit.external_ids` to Scopus papers

### **Phase 2: Author Cross-Matching** 
1. Extract inventor names from `biblio.parties.inventors[].name.name`
2. Cross-reference with paper authors in Scopus
3. Use confidence scoring based on name similarity + institution match

### **Phase 3: Subject/Temporal Validation**
1. Compare patent `date_published` with paper publication dates
2. Analyze patent `abstract` vs paper `authkeywords`
3. Validate logical publication → patent timeline

---

## 📊 **Database Schema Requirements**

Based on this structure, our schema needs:

### **Patents Table**
```sql
CREATE TABLE patents (
    lens_id TEXT PRIMARY KEY,
    doc_key TEXT,
    doc_number TEXT,
    jurisdiction TEXT,
    kind TEXT,
    date_published TEXT,
    title TEXT,
    abstract TEXT,
    legal_status TEXT
);
```

### **Patent Citations Table**
```sql  
CREATE TABLE patent_citations (
    id INTEGER PRIMARY KEY,
    patent_lens_id TEXT,
    sequence INTEGER,
    citation_type TEXT, -- 'patcit' or 'nplcit'
    cited_lens_id TEXT, -- for resolved citations
    citation_text TEXT,
    external_ids TEXT,  -- JSON array of DOIs, PubMed IDs, etc.
    FOREIGN KEY (patent_lens_id) REFERENCES patents(lens_id)
);
```

### **Patent-Publication Links Table**
```sql
CREATE TABLE patent_publication_links (
    id INTEGER PRIMARY KEY,
    patent_lens_id TEXT,
    scopus_eid TEXT,
    link_type TEXT, -- 'author_match', 'citation_match', 'institution_match'
    confidence_score REAL,
    match_evidence TEXT, -- JSON with matching details
    FOREIGN KEY (patent_lens_id) REFERENCES patents(lens_id),
    FOREIGN KEY (scopus_eid) REFERENCES documents(eid)
);
```

---

## 🚀 **Implementation Priority**

1. **High Priority**: Extract and store `nplcit` citations with `lens_id` and `external_ids`
2. **Medium Priority**: Author/inventor name matching algorithms  
3. **Low Priority**: Patent family analysis for comprehensive coverage

---

## 💡 **Key Insights**

- **Lens resolves many paper citations**: The `npl_resolved_count` shows how many papers have been matched to Lens IDs
- **DOI matching is critical**: Many papers can be linked via `external_ids` containing DOIs  
- **Institution matching is reliable**: Applicant data is well-structured for matching
- **Timeline validation works**: Patents typically cite papers published 1-10 years earlier

This structure provides excellent opportunities for bidirectional linking between Scopus publications and Lens patents!
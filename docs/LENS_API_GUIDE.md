# Lens API Integration Guide

## Overview

The Lens API provides access to comprehensive patent data that can be linked to Scopus publications, enabling powerful analysis of the innovation pipeline from research to patents. This guide covers the API structure, data format, and integration patterns.

---

## 🔧 **API Configuration**

### Authentication
```bash
# Add to .env file
LENS_API=your_lens_api_token_here
```

### Basic Client Usage
```python
from scopus_db.lens.client import LensClient

# Initialize client
client = LensClient(
    api_token="your_token",
    rate_limit=10,  # requests per second
    cache_ttl_days=30
)

# Test connection
if client.test_connection():
    print("✅ Connected to Lens API")
```

---

## 📊 **Data Structure**

### Core Patent Fields
```json
{
  "lens_id": "091-428-408-796-283",
  "doc_key": "WO_2013121230_A1_20130822",
  "doc_number": "2013121230",
  "jurisdiction": "WO",
  "kind": "A1",
  "date_published": "2013-08-22",
  "publication_type": "PATENT_APPLICATION"
}
```

### Title and Abstract
```json
{
  "biblio": {
    "invention_title": [
      {
        "text": "APPARATUS AND METHODS FOR THE PREPARATION OF REACTION VESSELS WITH A 3D-PRINTER",
        "lang": "en"
      }
    ]
  },
  "abstract": [
    {
      "text": "Provided are methods for preparing and using reaction vessels...",
      "lang": "en"
    }
  ]
}
```

### Inventors and Applicants
```json
{
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
}
```

---

## 🔗 **Citation Structure (Key for Reverse Engineering)**

The most valuable part for linking patents to publications:

```json
{
  "references_cited": {
    "citations": [
      {
        "sequence": 1,
        "patcit": {
          "document_id": {
            "jurisdiction": "US",
            "doc_number": "20120135893"
          },
          "lens_id": "023-456-789-012-345"
        },
        "cited_phase": "ISR"
      },
      {
        "sequence": 2,
        "nplcit": {
          "text": "GOVIND V. KAIGALA ET AL: \"Rapid prototyping of microfluidic devices with a wax printer\", LAB ON A CHIP, vol. 7, no. 3, 10 January 2007 (2007-01-10), pages 384, XP055063874, ISSN: 1473-0197, DOI: 10.1039/b617764f",
          "lens_id": "105-639-743-003-304",
          "external_ids": [
            "10.1039/b617764f",
            "17330171"
          ]
        },
        "cited_phase": "ISR"
      }
    ],
    "patent_count": 3,
    "npl_count": 1,
    "npl_resolved_count": 1
  }
}
```

### Citation Types
- **`patcit`**: Patent citations (other patents)
- **`nplcit`**: Non-patent literature (**papers, books - what we want!**)

---

## 🔍 **Search Examples**

### 1. Find Patents by Institution
```python
# Search for University of Glasgow patents
response = client.search_patents(
    query='applicant.name:"UNIV GLASGOW"',
    size=50
)

print(f"Found {response.total} patents from University of Glasgow")
```

### 2. Find Patents by Inventor Name
```python
# Search for patents by specific inventor
response = client.search_patents(
    query='inventor.name:"CRONIN LEROY"',
    size=20
)
```

### 3. Find Patents in Date Range
```python
# Patents published in 2013
response = client.search_patents(
    query='date_published:[2013-01-01 TO 2013-12-31]',
    size=100
)
```

### 4. Complex Boolean Search
```python
# Patents from UK universities with "3D printing" in title
response = client.search_patents(
    query='applicant.residence:"GB" AND applicant.type:"UNIVERSITY" AND title:"3D printing"',
    size=25
)
```

---

## 🔄 **Enrichment Workflow**

### Phase 1: Institution-Based Discovery
```python
# 1. Extract institutions from Scopus database
institutions = ["University of Glasgow", "MIT", "Stanford University"]

# 2. Search patents for each institution
for institution in institutions:
    clean_name = clean_institution_name(institution)
    patents = client.search_patents(f'applicant.name:"{clean_name}"')
    
    # 3. Extract paper citations from each patent
    for patent in patents.results:
        citations = patent.get('references_cited', {}).get('citations', [])
        
        for citation in citations:
            if 'nplcit' in citation:
                # Found a paper citation!
                paper_info = citation['nplcit']
                lens_id = paper_info.get('lens_id')
                doi = extract_doi(paper_info.get('external_ids', []))
                
                # Link to Scopus via DOI or title matching
                if doi:
                    link_patent_to_scopus_by_doi(patent, doi)
```

### Phase 2: Author-Based Matching
```python
# 1. Get author names from Scopus publications  
authors = get_scopus_authors("University of Glasgow")

# 2. Search patents by inventor names
for author in authors:
    name_variants = generate_name_variants(author)
    
    for variant in name_variants:
        patents = client.search_patents(f'inventor.name:"{variant}"')
        
        # 3. Calculate confidence scores
        for patent in patents.results:
            confidence = calculate_match_confidence(
                scopus_publication=author.publication,
                patent_data=patent,
                match_type='author_match'
            )
            
            if confidence > 0.7:
                store_patent_link(author.publication, patent, confidence)
```

### Phase 3: Citation-Based Linking
```python
def extract_paper_citations(patent):
    """Extract all paper citations from a patent."""
    citations = patent.get('references_cited', {}).get('citations', [])
    
    papers = []
    for citation in citations:
        if 'nplcit' in citation:
            npl = citation['nplcit']
            
            paper = {
                'lens_id': npl.get('lens_id'),
                'citation_text': npl.get('text', ''),
                'doi': extract_doi(npl.get('external_ids', [])),
                'sequence': citation.get('sequence')
            }
            
            if paper['lens_id'] or paper['doi']:
                papers.append(paper)
    
    return papers

# Usage
patent = get_patent("091-428-408-796-283")
cited_papers = extract_paper_citations(patent)

for paper in cited_papers:
    # Try to match to Scopus by DOI
    if paper['doi']:
        scopus_match = find_scopus_by_doi(paper['doi'])
        if scopus_match:
            create_citation_link(patent, scopus_match, paper)
```

---

## 📊 **Database Integration**

### Schema Extensions
```sql
-- Patent storage
CREATE TABLE patents (
    lens_id TEXT PRIMARY KEY,
    doc_key TEXT,
    doc_number TEXT,
    jurisdiction TEXT,
    title TEXT,
    abstract TEXT,
    date_published TEXT,
    legal_status TEXT
);

-- Patent citations (papers referenced by patents)
CREATE TABLE patent_citations (
    id INTEGER PRIMARY KEY,
    patent_lens_id TEXT,
    citation_type TEXT, -- 'patcit' or 'nplcit'
    cited_lens_id TEXT,
    citation_text TEXT,
    external_ids TEXT, -- JSON: DOIs, PubMed IDs
    sequence INTEGER,
    FOREIGN KEY (patent_lens_id) REFERENCES patents(lens_id)
);

-- Links between Scopus publications and patents
CREATE TABLE publication_patent_links (
    id INTEGER PRIMARY KEY,
    scopus_eid TEXT,
    patent_lens_id TEXT,
    link_type TEXT, -- 'author_match', 'citation_match', 'institution_match'
    confidence_score REAL,
    match_evidence TEXT, -- JSON with details
    FOREIGN KEY (scopus_eid) REFERENCES documents(eid),
    FOREIGN KEY (patent_lens_id) REFERENCES patents(lens_id)
);
```

---

## 🎯 **Practical Example: Complete Workflow**

### Step 1: Find Patent
```python
# Search for patents from University of Glasgow
response = client.search_patents('applicant.name:"UNIV GLASGOW"', size=1)
patent = response.results[0]

print(f"Patent: {patent['doc_key']}")
print(f"Title: {patent['biblio']['invention_title'][0]['text']}")
```

**Output:**
```
Patent: WO_2013121230_A1_20130822
Title: APPARATUS AND METHODS FOR THE PREPARATION OF REACTION VESSELS WITH A 3D-PRINTER
```

### Step 2: Extract Citations
```python
citations = patent['references_cited']['citations']
paper_citations = [c for c in citations if 'nplcit' in c]

print(f"Found {len(paper_citations)} paper citations:")
for citation in paper_citations:
    npl = citation['nplcit']
    print(f"- {npl['text'][:100]}...")
    print(f"  DOI: {extract_doi(npl.get('external_ids', []))}")
    print(f"  Lens ID: {npl.get('lens_id')}")
```

**Output:**
```
Found 1 paper citations:
- GOVIND V. KAIGALA ET AL: "Rapid prototyping of microfluidic devices with a wax printer", LAB ON A...
  DOI: 10.1039/b617764f
  Lens ID: 105-639-743-003-304
```

### Step 3: Link to Scopus
```python
# Find matching Scopus publication by DOI
doi = "10.1039/b617764f"
scopus_record = find_scopus_by_doi(doi)

if scopus_record:
    # Create high-confidence citation link
    create_patent_publication_link(
        patent_lens_id=patent['lens_id'],
        scopus_eid=scopus_record['eid'],
        link_type='citation_match',
        confidence_score=0.95,
        evidence={'doi_match': doi, 'citation_sequence': 2}
    )
    
    print(f"✅ Linked patent {patent['doc_key']} to Scopus paper {scopus_record['eid']}")
```

---

## 📈 **Analytics Opportunities**

### Innovation Metrics
```python
# Calculate publication-to-patent conversion rates
stats = calculate_innovation_metrics()
print(f"Papers with patents: {stats['papers_with_patents']}")
print(f"Conversion rate: {stats['conversion_rate']:.1%}")

# Find top innovating institutions
top_institutions = get_top_innovating_institutions(limit=10)
for inst in top_institutions:
    print(f"{inst['name']}: {inst['patents']} patents from {inst['papers']} papers")
```

### Citation Analysis
```python
# Find highly cited papers that led to patents
highly_cited = find_highly_cited_papers_with_patents()
for paper in highly_cited:
    print(f"Paper: {paper['title']}")
    print(f"Citations: {paper['citation_count']}")
    print(f"Patents citing it: {paper['patent_count']}")
    print(f"Time to patent: {paper['avg_time_to_patent']:.1f} years")
```

---

## 🚀 **CLI Usage**

### Basic Enrichment
```bash
# Enrich entire database
python lens_db_enrichment.py database.db

# Institution-based matching only
python lens_db_enrichment.py database.db --phase institutions

# Resume interrupted process  
python lens_db_enrichment.py database.db --resume
```

### Querying Results
```bash
# Find patents by author
python lens_db_enrichment.py database.db --query "author:Cronin"

# Find patents by institution
python lens_db_enrichment.py database.db --query "institution:Glasgow"

# Generate reports only
python lens_db_enrichment.py database.db --reports-only
```

---

## ⚠️ **Best Practices**

### Rate Limiting
- Stay within 10 requests/second default limit
- Use caching to avoid duplicate API calls
- Process in batches with progress tracking

### Data Quality
- Validate DOI matches before creating links
- Use confidence scoring (0.0-1.0) for all matches
- Store match evidence for later review

### Performance
- Cache API responses in SQLite (30-day TTL)
- Use batch processing for large datasets
- Monitor memory usage for large result sets

### Error Handling
- Implement exponential backoff for rate limits
- Log all API errors with context
- Provide resumability for long-running processes

---

## 🔗 **Integration Points**

This Lens enrichment system integrates seamlessly with:
- **Existing Scopus Database Builder** (same patterns and architecture)
- **CrossRef DOI Recovery** (complementary patent data)
- **Data Quality Filtering** (maintains data integrity standards)
- **Analytics Framework** (extends innovation metrics)

The result is a comprehensive view of the research-to-innovation pipeline! 🎉
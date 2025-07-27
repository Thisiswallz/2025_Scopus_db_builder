"""
Scopus Database Creator Module

Creates optimized SQLite database from Scopus CSV data with pure data extraction.
Focuses on structuring raw Scopus data without pre-computed analytics.
"""

import sqlite3
import csv
import json
import os
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List


class OptimalScopusDatabase:
    """
    Creates research-optimized SQLite database from Scopus CSV data.
    
    Implements three-phase architecture:
    1. Entity normalization with unique registries
    2. Relationship optimization with pre-computed metrics
    3. Analytics layer with materialized collaboration networks
    """
    
    def __init__(self, csv_path: str):
        """
        Initialize optimal database creator.
        
        Args:
            csv_path: Path to Scopus CSV export file
        """
        self.csv_path = Path(csv_path)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.db_path = self.csv_path.parent / f"{self.csv_path.stem}_research_optimized_{timestamp}.db"
        
        # Entity registries for normalization
        self.authors_registry = {}      # scopus_id -> author_id
        self.institutions_registry = {} # canonical_name -> institution_id
        self.keywords_registry = {}     # keyword_text -> keyword_id
        
        # Counters for entity IDs
        self.author_counter = 1
        self.institution_counter = 1
        self.keyword_counter = 1
        
        # Statistics tracking
        self.stats = {
            "papers_processed": 0,
            "authors_normalized": 0,
            "institutions_normalized": 0,
            "keywords_normalized": 0,
        }
    
    def create_optimal_schema(self):
        """Create three-phase database schema optimized for research queries."""
        print(f"Creating optimal database schema: {self.db_path}")
        
        self.conn = sqlite3.connect(self.db_path)
        cursor = self.conn.cursor()
        
        # Enable foreign keys
        cursor.execute("PRAGMA foreign_keys = ON")
        
        # Phase 1: Master entity tables
        self._create_master_tables(cursor)
        
        # Core papers table
        self._create_papers_table(cursor)
        
        # Phase 2: Relationship tables
        self._create_relationship_tables(cursor)
        
        # Create basic indexes
        self._create_basic_indexes(cursor)
        
        self.conn.commit()
        print("Optimal database schema created successfully")
    
    def _create_master_tables(self, cursor: sqlite3.Cursor):
        """Create master entity tables with unique registries."""
        
        # Authors master table (data only)
        cursor.execute("""
            CREATE TABLE authors_master (
                author_id INTEGER PRIMARY KEY,
                scopus_id TEXT UNIQUE,
                full_name TEXT,
                canonical_name TEXT,
                abbreviated_name TEXT
            )
        """)
        
        # Institutions master table (data only)
        cursor.execute("""
            CREATE TABLE institutions_master (
                institution_id INTEGER PRIMARY KEY,
                canonical_name TEXT UNIQUE,
                country TEXT,
                institution_type TEXT,
                coordinates TEXT -- lat,lng for geo analysis
            )
        """)
        
        # Keywords master table (data only)
        cursor.execute("""
            CREATE TABLE keywords_master (
                keyword_id INTEGER PRIMARY KEY,
                keyword_text TEXT UNIQUE,
                keyword_category TEXT, -- 'author', 'index', 'combined'
                normalized_text TEXT  -- cleaned/standardized version
            )
        """)
    
    def _create_papers_table(self, cursor: sqlite3.Cursor):
        """Create papers table with raw Scopus data only."""
        cursor.execute("""
            CREATE TABLE papers (
                paper_id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                year INTEGER,
                doi TEXT,
                source_title TEXT,
                volume TEXT,
                issue TEXT,
                page_start TEXT,
                page_end TEXT,
                page_count INTEGER,
                cited_by INTEGER DEFAULT 0,
                scopus_link TEXT,
                abstract TEXT,
                language_original TEXT,
                document_type TEXT,
                publication_stage TEXT,
                issn TEXT,
                isbn TEXT
            )
        """)
    
    def _create_relationship_tables(self, cursor: sqlite3.Cursor):
        """Create relationship tables with raw data only."""
        
        # Paper-author relationships
        cursor.execute("""
            CREATE TABLE paper_authors (
                paper_id INTEGER,
                author_id INTEGER,
                position INTEGER,
                corresponding_author BOOLEAN DEFAULT FALSE,
                first_author BOOLEAN DEFAULT FALSE,
                last_author BOOLEAN DEFAULT FALSE,
                
                PRIMARY KEY (paper_id, author_id),
                FOREIGN KEY (paper_id) REFERENCES papers(paper_id),
                FOREIGN KEY (author_id) REFERENCES authors_master(author_id)
            )
        """)
        
        # Paper-keyword relationships
        cursor.execute("""
            CREATE TABLE paper_keywords (
                paper_id INTEGER,
                keyword_id INTEGER,
                keyword_type TEXT CHECK (keyword_type IN ('author', 'index')),
                position INTEGER,
                
                PRIMARY KEY (paper_id, keyword_id, keyword_type),
                FOREIGN KEY (paper_id) REFERENCES papers(paper_id),
                FOREIGN KEY (keyword_id) REFERENCES keywords_master(keyword_id)
            )
        """)
        
        # Paper-institution relationships
        cursor.execute("""
            CREATE TABLE paper_institutions (
                paper_id INTEGER,
                institution_id INTEGER,
                author_count INTEGER DEFAULT 1,
                primary_affiliation BOOLEAN DEFAULT FALSE,
                
                PRIMARY KEY (paper_id, institution_id),
                FOREIGN KEY (paper_id) REFERENCES papers(paper_id),
                FOREIGN KEY (institution_id) REFERENCES institutions_master(institution_id)
            )
        """)
        
        # Citation relationships (structured with enhanced schema)
        cursor.execute("""
            CREATE TABLE paper_citations (
                citation_id INTEGER PRIMARY KEY AUTOINCREMENT,
                citing_paper_id INTEGER,
                reference_text TEXT,
                reference_year INTEGER,
                reference_authors TEXT,
                reference_title TEXT,
                reference_journal TEXT,
                reference_volume TEXT,
                reference_issue TEXT,
                reference_pages TEXT,
                position INTEGER,
                cited_paper_id INTEGER, -- Optional: link to actual paper if found
                citation_weight REAL DEFAULT 1.0,
                self_citation BOOLEAN DEFAULT FALSE,
                
                FOREIGN KEY (citing_paper_id) REFERENCES papers(paper_id),
                FOREIGN KEY (cited_paper_id) REFERENCES papers(paper_id)
            )
        """)
        
        # Funding relationships
        cursor.execute("""
            CREATE TABLE paper_funding (
                paper_id INTEGER,
                funding_id INTEGER PRIMARY KEY AUTOINCREMENT,
                agency_name TEXT NOT NULL,
                grant_numbers TEXT, -- JSON array
                country TEXT,
                funding_amount REAL,
                
                FOREIGN KEY (paper_id) REFERENCES papers(paper_id)
            )
        """)
    
    def _create_basic_indexes(self, cursor: sqlite3.Cursor):
        """Create basic indexes for query performance."""
        
        indexes = [
            # Papers table indexes
            "CREATE INDEX IF NOT EXISTS idx_papers_year ON papers (year)",
            "CREATE INDEX IF NOT EXISTS idx_papers_citations ON papers (cited_by)",
            "CREATE INDEX IF NOT EXISTS idx_papers_type ON papers (document_type)",
            
            # Authors master indexes
            "CREATE INDEX IF NOT EXISTS idx_authors_scopus_id ON authors_master (scopus_id)",
            "CREATE INDEX IF NOT EXISTS idx_authors_canonical ON authors_master (canonical_name)",
            
            # Keywords master indexes  
            "CREATE INDEX IF NOT EXISTS idx_keywords_text ON keywords_master (keyword_text)",
            "CREATE INDEX IF NOT EXISTS idx_keywords_category ON keywords_master (keyword_category)",
            
            # Institutions master indexes
            "CREATE INDEX IF NOT EXISTS idx_institutions_name ON institutions_master (canonical_name)",
            "CREATE INDEX IF NOT EXISTS idx_institutions_country ON institutions_master (country)",
            
            # Relationship table indexes
            "CREATE INDEX IF NOT EXISTS idx_paper_authors_paper ON paper_authors (paper_id)",
            "CREATE INDEX IF NOT EXISTS idx_paper_authors_author ON paper_authors (author_id)",
            "CREATE INDEX IF NOT EXISTS idx_paper_authors_position ON paper_authors (paper_id, position)",
            
            "CREATE INDEX IF NOT EXISTS idx_paper_keywords_paper ON paper_keywords (paper_id)",
            "CREATE INDEX IF NOT EXISTS idx_paper_keywords_keyword ON paper_keywords (keyword_id)",
            "CREATE INDEX IF NOT EXISTS idx_paper_keywords_type ON paper_keywords (keyword_type)",
            
            "CREATE INDEX IF NOT EXISTS idx_paper_institutions_paper ON paper_institutions (paper_id)",
            "CREATE INDEX IF NOT EXISTS idx_paper_institutions_institution ON paper_institutions (institution_id)",
            
            # Citation table indexes
            "CREATE INDEX IF NOT EXISTS idx_paper_citations_citing ON paper_citations (citing_paper_id)",
            "CREATE INDEX IF NOT EXISTS idx_paper_citations_year ON paper_citations (reference_year)",
            "CREATE INDEX IF NOT EXISTS idx_paper_citations_journal ON paper_citations (reference_journal)",
            "CREATE INDEX IF NOT EXISTS idx_paper_citations_authors ON paper_citations (reference_authors)"
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
    
    def _create_research_indexes(self, cursor: sqlite3.Cursor):
        """Create performance indexes optimized for research query patterns."""
        
        indexes = [
            # Papers table indexes
            "CREATE INDEX IF NOT EXISTS idx_papers_year ON papers (year)",
            "CREATE INDEX IF NOT EXISTS idx_papers_citations ON papers (cited_by)",
            "CREATE INDEX IF NOT EXISTS idx_papers_type ON papers (document_type)",
            "CREATE INDEX IF NOT EXISTS idx_papers_author_count ON papers (author_count)",
            
            # Authors master indexes
            "CREATE INDEX IF NOT EXISTS idx_authors_scopus_id ON authors_master (scopus_id)",
            "CREATE INDEX IF NOT EXISTS idx_authors_canonical ON authors_master (canonical_name)",
            "CREATE INDEX IF NOT EXISTS idx_authors_h_index ON authors_master (h_index)",
            "CREATE INDEX IF NOT EXISTS idx_authors_papers ON authors_master (total_papers)",
            
            # Keywords master indexes
            "CREATE INDEX IF NOT EXISTS idx_keywords_text ON keywords_master (keyword_text)",
            "CREATE INDEX IF NOT EXISTS idx_keywords_frequency ON keywords_master (total_frequency)",
            "CREATE INDEX IF NOT EXISTS idx_keywords_tfidf ON keywords_master (avg_tfidf)",
            
            # Institutions master indexes
            "CREATE INDEX IF NOT EXISTS idx_institutions_name ON institutions_master (canonical_name)",
            "CREATE INDEX IF NOT EXISTS idx_institutions_country ON institutions_master (country)",
            "CREATE INDEX IF NOT EXISTS idx_institutions_papers ON institutions_master (paper_count)",
            
            # Relationship table indexes (composite for join optimization)
            "CREATE INDEX IF NOT EXISTS idx_paper_authors_paper ON paper_authors (paper_id)",
            "CREATE INDEX IF NOT EXISTS idx_paper_authors_author ON paper_authors (author_id)",
            "CREATE INDEX IF NOT EXISTS idx_paper_authors_position ON paper_authors (paper_id, position)",
            
            "CREATE INDEX IF NOT EXISTS idx_paper_keywords_paper ON paper_keywords (paper_id)",
            "CREATE INDEX IF NOT EXISTS idx_paper_keywords_keyword ON paper_keywords (keyword_id)",
            "CREATE INDEX IF NOT EXISTS idx_paper_keywords_type ON paper_keywords (keyword_type)",
            
            # Analytics table indexes
            "CREATE INDEX IF NOT EXISTS idx_collaborations_strength ON author_collaborations (collaboration_strength)",
            "CREATE INDEX IF NOT EXISTS idx_collaborations_count ON author_collaborations (collaboration_count)",
            "CREATE INDEX IF NOT EXISTS idx_temporal_year ON temporal_trends (year)",
            "CREATE INDEX IF NOT EXISTS idx_temporal_entity ON temporal_trends (entity_type, entity_id)",
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
    
    def process_csv_to_optimal_db(self):
        """Parse and import Scopus CSV data into structured database."""
        print(f"Loading Scopus CSV: {self.csv_path}")
        
        # Load data using standard library CSV
        data = []
        with open(self.csv_path, 'r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)
            data = list(reader)
        
        print(f"Loaded {len(data)} papers from Scopus export")
        
        # Phase 1: Extract and normalize entities
        print("\n=== Phase 1: Entity Normalization ===")
        self._import_papers(data)
        self._normalize_authors(data)
        self._normalize_institutions(data)
        self._normalize_keywords(data)
        
        # Parse additional complex data fields
        print("\n=== Phase 1.5: Complex Data Parsing ===")
        self._parse_funding_data(data)
        self._parse_references_data(data)
        self._parse_chemicals_data(data)
        self._parse_trade_names_data(data)
        self._parse_correspondence_data(data)
        self._parse_open_access_data(data)
        
        # Phase 2: Build data relationships
        print("\n=== Phase 2: Data Relationships ===")
        self._build_paper_author_relationships(data)
        self._build_paper_keyword_relationships(data)
        self._build_paper_institution_relationships(data)
        
        # Create basic indexes for query performance
        cursor = self.conn.cursor()
        self._create_basic_indexes(cursor)
        
        print(f"\n✅ Pure data database created: {self.db_path}")
        print(f"Database size: {self.db_path.stat().st_size / (1024*1024):.1f} MB")
        self._print_statistics()
    
    def _import_papers(self, data: List[Dict]):
        """Import papers with enhanced metadata."""
        print("Importing papers with research metrics...")
        cursor = self.conn.cursor()
        
        for idx, row in enumerate(data):
            # Parse basic paper data
            year = int(row.get('Year', 0)) if str(row.get('Year', '')).isdigit() else None
            cited_by = int(row.get('Cited by', 0)) if str(row.get('Cited by', '')).isdigit() else 0
            
            cursor.execute("""
                INSERT INTO papers (
                    paper_id, title, year, doi, source_title, volume, issue,
                    page_start, page_end, page_count, cited_by, scopus_link,
                    abstract, language_original, document_type, publication_stage,
                    issn, isbn
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                idx + 1,  # paper_id (1-based)
                row.get('Title', ''),
                year,
                row.get('DOI', ''),
                row.get('Source title', ''),
                row.get('Volume', ''),
                row.get('Issue', ''),
                row.get('Page start', ''),
                row.get('Page end', ''),
                int(row.get('Page count', 0)) if str(row.get('Page count', '')).isdigit() else None,
                cited_by,
                row.get('Link', ''),
                row.get('Abstract', ''),
                row.get('Language of Original Document', ''),
                row.get('Document Type', ''),
                row.get('Publication Stage', ''),
                row.get('ISSN', ''),
                row.get('ISBN', '')
            ))
            
            self.stats["papers_processed"] += 1
        
        self.conn.commit()
        print(f"Imported {self.stats['papers_processed']} papers")
    
    def _normalize_authors(self, data: List[Dict]):
        """Extract and normalize unique authors with disambiguation."""
        print("Normalizing authors with disambiguation...")
        cursor = self.conn.cursor()
        
        for idx, row in enumerate(data):
            authors_raw = row.get('Authors', '')
            author_ids_raw = row.get('Author(s) ID', '')
            full_names_raw = row.get('Author full names', '')
            
            if authors_raw and author_ids_raw:
                authors = [a.strip() for a in str(authors_raw).split(';') if a.strip()]
                author_ids = [a.strip() for a in str(author_ids_raw).split(';') if a.strip()]
                
                # Extract full names if available
                full_names = []
                if full_names_raw:
                    full_names = [a.strip() for a in str(full_names_raw).split(';') if a.strip()]
                
                # Process each author
                for i, (author, scopus_id) in enumerate(zip(authors, author_ids)):
                    if scopus_id and scopus_id not in self.authors_registry:
                        # Extract full name if available
                        full_name = author
                        if i < len(full_names):
                            # Extract name from "Last, First (ID)" format
                            full_name_part = full_names[i]
                            if '(' in full_name_part:
                                full_name = full_name_part.split('(')[0].strip()
                        
                        # Create canonical name (simplified for matching)
                        canonical_name = self._canonicalize_author_name(full_name)
                        
                        cursor.execute("""
                            INSERT INTO authors_master 
                            (author_id, scopus_id, full_name, canonical_name, abbreviated_name)
                            VALUES (?, ?, ?, ?, ?)
                        """, (
                            self.author_counter, 
                            scopus_id, 
                            full_name, 
                            canonical_name,
                            author
                        ))
                        
                        self.authors_registry[scopus_id] = self.author_counter
                        self.author_counter += 1
                        self.stats["authors_normalized"] += 1
        
        self.conn.commit()
        print(f"Normalized {self.stats['authors_normalized']} unique authors")
    
    def _canonicalize_author_name(self, name: str) -> str:
        """Create canonical version of author name for disambiguation."""
        if not name:
            return ""
        
        # Remove punctuation and extra spaces
        import string
        name = name.translate(str.maketrans('', '', string.punctuation))
        
        # Convert to lowercase and split
        parts = name.lower().split()
        
        # Simple canonicalization: "Last, First" -> "first last"
        if len(parts) >= 2:
            return ' '.join(sorted(parts))
        
        return ' '.join(parts)
    
    def _normalize_institutions(self, data: List[Dict]):
        """Extract and normalize institutions from affiliation data."""
        print("Normalizing institutions...")
        cursor = self.conn.cursor()
        
        institution_patterns = set()
        
        for idx, row in enumerate(data):
            affiliations_raw = row.get('Affiliations', '')
            
            if affiliations_raw:
                # Split affiliations and extract institution names
                affiliations = [a.strip() for a in str(affiliations_raw).split(';') if a.strip()]
                
                for affiliation in affiliations:
                    # Extract institution name (simple heuristic)
                    institution_name = self._extract_institution_name(affiliation)
                    
                    if institution_name and institution_name not in self.institutions_registry:
                        # Extract country if possible
                        country = self._extract_country(affiliation)
                        
                        cursor.execute("""
                            INSERT INTO institutions_master 
                            (institution_id, canonical_name, country)
                            VALUES (?, ?, ?)
                        """, (self.institution_counter, institution_name, country))
                        
                        self.institutions_registry[institution_name] = self.institution_counter
                        self.institution_counter += 1
                        self.stats["institutions_normalized"] += 1
        
        self.conn.commit()
        print(f"Normalized {self.stats['institutions_normalized']} unique institutions")
    
    def _extract_institution_name(self, affiliation: str) -> str:
        """Extract institution name from affiliation string."""
        if not affiliation:
            return ""
        
        # Simple heuristic: take first significant part before comma
        parts = affiliation.split(',')
        if parts:
            # Look for university, institute, etc.
            for part in parts:
                part = part.strip()
                if any(keyword in part.lower() for keyword in ['university', 'institute', 'college', 'school']):
                    return part
            
            # Fallback to first substantial part
            if len(parts[0].strip()) > 3:
                return parts[0].strip()
        
        return affiliation.strip()
    
    def _extract_country(self, affiliation: str) -> str:
        """Extract country from affiliation string."""
        if not affiliation:
            return ""
        
        # Simple heuristic: last part is often country
        parts = [p.strip() for p in affiliation.split(',')]
        if len(parts) > 1:
            potential_country = parts[-1]
            # Basic country validation (length and common patterns)
            if 2 <= len(potential_country) <= 20 and potential_country.isalpha():
                return potential_country
        
        return ""
    
    def _normalize_keywords(self, data: List[Dict]):
        """Extract and normalize keywords from both author and index keywords."""
        print("Normalizing keywords...")
        cursor = self.conn.cursor()
        
        # Process both author and index keywords
        keyword_columns = [
            ('Author Keywords', 'author'),
            ('Index Keywords', 'index')
        ]
        
        for col_name, keyword_type in keyword_columns:
            for idx, row in enumerate(data):
                keywords_raw = row.get(col_name, '')
                
                if keywords_raw:
                    keywords = [k.strip() for k in str(keywords_raw).split(';') if k.strip()]
                    
                    for keyword in keywords:
                        # Normalize keyword text
                        normalized = self._normalize_keyword_text(keyword)
                        
                        if normalized and normalized not in self.keywords_registry:
                            cursor.execute("""
                                INSERT INTO keywords_master 
                                (keyword_id, keyword_text, keyword_category, normalized_text)
                                VALUES (?, ?, ?, ?)
                            """, (
                                self.keyword_counter, 
                                keyword, 
                                keyword_type,
                                normalized
                            ))
                            
                            self.keywords_registry[normalized] = self.keyword_counter
                            self.keyword_counter += 1
                            self.stats["keywords_normalized"] += 1
        
        self.conn.commit()
        print(f"Normalized {self.stats['keywords_normalized']} unique keywords")
    
    def _normalize_keyword_text(self, keyword: str) -> str:
        """Normalize keyword text for better matching."""
        if not keyword:
            return ""
        
        # Convert to lowercase, remove extra spaces
        normalized = ' '.join(keyword.lower().split())
        
        # Remove common punctuation
        import string
        normalized = normalized.translate(str.maketrans('', '', string.punctuation))
        
        return normalized
    
    def _build_paper_author_relationships(self, data: List[Dict]):
        """Build paper-author relationships with position information."""
        print("Building paper-author relationships...")
        cursor = self.conn.cursor()
        
        for idx, row in enumerate(data):
            paper_id = idx + 1
            authors_raw = row.get('Authors', '')
            author_ids_raw = row.get('Author(s) ID', '')
            
            if authors_raw and author_ids_raw:
                author_ids = [a.strip() for a in str(author_ids_raw).split(';') if a.strip()]
                
                for position, scopus_id in enumerate(author_ids, 1):
                    if scopus_id in self.authors_registry:
                        author_id = self.authors_registry[scopus_id]
                        
                        # Determine author role
                        first_author = (position == 1)
                        last_author = (position == len(author_ids))
                        
                        cursor.execute("""
                            INSERT INTO paper_authors 
                            (paper_id, author_id, position, first_author, last_author)
                            VALUES (?, ?, ?, ?, ?)
                        """, (paper_id, author_id, position, first_author, last_author))
        
        self.conn.commit()
        print("Paper-author relationships built")
    
    def _build_paper_keyword_relationships(self, data: List[Dict]):
        """Build paper-keyword relationships."""
        print("Building paper-keyword relationships...")
        cursor = self.conn.cursor()
        
        keyword_columns = [
            ('Author Keywords', 'author'),
            ('Index Keywords', 'index')
        ]
        
        for col_name, keyword_type in keyword_columns:
            for idx, row in enumerate(data):
                paper_id = idx + 1
                keywords_raw = row.get(col_name, '')
                
                if keywords_raw:
                    keywords = [k.strip() for k in str(keywords_raw).split(';') if k.strip()]
                    
                    for position, keyword in enumerate(keywords, 1):
                        normalized = self._normalize_keyword_text(keyword)
                        
                        if normalized in self.keywords_registry:
                            keyword_id = self.keywords_registry[normalized]
                            
                            cursor.execute("""
                                INSERT OR IGNORE INTO paper_keywords 
                                (paper_id, keyword_id, keyword_type, position)
                                VALUES (?, ?, ?, ?)
                            """, (paper_id, keyword_id, keyword_type, position))
        
        self.conn.commit()
        print("Paper-keyword relationships built")
    
    def _build_paper_institution_relationships(self, data: List[Dict]):
        """Build paper-institution relationships."""
        print("Building paper-institution relationships...")
        cursor = self.conn.cursor()
        
        for idx, row in enumerate(data):
            paper_id = idx + 1
            affiliations_raw = row.get('Affiliations', '')
            
            if affiliations_raw:
                affiliations = [a.strip() for a in str(affiliations_raw).split(';') if a.strip()]
                institution_counts = defaultdict(int)
                
                for affiliation in affiliations:
                    institution_name = self._extract_institution_name(affiliation)
                    if institution_name in self.institutions_registry:
                        institution_counts[institution_name] += 1
                
                # Insert relationships
                for institution_name, count in institution_counts.items():
                    institution_id = self.institutions_registry[institution_name]
                    primary = (count == max(institution_counts.values()))
                    
                    cursor.execute("""
                        INSERT OR IGNORE INTO paper_institutions 
                        (paper_id, institution_id, author_count, primary_affiliation)
                        VALUES (?, ?, ?, ?)
                    """, (paper_id, institution_id, count, primary))
        
        self.conn.commit()
        print("Paper-institution relationships built")
    
    def _parse_funding_data(self, data: List[Dict]):
        """Parse and import funding information."""
        print("Parsing funding data...")
        cursor = self.conn.cursor()
        
        for idx, row in enumerate(data):
            paper_id = idx + 1
            funding_text = row.get('Funding Details', '') or row.get('Funding Text', '')
            
            if funding_text:
                # Parse funding agencies and grant numbers
                funding_entries = [f.strip() for f in str(funding_text).split(';') if f.strip()]
                
                for funding_entry in funding_entries:
                    # Extract agency name and grant numbers
                    agency_name = funding_entry
                    grant_numbers = []
                    
                    # Look for grant numbers in parentheses or after specific patterns
                    import re
                    grant_matches = re.findall(r'[A-Z]{2,}\s*[-\d]+|Grant\s*[#:]?\s*[\w-]+|\d{4,}', funding_entry)
                    if grant_matches:
                        grant_numbers = grant_matches
                        # Remove grant numbers from agency name
                        for grant in grant_matches:
                            agency_name = agency_name.replace(grant, '').strip()
                    
                    # Clean agency name
                    agency_name = re.sub(r'[,;]+$', '', agency_name).strip()
                    
                    if agency_name and len(agency_name) > 3:  # Filter out very short entries
                        cursor.execute("""
                            INSERT INTO paper_funding 
                            (paper_id, agency_name, grant_numbers)
                            VALUES (?, ?, ?)
                        """, (paper_id, agency_name, json.dumps(grant_numbers)))
        
        self.conn.commit()
        print("Funding data parsed and imported")
    
    def _parse_references_data(self, data: List[Dict]):
        """Parse and import citation references with structured data extraction."""
        print("Parsing references data with enhanced structure...")
        cursor = self.conn.cursor()
        
        
        for idx, row in enumerate(data):
            paper_id = idx + 1
            references_text = row.get('References', '')
            
            if references_text:
                # Split references (typically separated by semicolons)
                references = [r.strip() for r in str(references_text).split(';') if r.strip()]
                
                for ref_idx, reference in enumerate(references, 1):
                    # Parse structured reference data
                    parsed_ref = self._parse_single_reference(reference)
                    
                    cursor.execute("""
                        INSERT INTO paper_citations 
                        (citing_paper_id, reference_text, reference_year, reference_authors,
                         reference_title, reference_journal, reference_volume, 
                         reference_issue, reference_pages, position)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        paper_id, 
                        reference[:500],  # Limit full text length
                        parsed_ref['year'],
                        parsed_ref['authors'],
                        parsed_ref['title'],
                        parsed_ref['journal'],
                        parsed_ref['volume'],
                        parsed_ref['issue'],
                        parsed_ref['pages'],
                        ref_idx
                    ))
        
        self.conn.commit()
        print("References data parsed and imported with structured extraction")
    
    def _parse_single_reference(self, reference: str) -> Dict:
        """
        Parse individual reference string into structured components.
        
        Expected format: "Authors, Title, Journal, Volume, Issue, pp. Pages, (Year)"
        Uses a more robust approach to identify journal names and separate components.
        """
        import re
        
        result = {
            'authors': None,
            'title': None,
            'journal': None,
            'volume': None,
            'issue': None,
            'pages': None,
            'year': None
        }
        
        if not reference:
            return result
        
        # Extract year first (usually in parentheses at the end)
        year_match = re.search(r'\((\d{4})\)(?:\s*$)', reference)
        if year_match:
            result['year'] = int(year_match.group(1))
            # Remove year from reference for further parsing
            reference = reference[:year_match.start()].strip().rstrip(',').strip()
        else:
            # Try to find year without parentheses
            year_match = re.search(r'\b(19|20)\d{2}\b', reference)
            if year_match:
                result['year'] = int(year_match.group())
        
        # Split by commas to get components
        parts = [part.strip() for part in reference.split(',') if part.strip()]
        
        if len(parts) < 2:
            return result
        
        # Known journal abbreviations and patterns (common in academic references)
        journal_patterns = [
            r'\bJ\b',  # Journal abbreviations like "J Med"
            r'\bProc\b',  # Proceedings
            r'\bNature\b', r'\bScience\b',  # Major journals
            r'\bIEEE\b', r'\bACM\b',  # Tech journals
            r'\bAnn\b', r'\bArch\b',  # Annals, Archives
            r'\bBr\b.*\bJ\b',  # British Journal
            r'\bAm\b.*\bJ\b',  # American Journal
            r'\bInt\b.*\bJ\b',  # International Journal
            r'\bEur\b.*\bJ\b',  # European Journal
            r'\bSci\b.*\bRep\b',  # Scientific Reports
            r'Surgery|Medicine|Engineering|Robotics|Manufacturing',
            r'Transaction|Review|Letter'
        ]
        
        # Find journal by looking for known patterns
        journal_idx = None
        for i in range(1, len(parts)):
            part = parts[i]
            part_lower = part.lower()
            
            # Skip if it's clearly numeric data
            if parts[i].isdigit() or re.match(r'^pp\.|^\d+-\d+$', part):
                continue
                
            # Check for journal patterns
            for pattern in journal_patterns:
                if re.search(pattern, part, re.IGNORECASE):
                    journal_idx = i
                    break
            
            if journal_idx is not None:
                break
        
        # If no pattern match, use position-based heuristic
        if journal_idx is None:
            # Journal is usually the first substantial non-numeric text after position 1
            for i in range(2, min(len(parts), 4)):  # Check positions 2-3 (likely journal positions)
                part = parts[i].strip()
                if (len(part) > 3 and 
                    not part.isdigit() and 
                    not re.match(r'^pp\.|^\d+$|^\d+-\d+$', part) and
                    len(part.split()) >= 2):  # Journal names usually have multiple words
                    journal_idx = i
                    break
        
        # Parse based on identified journal position
        if journal_idx is not None and journal_idx >= 2:
            # Standard format: Author(s), Title, Journal, Volume, Issue, Pages
            result['authors'] = parts[0]
            result['title'] = ', '.join(parts[1:journal_idx])  # Everything between author and journal
            result['journal'] = parts[journal_idx]
            
            # Process remaining parts for volume, issue, pages
            remaining_parts = parts[journal_idx + 1:]
            
            for part in remaining_parts:
                part = part.strip()
                
                # Pages (contains 'pp.' or number ranges)
                if 'pp.' in part.lower():
                    pages = re.sub(r'pp\.\s*', '', part, flags=re.IGNORECASE)
                    result['pages'] = pages.strip()
                elif re.match(r'^\d+-\d+$', part) and not result['pages']:
                    result['pages'] = part
                
                # Volume (typically first standalone number)
                elif part.isdigit() and not result['volume']:
                    result['volume'] = part
                
                # Issue (second standalone number)
                elif part.isdigit() and result['volume'] and not result['issue']:
                    result['issue'] = part
        
        else:
            # Fallback: simple parsing when journal can't be clearly identified
            if len(parts) >= 3:
                result['authors'] = parts[0]
                result['title'] = parts[1]
                result['journal'] = parts[2]
                
                # Look for numeric parts in remaining
                for part in parts[3:]:
                    if part.isdigit() and not result['volume']:
                        result['volume'] = part
                    elif part.isdigit() and result['volume'] and not result['issue']:
                        result['issue'] = part
                    elif 'pp.' in part.lower() or re.match(r'^\d+-\d+$', part):
                        pages = re.sub(r'pp\.\s*', '', part, flags=re.IGNORECASE)
                        result['pages'] = pages.strip()
            else:
                # Minimal parsing
                result['authors'] = parts[0]
                if len(parts) > 1:
                    result['title'] = parts[1]
        
        # Clean up empty strings and normalize whitespace
        for key in result:
            if result[key] and isinstance(result[key], str):
                result[key] = ' '.join(result[key].split())  # Normalize whitespace
                if result[key] == '':
                    result[key] = None
        
        return result
    
    def _parse_chemicals_data(self, data: List[Dict]):
        """Parse chemical substances and CAS numbers."""
        print("Parsing chemicals data...")
        cursor = self.conn.cursor()
        
        # Create chemicals table if needed
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS paper_chemicals (
                paper_id INTEGER,
                chemical_name TEXT,
                cas_number TEXT,
                position INTEGER,
                FOREIGN KEY (paper_id) REFERENCES papers(paper_id)
            )
        """)
        
        for idx, row in enumerate(data):
            paper_id = idx + 1
            chemicals_text = row.get('Chemicals/CAS', '') or row.get('Chemical', '')
            
            if chemicals_text:
                chemicals = [c.strip() for c in str(chemicals_text).split(';') if c.strip()]
                
                for chem_idx, chemical in enumerate(chemicals, 1):
                    # Extract CAS number if present
                    cas_number = None
                    chemical_name = chemical
                    
                    import re
                    cas_match = re.search(r'\b\d{2,7}-\d{2}-\d\b', chemical)
                    if cas_match:
                        cas_number = cas_match.group()
                        chemical_name = chemical.replace(cas_number, '').strip()
                    
                    if chemical_name:
                        cursor.execute("""
                            INSERT INTO paper_chemicals 
                            (paper_id, chemical_name, cas_number, position)
                            VALUES (?, ?, ?, ?)
                        """, (paper_id, chemical_name, cas_number, chem_idx))
        
        self.conn.commit()
        print("Chemicals data parsed and imported")
    
    def _parse_trade_names_data(self, data: List[Dict]):
        """Parse trade names and manufacturer information."""
        print("Parsing trade names data...")
        cursor = self.conn.cursor()
        
        # Create trade names table if needed
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS paper_trade_names (
                paper_id INTEGER,
                trade_name TEXT,
                manufacturer TEXT,
                position INTEGER,
                FOREIGN KEY (paper_id) REFERENCES papers(paper_id)
            )
        """)
        
        for idx, row in enumerate(data):
            paper_id = idx + 1
            trade_names_text = row.get('Tradenames', '') or row.get('Trade Names', '')
            
            if trade_names_text:
                trade_names = [t.strip() for t in str(trade_names_text).split(';') if t.strip()]
                
                for trade_idx, trade_name in enumerate(trade_names, 1):
                    # Try to extract manufacturer (often in parentheses)
                    manufacturer = None
                    import re
                    mfg_match = re.search(r'\(([^)]+)\)', trade_name)
                    if mfg_match:
                        manufacturer = mfg_match.group(1)
                        trade_name = re.sub(r'\([^)]+\)', '', trade_name).strip()
                    
                    if trade_name:
                        cursor.execute("""
                            INSERT INTO paper_trade_names 
                            (paper_id, trade_name, manufacturer, position)
                            VALUES (?, ?, ?, ?)
                        """, (paper_id, trade_name, manufacturer, trade_idx))
        
        self.conn.commit()
        print("Trade names data parsed and imported")
    
    def _parse_correspondence_data(self, data: List[Dict]):
        """Parse correspondence author information."""
        print("Parsing correspondence data...")
        cursor = self.conn.cursor()
        
        # Create correspondence table if needed
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS paper_correspondence (
                paper_id INTEGER,
                author_name TEXT,
                email TEXT,
                institution TEXT,
                PRIMARY KEY (paper_id),
                FOREIGN KEY (paper_id) REFERENCES papers(paper_id)
            )
        """)
        
        for idx, row in enumerate(data):
            paper_id = idx + 1
            correspondence = row.get('Correspondence Address', '') or row.get('Corresponding Author', '')
            
            if correspondence:
                # Extract email if present
                import re
                email_match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', correspondence)
                email = email_match.group() if email_match else None
                
                # Extract author name (often before the semicolon or comma)
                author_name = correspondence.split(';')[0].split(',')[0].strip()
                
                # Extract institution (often after author name)
                institution = correspondence.replace(author_name, '').replace(email or '', '').strip()
                institution = re.sub(r'^[,;]\s*', '', institution).strip()
                
                if author_name:
                    cursor.execute("""
                        INSERT OR REPLACE INTO paper_correspondence 
                        (paper_id, author_name, email, institution)
                        VALUES (?, ?, ?, ?)
                    """, (paper_id, author_name, email, institution))
        
        self.conn.commit()
        print("Correspondence data parsed and imported")
    
    def _parse_open_access_data(self, data: List[Dict]):
        """Parse open access and publication information."""
        print("Parsing open access data...")
        cursor = self.conn.cursor()
        
        # Create open access table if needed
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS paper_open_access (
                paper_id INTEGER,
                access_type TEXT,
                publisher TEXT,
                open_access BOOLEAN DEFAULT FALSE,
                PRIMARY KEY (paper_id),
                FOREIGN KEY (paper_id) REFERENCES papers(paper_id)
            )
        """)
        
        for idx, row in enumerate(data):
            paper_id = idx + 1
            access_type = row.get('Access Type', '') or row.get('Open Access', '')
            publisher = row.get('Publisher', '')
            
            # Determine if it's open access
            open_access = False
            if access_type:
                open_access_keywords = ['open', 'free', 'public', 'gold', 'green']
                open_access = any(keyword in str(access_type).lower() for keyword in open_access_keywords)
            
            if access_type or publisher:
                cursor.execute("""
                    INSERT OR REPLACE INTO paper_open_access 
                    (paper_id, access_type, publisher, open_access)
                    VALUES (?, ?, ?, ?)
                """, (paper_id, access_type, publisher, open_access))
        
        self.conn.commit()
        print("Open access data parsed and imported")
    
    
    
    
    
    
    
    def _print_statistics(self):
        """Print database statistics."""
        print("\n" + "="*60)
        print("SCOPUS DATA DATABASE STATISTICS")
        print("="*60)
        
        cursor = self.conn.cursor()
        
        # Core data tables
        tables = [
            ('papers', 'Papers'),
            ('authors_master', 'Unique Authors'),
            ('institutions_master', 'Unique Institutions'),
            ('keywords_master', 'Unique Keywords'),
            ('paper_authors', 'Paper-Author Relationships'),
            ('paper_keywords', 'Paper-Keyword Relationships'),
            ('paper_institutions', 'Paper-Institution Relationships'),
            ('paper_citations', 'Citation References')
        ]
        
        for table, label in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"{label}: {count:,}")
            except Exception:
                # Table may not exist
                continue
        
        # Year distribution
        print("\n" + "-"*40)
        print("DATA OVERVIEW")
        print("-"*40)
        
        # Year range
        cursor.execute("""
            SELECT MIN(year), MAX(year), COUNT(*) 
            FROM papers 
            WHERE year IS NOT NULL
        """)
        result = cursor.fetchone()
        if result and result[0]:
            print(f"Publication Years: {result[0]} - {result[1]} ({result[2]} papers with dates)")
        
        # Most common author name
        cursor.execute("""
            SELECT full_name, COUNT(*) as paper_count
            FROM authors_master am
            JOIN paper_authors pa ON am.author_id = pa.author_id
            GROUP BY am.author_id
            ORDER BY paper_count DESC LIMIT 1
        """)
        result = cursor.fetchone()
        if result:
            print(f"Most Prolific Author: {result[0]} ({result[1]} papers)")
        
        # Most common keyword
        cursor.execute("""
            SELECT keyword_text, COUNT(*) as frequency
            FROM keywords_master km
            JOIN paper_keywords pk ON km.keyword_id = pk.keyword_id
            GROUP BY km.keyword_id
            ORDER BY frequency DESC LIMIT 1
        """)
        result = cursor.fetchone()
        if result:
            print(f"Most Frequent Keyword: {result[0]} ({result[1]} occurrences)")
        
        print("\n" + "="*60)



"""
Lens Database Enricher

Core enrichment logic for adding patent data to Scopus databases.
Handles multi-phase matching, progress tracking, and database updates.
"""

import sqlite3
import json
import time
import os
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple, Generator
from dataclasses import dataclass
from datetime import datetime

from .client import LensClient, LensAPIError
from .matcher import PatentMatcher
from ..config_loader import ConfigLoader


@dataclass
class EnrichmentStats:
    """Statistics for enrichment process."""
    publications_processed: int = 0
    patents_found: int = 0
    high_confidence_links: int = 0
    medium_confidence_links: int = 0
    low_confidence_links: int = 0
    api_calls: int = 0
    cache_hits: int = 0
    errors: int = 0
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    
    @property
    def duration(self) -> float:
        """Get duration in seconds."""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return 0.0
    
    @property
    def total_links(self) -> int:
        """Get total number of links created."""
        return self.high_confidence_links + self.medium_confidence_links + self.low_confidence_links


class LensEnricher:
    """
    Main enricher class for adding Lens patent data to Scopus databases.
    
    Implements multi-phase enrichment:
    1. Author-based patent matching
    2. Institution-based patent matching
    3. Subject-based patent discovery (optional)
    """
    
    def __init__(self, database_path: str, config: ConfigLoader, verbose: bool = False):
        """
        Initialize the enricher.
        
        Args:
            database_path: Path to Scopus database file
            config: Configuration loader instance
            verbose: Enable verbose output
        """
        self.database_path = database_path
        self.config = config
        self.verbose = verbose
        
        # Get Lens configuration
        self.lens_config = config.get_lens_config()
        
        # Initialize Lens client
        api_token = config.get_lens_token()
        if not api_token:
            raise ValueError("Lens API token not configured")
        
        self.lens_client = LensClient(
            api_token=api_token,
            rate_limit=self.lens_config['rate_limit_requests_per_second'],
            timeout=self.lens_config['timeout_seconds'],
            retry_attempts=self.lens_config['retry_attempts'],
            cache_ttl_days=self.lens_config['cache_ttl_days'],
            verbose=verbose
        )
        
        # Initialize patent matcher
        self.matcher = PatentMatcher(self.lens_config['confidence_thresholds'])
        
        # Statistics
        self.stats = EnrichmentStats()
        
        if verbose:
            print(f"🔬 Lens enricher initialized for database: {database_path}")
    
    def is_database_compatible(self) -> bool:
        """Check if database has the required Scopus schema."""
        try:
            with sqlite3.connect(self.database_path) as conn:
                cursor = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='documents'"
                )
                return cursor.fetchone() is not None
        except sqlite3.Error:
            return False
    
    def has_lens_data(self) -> bool:
        """Check if database already contains Lens enrichment data."""
        try:
            with sqlite3.connect(self.database_path) as conn:
                cursor = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='patents'"
                )
                return cursor.fetchone() is not None
        except sqlite3.Error:
            return False
    
    def setup_lens_schema(self):
        """Create Lens-specific tables in the database."""
        if self.verbose:
            print("📊 Setting up Lens database schema...")
        
        schema_sql = """
        -- Core patent data
        CREATE TABLE IF NOT EXISTS patents (
            lens_id TEXT PRIMARY KEY,
            title TEXT,
            abstract TEXT,
            publication_date TEXT,
            patent_type TEXT,
            jurisdiction TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Patent inventors
        CREATE TABLE IF NOT EXISTS patent_inventors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            patent_lens_id TEXT,
            name TEXT,
            extracted_given_name TEXT,
            extracted_family_name TEXT,
            FOREIGN KEY (patent_lens_id) REFERENCES patents(lens_id)
        );
        
        -- Patent applicants/assignees
        CREATE TABLE IF NOT EXISTS patent_applicants (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            patent_lens_id TEXT,
            name TEXT,
            type TEXT,
            country TEXT,
            FOREIGN KEY (patent_lens_id) REFERENCES patents(lens_id)
        );
        
        -- Links between publications and patents
        CREATE TABLE IF NOT EXISTS publication_patent_links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            eid TEXT,
            lens_id TEXT,
            link_type TEXT,
            confidence_score REAL,
            match_details TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (eid) REFERENCES documents(eid),
            FOREIGN KEY (lens_id) REFERENCES patents(lens_id)
        );
        
        -- Processing log for tracking progress
        CREATE TABLE IF NOT EXISTS lens_enrichment_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            eid TEXT,
            status TEXT,
            attempt_count INTEGER DEFAULT 0,
            last_error TEXT,
            processed_at TIMESTAMP,
            FOREIGN KEY (eid) REFERENCES documents(eid)
        );
        
        -- Create indexes for performance
        CREATE INDEX IF NOT EXISTS idx_patent_inventors_lens_id ON patent_inventors(patent_lens_id);
        CREATE INDEX IF NOT EXISTS idx_patent_applicants_lens_id ON patent_applicants(patent_lens_id);
        CREATE INDEX IF NOT EXISTS idx_publication_patent_links_eid ON publication_patent_links(eid);
        CREATE INDEX IF NOT EXISTS idx_publication_patent_links_lens_id ON publication_patent_links(lens_id);
        CREATE INDEX IF NOT EXISTS idx_enrichment_log_eid ON lens_enrichment_log(eid);
        CREATE INDEX IF NOT EXISTS idx_enrichment_log_status ON lens_enrichment_log(status);
        """
        
        with sqlite3.connect(self.database_path) as conn:
            # Execute schema creation
            for statement in schema_sql.split(';'):
                if statement.strip():
                    conn.execute(statement)
            conn.commit()
        
        if self.verbose:
            print("✅ Lens schema setup completed")
    
    def enrich_database(self, phase: str = 'all', resume: bool = False) -> Dict[str, Any]:
        """
        Main enrichment process.
        
        Args:
            phase: Which phase to run ('authors', 'institutions', 'subjects', 'all')
            resume: Resume from last processed record
        
        Returns:
            Dictionary with enrichment statistics
        """
        self.stats.start_time = time.time()
        
        if self.verbose:
            print(f"🚀 Starting Lens enrichment (phase: {phase}, resume: {resume})")
        
        # Get publications to process
        publications = self._get_publications_to_process(resume)
        total_publications = len(publications)
        
        if total_publications == 0:
            if self.verbose:
                print("ℹ️  No publications to process")
            return self._finalize_stats()
        
        if self.verbose:
            print(f"📊 Processing {total_publications:,} publications")
        
        # Process publications in batches
        batch_size = 100
        processed = 0
        
        for batch_start in range(0, total_publications, batch_size):
            batch_end = min(batch_start + batch_size, total_publications)
            batch = publications[batch_start:batch_end]
            
            # Process batch
            self._process_publication_batch(batch, phase)
            
            processed += len(batch)
            self.stats.publications_processed = processed
            
            # Progress update
            if self.verbose:
                percent = (processed / total_publications) * 100
                rate = processed / (time.time() - self.stats.start_time)
                eta = (total_publications - processed) / rate if rate > 0 else 0
                print(f"📈 Progress: {processed:,}/{total_publications:,} ({percent:.1f}%) "
                      f"Rate: {rate:.1f}/sec ETA: {eta:.0f}s")
        
        self.stats.end_time = time.time()
        
        # Update client stats
        client_stats = self.lens_client.get_stats()
        self.stats.api_calls = client_stats['requests_made']
        self.stats.cache_hits = client_stats['cache_hits']
        
        if self.verbose:
            print(f"✅ Enrichment completed in {self.stats.duration:.1f} seconds")
        
        return self._finalize_stats()
    
    def _get_publications_to_process(self, resume: bool) -> List[Dict[str, Any]]:
        """Get list of publications that need processing."""
        with sqlite3.connect(self.database_path) as conn:
            if resume:
                # Get unprocessed publications
                query = """
                SELECT d.eid, d.title, d.authors, d.authkeywords, d.affilname
                FROM documents d
                LEFT JOIN lens_enrichment_log l ON d.eid = l.eid
                WHERE l.eid IS NULL OR l.status != 'completed'
                ORDER BY d.eid
                """
            else:
                # Get all publications
                query = """
                SELECT eid, title, authors, authkeywords, affilname
                FROM documents
                ORDER BY eid
                """
            
            cursor = conn.execute(query)
            
            publications = []
            for row in cursor:
                publications.append({
                    'eid': row[0],
                    'title': row[1] or '',
                    'authors': row[2] or '',
                    'keywords': row[3] or '',
                    'affiliations': row[4] or ''
                })
            
            return publications
    
    def _process_publication_batch(self, publications: List[Dict[str, Any]], phase: str):
        """Process a batch of publications."""
        for pub in publications:
            try:
                self._process_single_publication(pub, phase)
                self._mark_publication_processed(pub['eid'], 'completed')
            except Exception as e:
                self.stats.errors += 1
                error_msg = str(e)
                self._mark_publication_processed(pub['eid'], 'failed', error_msg)
                
                if self.verbose:
                    print(f"❌ Error processing {pub['eid']}: {error_msg}")
    
    def _process_single_publication(self, publication: Dict[str, Any], phase: str):
        """Process a single publication for patent matching."""
        eid = publication['eid']
        
        if phase in ['authors', 'all'] and self.lens_config['phases']['authors_enabled']:
            self._process_author_matches(publication)
        
        if phase in ['institutions', 'all'] and self.lens_config['phases']['institutions_enabled']:
            self._process_institution_matches(publication)
        
        if phase in ['subjects', 'all'] and self.lens_config['phases']['subjects_enabled']:
            self._process_subject_matches(publication)
    
    def _process_author_matches(self, publication: Dict[str, Any]):
        """Find patents by author names."""
        authors = self._parse_authors(publication['authors'])
        
        for author in authors[:5]:  # Limit to first 5 authors to avoid rate limits
            try:
                # Generate name variations
                name_variants = self.matcher.generate_name_variants(author)
                
                for variant in name_variants:
                    # Search patents by inventor
                    response = self.lens_client.search_patents_by_inventor(variant, size=20)
                    
                    # Process results
                    for patent_data in response.results:
                        self._evaluate_and_store_match(
                            publication, patent_data, 'author_match', author
                        )
                    
                    # Don't overload API
                    if len(response.results) == 0:
                        break
                        
            except LensAPIError as e:
                if self.verbose:
                    print(f"⚠️  Author search failed for {author}: {e}")
                continue
    
    def _process_institution_matches(self, publication: Dict[str, Any]):
        """Find patents by institution names."""
        institutions = self._parse_institutions(publication['affiliations'])
        
        for institution in institutions[:3]:  # Limit to avoid rate limits
            try:
                # Clean institution name
                clean_name = self.matcher.clean_institution_name(institution)
                
                # Search patents by applicant
                response = self.lens_client.search_patents_by_applicant(clean_name, size=20)
                
                # Process results
                for patent_data in response.results:
                    self._evaluate_and_store_match(
                        publication, patent_data, 'institution_match', institution
                    )
                
            except LensAPIError as e:
                if self.verbose:
                    print(f"⚠️  Institution search failed for {institution}: {e}")
                continue
    
    def _process_subject_matches(self, publication: Dict[str, Any]):
        """Find patents by subject/keyword matching."""
        keywords = self._parse_keywords(publication['keywords'])
        
        if keywords:
            try:
                # Use top 3 keywords
                keyword_query = ' '.join(keywords[:3])
                
                # Search patents by title keywords
                response = self.lens_client.search_patents_by_title_keywords(keyword_query, size=10)
                
                # Process results
                for patent_data in response.results:
                    self._evaluate_and_store_match(
                        publication, patent_data, 'subject_match', keyword_query
                    )
                
            except LensAPIError as e:
                if self.verbose:
                    print(f"⚠️  Subject search failed: {e}")
    
    def _evaluate_and_store_match(
        self,
        publication: Dict[str, Any],
        patent_data: Dict[str, Any],
        match_type: str,
        match_context: str
    ):
        """Evaluate and store a potential patent-publication match."""
        # Calculate confidence score
        confidence = self.matcher.calculate_confidence(
            publication, patent_data, match_type, match_context
        )
        
        # Get threshold for this match type
        threshold_key = f"{match_type.replace('_match', '')}_match"
        threshold = self.lens_config['confidence_thresholds'].get(threshold_key, 0.5)
        
        if confidence >= threshold:
            # Store the match
            self._store_patent_and_link(publication, patent_data, match_type, confidence, match_context)
            
            # Update statistics
            if confidence >= 0.8:
                self.stats.high_confidence_links += 1
            elif confidence >= 0.6:
                self.stats.medium_confidence_links += 1
            else:
                self.stats.low_confidence_links += 1
    
    def _store_patent_and_link(
        self,
        publication: Dict[str, Any],
        patent_data: Dict[str, Any],
        match_type: str,
        confidence: float,
        match_context: str
    ):
        """Store patent data and create publication-patent link."""
        lens_id = patent_data.get('lens_id')
        if not lens_id:
            return
        
        with sqlite3.connect(self.database_path) as conn:
            # Store patent data
            conn.execute("""
                INSERT OR IGNORE INTO patents (
                    lens_id, title, abstract, publication_date, patent_type, jurisdiction
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, (
                lens_id,
                patent_data.get('title', '')[:500],  # Limit length
                patent_data.get('abstract', '')[:1000],
                patent_data.get('publication_date'),
                patent_data.get('type'),
                patent_data.get('jurisdiction')
            ))
            
            # Store inventors
            inventors = patent_data.get('inventors', [])
            for inventor in inventors:
                conn.execute("""
                    INSERT OR IGNORE INTO patent_inventors (
                        patent_lens_id, name, extracted_given_name, extracted_family_name
                    ) VALUES (?, ?, ?, ?)
                """, (
                    lens_id,
                    inventor.get('name', ''),
                    inventor.get('given_name', ''),
                    inventor.get('family_name', '')
                ))
            
            # Store applicants
            applicants = patent_data.get('applicants', [])
            for applicant in applicants:
                conn.execute("""
                    INSERT OR IGNORE INTO patent_applicants (
                        patent_lens_id, name, type, country
                    ) VALUES (?, ?, ?, ?)
                """, (
                    lens_id,
                    applicant.get('name', ''),
                    applicant.get('type', ''),
                    applicant.get('country', '')
                ))
            
            # Create publication-patent link
            match_details = json.dumps({
                'match_context': match_context,
                'patent_title': patent_data.get('title', ''),
                'timestamp': datetime.now().isoformat()
            })
            
            conn.execute("""
                INSERT OR IGNORE INTO publication_patent_links (
                    eid, lens_id, link_type, confidence_score, match_details
                ) VALUES (?, ?, ?, ?, ?)
            """, (
                publication['eid'],
                lens_id,
                match_type,
                confidence,
                match_details
            ))
            
            conn.commit()
            self.stats.patents_found += 1
    
    def _mark_publication_processed(self, eid: str, status: str, error: Optional[str] = None):
        """Mark a publication as processed in the log."""
        with sqlite3.connect(self.database_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO lens_enrichment_log (
                    eid, status, attempt_count, last_error, processed_at
                ) VALUES (?, ?, COALESCE((
                    SELECT attempt_count + 1 FROM lens_enrichment_log WHERE eid = ?
                ), 1), ?, ?)
            """, (eid, status, eid, error, datetime.now().isoformat()))
            conn.commit()
    
    def _parse_authors(self, authors_string: str) -> List[str]:
        """Parse author names from Scopus format."""
        if not authors_string:
            return []
        
        # Simple parsing - split by semicolon and clean
        authors = []
        for author in authors_string.split(';'):
            cleaned = author.strip()
            if cleaned and len(cleaned) > 2:
                authors.append(cleaned)
        
        return authors
    
    def _parse_institutions(self, affiliations_string: str) -> List[str]:
        """Parse institution names from Scopus format."""
        if not affiliations_string:
            return []
        
        institutions = []
        for affil in affiliations_string.split(';'):
            cleaned = affil.strip()
            if cleaned and len(cleaned) > 5:
                institutions.append(cleaned)
        
        return institutions
    
    def _parse_keywords(self, keywords_string: str) -> List[str]:
        """Parse keywords from Scopus format."""
        if not keywords_string:
            return []
        
        keywords = []
        for keyword in keywords_string.split(';'):
            cleaned = keyword.strip()
            if cleaned and len(cleaned) > 2:
                keywords.append(cleaned)
        
        return keywords
    
    def _finalize_stats(self) -> Dict[str, Any]:
        """Convert stats to dictionary for return."""
        return {
            'publications_processed': self.stats.publications_processed,
            'patents_found': self.stats.patents_found,
            'high_confidence_links': self.stats.high_confidence_links,
            'medium_confidence_links': self.stats.medium_confidence_links,
            'low_confidence_links': self.stats.low_confidence_links,
            'total_links': self.stats.total_links,
            'api_calls': self.stats.api_calls,
            'cache_hits': self.stats.cache_hits,
            'errors': self.stats.errors,
            'duration': self.stats.duration
        }
    
    def generate_reports(self):
        """Generate enrichment reports."""
        if self.verbose:
            print("📊 Generating Lens enrichment reports...")
        
        # Implementation would generate HTML/JSON/CSV reports
        # Similar to existing reporting in the project
        pass
"""
Scopus Database Creator Module

Creates optimized SQLite database from Scopus CSV data with pure data extraction.
Focuses on structuring raw Scopus data without pre-computed analytics.
"""

import sqlite3
import csv
import json
import os
import logging
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple
from ..data_quality_filter_simple import ScopusDataQualityFilter


class OptimalScopusDatabase:
    """
    Creates research-optimized SQLite database from Scopus CSV data.
    
    Implements three-phase architecture:
    1. Entity normalization with unique registries
    2. Relationship optimization with pre-computed metrics
    3. Analytics layer with materialized collaboration networks
    """
    
    def __init__(self, csv_path: str, enable_data_filtering: bool = True, csv_files: List = None, keyword: str = None, query_file: str = None):
        """
        Initialize optimal database creator.
        
        Args:
            csv_path: Path to Scopus CSV export file or directory
            enable_data_filtering: Whether to apply data quality filtering
            csv_files: List of CSV files (for multi-CSV mode)
            keyword: Keyword for master database naming (e.g., '3DP' -> 'master-3DP.db')
            query_file: Path to file containing Scopus query (optional)
        """
        self.csv_path = Path(csv_path)
        self.csv_files = csv_files or []
        self.multi_csv_mode = bool(csv_files)
        self.keyword = keyword
        self.scopus_query = None
        
        # Load Scopus query if file provided
        self.expected_total_results = None
        if query_file:
            try:
                query_path = Path(query_file)
                if query_path.exists():
                    with open(query_path, 'r', encoding='utf-8') as f:
                        content = f.read().strip()
                        lines = content.split('\n')
                        
                        # Parse the query and total results
                        for line in lines:
                            line = line.strip()
                            if line.startswith("Scopus_query="):
                                self.scopus_query = line[len("Scopus_query="):].strip()
                            elif line.startswith("total_results="):
                                try:
                                    self.expected_total_results = int(line[len("total_results="):].strip())
                                except ValueError:
                                    pass
                    
                    if self.scopus_query:
                        print(f"✅ Loaded Scopus query from: {query_file}")
                        print(f"   Query length: {len(self.scopus_query)} characters")
                        if self.expected_total_results:
                            print(f"   Expected total results: {self.expected_total_results:,}")
                    else:
                        print(f"⚠️ No valid Scopus query found in: {query_file}")
                else:
                    print(f"⚠️ Query file not found: {query_file}")
            except Exception as e:
                print(f"⚠️ Error loading query file: {e}")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Determine the base directory and organize files properly
        if self.multi_csv_mode:
            # For multi-CSV mode (directory input)
            base_dir = self.csv_path.parent  # Use parent directory for output placement
        else:
            # For single CSV mode
            # Check if CSV is in raw_scopus/ subfolder (organized structure)
            if self.csv_path.parent.name == "raw_scopus":
                base_dir = self.csv_path.parent.parent  # Go up to export directory
            else:
                # CSV is in the main directory (legacy structure)
                base_dir = self.csv_path.parent
        
        # Create timestamped output directory for ALL outputs including database
        run_output_dir = base_dir / "output" / f"Run_{timestamp}"
        run_output_dir.mkdir(parents=True, exist_ok=True)  # Create timestamped output directory
        
        # Database now goes INSIDE the timestamped run folder
        db_name = f"master_{timestamp}.db"
        self.db_path = run_output_dir / db_name
        
        # Quality filter logs go in timestamped output folder
        if self.multi_csv_mode:
            filter_log_path = run_output_dir / f"data_quality_exclusions_combined_{timestamp}.json"
        else:
            filter_log_path = run_output_dir / f"data_quality_exclusions_{timestamp}.json"
            
        # Load configuration for data quality settings
        from ..config_loader import get_config
        config = get_config()
        
        # Get data quality configuration
        data_quality_config = config.get_data_quality_config()
        
        self.data_filter = ScopusDataQualityFilter(
            enable_filtering=enable_data_filtering and data_quality_config['filtering_enabled'],
            log_path=str(filter_log_path)
        )
        
        # Store config for use in database creation
        self.config = config
        
        # Entity registries for normalization
        self.authors_registry = {}      # scopus_id -> author_id
        self.institutions_registry = {} # canonical_name -> institution_id
        self.keywords_registry = {}     # keyword_text -> keyword_id
        
        # Counters for entity IDs
        self.author_counter = 1
        self.institution_counter = 1
        self.keyword_counter = 1
        
        # Statistics tracking with expected vs actual counts
        self.stats = {
            "papers_processed": 0,
            "papers_filtered_out": 0,
            "duplicates_removed": 0,
            "csv_files_processed": 0,
            "authors_normalized": 0,
            "institutions_normalized": 0,
            "keywords_normalized": 0,
        }
        
        # Expected vs Actual table population tracking
        self.population_tracking = {
            "expected": {
                "papers": 0,
                "authors_master": 0,
                "institutions_master": 0,
                "keywords_master": 0,
                "paper_funding": 0,
                "paper_authors": 0,
                "paper_keywords": 0,
                "paper_institutions": 0,
                "paper_citations": 0
            },
            "actual": {},
            "validation_issues": []
        }
    
    def _get_column_value(self, row: Dict, column_name: str, alternatives: List[str] = None) -> str:
        """
        Get column value handling BOM and formatting issues in CSV headers.
        
        Args:
            row: CSV row dictionary
            column_name: Primary column name to look for
            alternatives: Alternative column names to try
        
        Returns:
            Column value or empty string if not found
        """
        # Try exact match first
        if column_name in row:
            return row.get(column_name, '')
        
        # Handle BOM and quote issues - look for column containing the name
        for key in row.keys():
            if column_name in key:
                return row.get(key, '')
        
        # Try alternatives if provided
        if alternatives:
            for alt in alternatives:
                if alt in row:
                    return row.get(alt, '')
                # Also check for BOM/quote issues in alternatives
                for key in row.keys():
                    if alt in key:
                        return row.get(key, '')
        
        return ''
    
    def _track_expected_counts(self, data: List[Dict]):
        """Calculate expected table population counts from data."""
        print("📊 Calculating expected table population counts...")
        
        # Papers count (should match input data)
        self.population_tracking["expected"]["papers"] = len(data)
        
        # Track unique entities to estimate normalized table sizes
        unique_authors = set()
        unique_institutions = set()
        unique_keywords = set()
        total_funding_entries = 0
        total_author_papers = 0
        total_keyword_papers = 0
        total_institution_papers = 0
        total_citation_entries = 0
        
        for row in data:
            # Count unique authors
            authors_raw = self._get_column_value(row, 'Authors')
            author_ids_raw = self._get_column_value(row, 'Author(s) ID')
            if authors_raw and author_ids_raw:
                author_ids = [a.strip() for a in str(author_ids_raw).split(';') if a.strip()]
                unique_authors.update(author_ids)
                total_author_papers += len(author_ids)
            
            # Count unique institutions
            affiliations_raw = self._get_column_value(row, 'Affiliations')
            if affiliations_raw:
                affiliations = [a.strip() for a in str(affiliations_raw).split(';') if a.strip()]
                unique_institutions.update(affiliations)
                total_institution_papers += len(affiliations)
            
            # Count unique keywords
            for col_name in ['Author Keywords', 'Index Keywords']:
                keywords_raw = self._get_column_value(row, col_name)
                if keywords_raw:
                    keywords = [k.strip() for k in str(keywords_raw).split(';') if k.strip()]
                    unique_keywords.update(keywords)
                    total_keyword_papers += len(keywords)
            
            # Count funding entries
            funding_text = self._get_column_value(row, 'Funding Details') or self._get_column_value(row, 'Funding Texts')
            if funding_text:
                funding_entries = [f.strip() for f in str(funding_text).split(';') if f.strip()]
                total_funding_entries += len(funding_entries)
            
            # Count citation entries
            references_text = self._get_column_value(row, 'References')
            if references_text:
                # Estimate citations based on text length and patterns
                citation_count = len([r for r in str(references_text).split(';') if len(r.strip()) > 10])
                total_citation_entries += citation_count
        
        # Update expected counts
        self.population_tracking["expected"]["authors_master"] = len(unique_authors)
        self.population_tracking["expected"]["institutions_master"] = len(unique_institutions)
        self.population_tracking["expected"]["keywords_master"] = len(unique_keywords)
        self.population_tracking["expected"]["paper_funding"] = total_funding_entries
        self.population_tracking["expected"]["paper_authors"] = total_author_papers
        self.population_tracking["expected"]["paper_keywords"] = total_keyword_papers
        self.population_tracking["expected"]["paper_institutions"] = total_institution_papers
        self.population_tracking["expected"]["paper_citations"] = total_citation_entries
        
        print(f"   📋 Expected papers: {self.population_tracking['expected']['papers']:,}")
        print(f"   👥 Expected unique authors: {self.population_tracking['expected']['authors_master']:,}")
        print(f"   🏢 Expected unique institutions: {self.population_tracking['expected']['institutions_master']:,}")
        print(f"   🔖 Expected unique keywords: {self.population_tracking['expected']['keywords_master']:,}")
        print(f"   💰 Expected funding entries: {self.population_tracking['expected']['paper_funding']:,}")
        print(f"   📊 Expected citations: {self.population_tracking['expected']['paper_citations']:,}")
    
    def _validate_table_population(self) -> Dict:
        """Validate that all tables populated as expected and return validation report."""
        print("\n🔍 VALIDATING DATABASE POPULATION...")
        
        cursor = self.conn.cursor()
        validation_report = {
            "overall_status": "PASS",
            "table_validations": {},
            "critical_issues": [],
            "warnings": [],
            "population_summary": {}
        }
        
        # Check each table
        for table_name, expected_count in self.population_tracking["expected"].items():
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                actual_count = cursor.fetchone()[0]
                self.population_tracking["actual"][table_name] = actual_count
                
                # Calculate population percentage
                if expected_count > 0:
                    population_percentage = (actual_count / expected_count) * 100
                else:
                    population_percentage = 100 if actual_count == 0 else 0
                
                # Determine validation status
                status = "PASS"
                issues = []
                
                if actual_count == 0 and expected_count > 0:
                    status = "CRITICAL_FAIL"
                    issues.append(f"Table is empty but expected {expected_count:,} records")
                    validation_report["critical_issues"].append(f"{table_name}: Empty table")
                elif population_percentage < 50:
                    status = "FAIL"
                    issues.append(f"Only {population_percentage:.1f}% populated ({actual_count:,}/{expected_count:,})")
                elif population_percentage < 90:
                    status = "WARNING"
                    issues.append(f"Under-populated: {population_percentage:.1f}% ({actual_count:,}/{expected_count:,})")
                    validation_report["warnings"].append(f"{table_name}: {population_percentage:.1f}% populated")
                
                validation_report["table_validations"][table_name] = {
                    "status": status,
                    "expected": expected_count,
                    "actual": actual_count,
                    "population_percentage": population_percentage,
                    "issues": issues
                }
                
                # Print status
                status_emoji = "✅" if status == "PASS" else "⚠️" if status == "WARNING" else "❌"
                print(f"   {status_emoji} {table_name}: {actual_count:,}/{expected_count:,} ({population_percentage:.1f}%)")
                
                if status in ["FAIL", "CRITICAL_FAIL"]:
                    validation_report["overall_status"] = "FAIL"
                elif status == "WARNING" and validation_report["overall_status"] == "PASS":
                    validation_report["overall_status"] = "WARNING"
                    
            except Exception as e:
                validation_report["critical_issues"].append(f"{table_name}: Database error - {str(e)}")
                validation_report["overall_status"] = "FAIL"
                print(f"   ❌ {table_name}: Database error - {str(e)}")
        
        return validation_report
    
    def _generate_validation_report(self, validation_report: Dict) -> str:
        """Generate a comprehensive validation report."""
        from datetime import datetime
        
        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("SCOPUS DATABASE POPULATION VALIDATION REPORT")
        report_lines.append("=" * 80)
        report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"Database: {self.db_path}")
        report_lines.append(f"Overall Status: {validation_report['overall_status']}")
        report_lines.append("")
        
        # Summary statistics
        report_lines.append("POPULATION SUMMARY:")
        report_lines.append("-" * 40)
        total_expected = sum(self.population_tracking["expected"].values())
        total_actual = sum(self.population_tracking["actual"].values())
        overall_percentage = (total_actual / total_expected * 100) if total_expected > 0 else 0
        
        report_lines.append(f"Total Expected Records: {total_expected:,}")
        report_lines.append(f"Total Actual Records:   {total_actual:,}")
        report_lines.append(f"Overall Population:     {overall_percentage:.1f}%")
        report_lines.append("")
        
        # Table-by-table details
        report_lines.append("TABLE VALIDATION DETAILS:")
        report_lines.append("-" * 40)
        for table_name, validation in validation_report["table_validations"].items():
            status_symbol = "✅" if validation["status"] == "PASS" else "⚠️" if validation["status"] == "WARNING" else "❌"
            report_lines.append(f"{status_symbol} {table_name}:")
            report_lines.append(f"    Expected: {validation['expected']:,}")
            report_lines.append(f"    Actual:   {validation['actual']:,}")
            report_lines.append(f"    Population: {validation['population_percentage']:.1f}%")
            if validation["issues"]:
                for issue in validation["issues"]:
                    report_lines.append(f"    Issue: {issue}")
            report_lines.append("")
        
        # Critical issues
        if validation_report["critical_issues"]:
            report_lines.append("CRITICAL ISSUES:")
            report_lines.append("-" * 40)
            for issue in validation_report["critical_issues"]:
                report_lines.append(f"❌ {issue}")
            report_lines.append("")
        
        # Warnings
        if validation_report["warnings"]:
            report_lines.append("WARNINGS:")
            report_lines.append("-" * 40)
            for warning in validation_report["warnings"]:
                report_lines.append(f"⚠️ {warning}")
            report_lines.append("")
        
        # Recommendations
        report_lines.append("RECOMMENDATIONS:")
        report_lines.append("-" * 40)
        if validation_report["overall_status"] == "PASS":
            report_lines.append("✅ Database populated successfully - no action required.")
        else:
            report_lines.append("🔧 Issues detected - review the following:")
            if validation_report["critical_issues"]:
                report_lines.append("   • Fix critical failures (empty tables)")
            if validation_report["warnings"]:
                report_lines.append("   • Investigate under-populated tables")
            report_lines.append("   • Check CSV data quality and column mappings")
            report_lines.append("   • Verify database creation process completed without errors")
        
        report_lines.append("")
        report_lines.append("=" * 80)
        
        return "\n".join(report_lines)
    
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
                isbn TEXT,
                scopus_query TEXT
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
    
    def _deduplicate_records(self, all_records: List[Dict[str, str]], file_sources: List[str]) -> Tuple[List[Dict[str, str]], int]:
        """
        Remove duplicate records using ONLY DOI-based deduplication.
        Records without DOI are flagged for manual DOI identification.
        
        Args:
            all_records: Combined records from all CSV files
            file_sources: List indicating which file each record came from
            
        Returns:
            Tuple of (unique_records, duplicate_count)
        """
        seen_dois = set()
        unique_records = []
        duplicate_count = 0
        unique_sources = []
        
        # Detailed reporting dictionaries
        doi_duplicates = []
        missing_doi_records = []
        duplicates_by_year = defaultdict(int)
        missing_doi_by_year = defaultdict(int)
        
        print(f"\n🔍 DOI-ONLY DEDUPLICATION ANALYSIS")
        print(f"   Total records before deduplication: {len(all_records):,}")
        print(f"   Using ONLY DOI for duplicate detection")
        
        for i, record in enumerate(all_records):
            doi = record.get('DOI', '').strip()
            title = record.get('Title', '').strip()
            year = record.get('Year', '').strip()
            
            is_duplicate = False
            
            if not doi:
                # NO DOI - Flag for review but include in database
                missing_doi_records.append({
                    'record_index': i,
                    'title': title[:100] + '...' if len(title) > 100 else title,
                    'year': year,
                    'source_file': file_sources[i],
                    'authors': record.get('Authors', '')[:100] + '...' if len(record.get('Authors', '')) > 100 else record.get('Authors', ''),
                    'source_title': record.get('Source title', ''),
                    'pubmed_id': record.get('PubMed ID', '').strip()
                })
                
                # Track missing DOI by year
                if year:
                    try:
                        year_int = int(year)
                        if 2000 <= year_int <= 2030:
                            missing_doi_by_year[year_int] += 1
                    except ValueError:
                        pass
                
                # Include record without DOI (no deduplication possible)
                unique_records.append(record)
                unique_sources.append(file_sources[i])
                
            elif doi in seen_dois:
                # DOI DUPLICATE - Remove
                is_duplicate = True
                duplicate_count += 1
                
                # Log DOI duplicate details
                doi_duplicates.append({
                    'record_index': i,
                    'doi': doi,
                    'title': title[:100] + '...' if len(title) > 100 else title,
                    'year': year,
                    'source_file': file_sources[i]
                })
                
                # Track duplicates by year
                if year:
                    try:
                        year_int = int(year)
                        if 2000 <= year_int <= 2030:
                            duplicates_by_year[year_int] += 1
                    except ValueError:
                        pass
            else:
                # UNIQUE DOI - Keep record
                seen_dois.add(doi)
                unique_records.append(record)
                unique_sources.append(file_sources[i])
        
        print(f"   ✅ Unique records after deduplication: {len(unique_records):,}")
        print(f"   ❌ Duplicate records removed: {duplicate_count:,}")
        if duplicate_count > 0:
            print(f"   📊 Deduplication rate: {duplicate_count/len(all_records)*100:.1f}%")
        
        # Calculate missing DOI count
        missing_doi_count = len(missing_doi_records)
        
        # DETAILED REPORTING
        print(f"\n📋 DETAILED DEDUPLICATION REPORT:")
        print(f"   DOI-based deduplication: {duplicate_count:,} duplicates removed")
        print(f"   Records without DOI: {missing_doi_count:,} flagged for review")
        
        # Report duplicates by publication year
        if duplicates_by_year:
            print(f"\n   📅 Duplicates by publication year:")
            for year in sorted(duplicates_by_year.keys(), reverse=True):
                count = duplicates_by_year[year]
                print(f"      {year}: {count:,} duplicates")
        
        # Report missing DOI records by year
        if missing_doi_by_year:
            print(f"\n   🔍 Records without DOI by year (flagged for review):")
            for year in sorted(missing_doi_by_year.keys(), reverse=True):
                count = missing_doi_by_year[year]
                print(f"      {year}: {count:,} records need DOI")
        
        # Save detailed duplicate logs to file
        if hasattr(self, 'data_filter') and self.data_filter:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            duplicate_log = {
                'summary': {
                    'total_records': len(all_records),
                    'unique_records': len(unique_records),
                    'duplicate_count': duplicate_count,
                    'deduplication_rate': duplicate_count/len(all_records)*100 if len(all_records) > 0 else 0,
                    'missing_doi_count': missing_doi_count,
                    'duplicates_by_year': dict(duplicates_by_year),
                    'missing_doi_by_year': dict(missing_doi_by_year)
                },
                'doi_duplicates': doi_duplicates[:100],  # Limit to first 100 for file size
                'missing_doi_records': missing_doi_records[:100],  # Sample of records needing DOI
                'generation_timestamp': timestamp
            }
            
            # Save to JSON file in output directory
            if hasattr(self.data_filter, 'log_path'):
                duplicate_log_path = self.data_filter.log_path.parent / f"deduplication_details_{timestamp}.json"
                try:
                    with open(duplicate_log_path, 'w', encoding='utf-8') as f:
                        json.dump(duplicate_log, f, indent=2, ensure_ascii=False)
                    print(f"   💾 Detailed duplicate log saved: {duplicate_log_path.name}")
                except Exception as e:
                    print(f"   ⚠️ Could not save duplicate log: {e}")
                
                # Save separate missing DOI records file for manual review
                if missing_doi_count > 0:
                    missing_doi_log_path = self.data_filter.log_path.parent / f"missing_doi_records_{timestamp}.json"
                    missing_doi_report = {
                        'summary': {
                            'total_missing_doi': missing_doi_count,
                            'missing_doi_by_year': dict(missing_doi_by_year)
                        },
                        'records_needing_doi': missing_doi_records,
                        'generation_timestamp': timestamp,
                        'instructions': 'These records lack DOI and were not deduplicated. Manual DOI identification needed.'
                    }
                    try:
                        with open(missing_doi_log_path, 'w', encoding='utf-8') as f:
                            json.dump(missing_doi_report, f, indent=2, ensure_ascii=False)
                        print(f"   🔍 Missing DOI report saved: {missing_doi_log_path.name}")
                    except Exception as e:
                        print(f"   ⚠️ Could not save missing DOI report: {e}")
        
        return unique_records, duplicate_count
    
    def process_csv_to_optimal_db(self):
        """Parse and import Scopus CSV data into structured database with quality filtering."""
        logger = logging.getLogger(__name__)
        
        logger.info("🔄 Starting CSV data processing and population")
        
        if self.multi_csv_mode:
            # Multi-CSV processing
            print(f"🔄 Loading Multiple CSV files from: {self.csv_path}")
            raw_data = []
            file_sources = []
            
            for csv_file in self.csv_files:
                print(f"   📄 Loading: {csv_file.name}")
                try:
                    with open(csv_file, 'r', encoding='utf-8-sig') as file:
                        reader = csv.DictReader(file)
                        file_data = list(reader)
                        raw_data.extend(file_data)
                        # Track which file each record came from
                        file_sources.extend([csv_file.name] * len(file_data))
                        print(f"      Records loaded: {len(file_data):,}")
                except Exception as e:
                    print(f"      ❌ Error loading {csv_file.name}: {e}")
                    continue
            
            self.stats["csv_files_processed"] = len(self.csv_files)
            print(f"\n📊 MULTI-CSV SUMMARY:")
            print(f"   CSV files processed: {self.stats['csv_files_processed']}")
            print(f"   Total records loaded: {len(raw_data):,}")
            
            # Deduplicate records across files
            if len(raw_data) > 0:
                raw_data, duplicates_removed = self._deduplicate_records(raw_data, file_sources)
                self.stats["duplicates_removed"] = duplicates_removed
            
        else:
            # Single CSV processing (original behavior)
            print(f"📄 Loading Single CSV: {self.csv_path}")
            raw_data = []
            with open(self.csv_path, 'r', encoding='utf-8-sig') as file:
                reader = csv.DictReader(file)
                raw_data = list(reader)
            
            print(f"   Records loaded: {len(raw_data):,}")
            self.stats["csv_files_processed"] = 1
        
        # Apply data quality filtering
        logger.info(f"📊 Starting data quality filtering on {len(raw_data):,} records")
        data, filter_report = self.data_filter.filter_csv_data(raw_data)
        logger.info(f"✅ Data quality filtering completed. Records after filtering: {len(data):,}")
        self.stats["papers_filtered_out"] = filter_report["summary"]["excluded_records"]
        
        # Print filtering summary
        self.data_filter.print_exclusion_summary()
        
        print(f"\n📊 FINAL DATASET SUMMARY:")
        print(f"   Total records in CSV: {len(raw_data):,}")
        print(f"   Records after filtering: {len(data):,}")
        print(f"   Quality improvement: {filter_report['summary']['quality_improvement']}")
        print(f"   Detailed exclusion log: {filter_report['log_file']}")
        print(f"\nProceeding with {len(data)} high-quality research papers...")
        
        # Track expected counts before processing
        self._track_expected_counts(data)
        
        # Database schema already created in main script - skip duplicate creation
        logger.info("🔧 Database schema already created, proceeding with data import")
        
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
        
        print(f"\n✅ High-quality research database created: {self.db_path}")
        print(f"Database size: {self.db_path.stat().st_size / (1024*1024):.1f} MB")
        self._print_statistics()
        
        # Validate database population
        validation_report = self._validate_table_population()
        
        # Generate and save validation report
        report_content = self._generate_validation_report(validation_report)
        
        # Save validation report to file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f"database_validation_report_{timestamp}.txt"
        if self.multi_csv_mode:
            report_path = self.csv_path / "output" / report_filename
        else:
            if self.csv_path.parent.name == "raw_scopus":
                report_path = self.csv_path.parent.parent / "output" / report_filename
            else:
                report_path = self.csv_path.parent / report_filename
        
        # Ensure output directory exists
        report_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        # Print validation summary
        print(f"\n🔍 DATABASE VALIDATION SUMMARY:")
        print(f"   Overall Status: {validation_report['overall_status']}")
        if validation_report['critical_issues']:
            print(f"   ❌ Critical Issues: {len(validation_report['critical_issues'])}")
        if validation_report['warnings']:
            print(f"   ⚠️ Warnings: {len(validation_report['warnings'])}")
        print(f"   📋 Detailed report saved: {report_path}")
        
        # Print final status
        if validation_report['overall_status'] == 'PASS':
            print(f"\n🎉 DATABASE SUCCESSFULLY VALIDATED - All tables populated correctly!")
        elif validation_report['overall_status'] == 'WARNING':
            print(f"\n⚠️ DATABASE CREATED WITH WARNINGS - Check report for details")
        else:
            print(f"\n❌ DATABASE VALIDATION FAILED - Check report for critical issues")
            print(f"   Report location: {report_path}")
    
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
                    issn, isbn, scopus_query
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                row.get('ISBN', ''),
                self.scopus_query  # Add the Scopus query for each record
            ))
            
            self.stats["papers_processed"] += 1
        
        self.conn.commit()
        print(f"Imported {self.stats['papers_processed']} papers")
    
    def _normalize_authors(self, data: List[Dict]):
        """Extract and normalize unique authors with disambiguation."""
        print("Normalizing authors with disambiguation...")
        cursor = self.conn.cursor()
        
        for idx, row in enumerate(data):
            authors_raw = self._get_column_value(row, 'Authors')
            author_ids_raw = self._get_column_value(row, 'Author(s) ID')
            full_names_raw = self._get_column_value(row, 'Author full names')
            
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
            authors_raw = self._get_column_value(row, 'Authors')
            author_ids_raw = self._get_column_value(row, 'Author(s) ID')
            
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
            funding_text = self._get_column_value(row, 'Funding Details') or self._get_column_value(row, 'Funding Texts')
            
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
        
        Handles multiple reference formats:
        - Journal articles: "Authors, Title, Journal, Volume, Issue, pp. Pages, (Year)"
        - Books: "Authors, Title, (Year)" or "Authors, Title, Publisher, (Year)"
        - Standards: "Standard Name, Standard Number, (Year)"
        - Web documents: "Title [WWW Document], (Year)"
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
        
        if not reference or len(reference.strip()) < 3:
            return result
        
        # Handle very short references (data quality issues)
        if len(reference.strip()) < 10:
            # Try to extract year if it's just a year
            if re.match(r'^\d{4}$', reference.strip()):
                result['year'] = int(reference.strip())
            # For very short text, put it in title
            elif not reference.strip().isdigit():
                result['title'] = reference.strip()
            return result
        
        # Handle truncated references starting with comma
        if reference.strip().startswith(','):
            reference = reference.strip()[1:].strip()
        
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
        
        # Handle web documents
        if '[WWW Document]' in reference:
            # Remove [WWW Document] and treat title as everything before it
            title_part = reference.replace('[WWW Document]', '').strip().rstrip(',').strip()
            result['title'] = title_part
            result['journal'] = 'Web Document'
            return result
        
        # Handle references without commas (books, standards, incomplete)
        if ',' not in reference:
            # If it looks like a standard title or incomplete reference
            if any(word in reference.lower() for word in ['standard', 'specification', 'guideline', 'principles', 'terminology']):
                result['title'] = reference
                result['journal'] = 'Standard/Document'
            elif len(reference.split()) >= 3:  # Reasonable length for a title
                result['title'] = reference
            return result
        
        # Split by commas to get components
        parts = [part.strip() for part in reference.split(',') if part.strip()]
        
        if len(parts) < 2:
            # Single part, treat as title
            result['title'] = reference
            return result
        
        # Detect if this is a standards document
        if any(keyword in ' '.join(parts).lower() for keyword in 
               ['iso ', 'astm ', 'standard', 'specification', 'guideline']):
            # Standards format: "Standard Name, Number, (Year)"
            result['title'] = ', '.join(parts[:-1]) if len(parts) > 1 else parts[0]
            result['journal'] = 'Standard'
            if len(parts) >= 2 and parts[-1].isdigit():
                result['volume'] = parts[-1]  # Standard number
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
            r'Transaction|Review|Letter|HNO|BMC'
        ]
        
        # Find journal by looking for known patterns
        journal_idx = None
        for i in range(1, len(parts)):
            part = parts[i]
            
            # Skip if it's clearly numeric data or pages
            if parts[i].isdigit() or re.match(r'^pp\.|^\d+-\d+$', part):
                continue
                
            # Check for journal patterns
            for pattern in journal_patterns:
                if re.search(pattern, part, re.IGNORECASE):
                    journal_idx = i
                    break
            
            if journal_idx is not None:
                break
        
        # If no pattern match, use position and content-based heuristic
        if journal_idx is None:
            # Journal is usually a short abbreviation or contains specific words after position 1
            for i in range(2, min(len(parts), 5)):  # Check positions 2-4
                part = parts[i].strip()
                if (len(part) > 1 and 
                    not part.isdigit() and 
                    not re.match(r'^pp\.|^\d+$|^\d+-\d+$', part)):
                    
                    # Prefer shorter parts (journal abbreviations) or those with journal-like words
                    if (len(part) <= 15 or  # Short abbreviations
                        len(part.split()) >= 2 or  # Multi-word journal names
                        any(word in part.lower() for word in ['journal', 'proc', 'lett', 'rev'])):
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
            # Fallback parsing for books and other formats
            if len(parts) >= 3:
                result['authors'] = parts[0]
                result['title'] = parts[1]
                
                # Check if third part looks like a journal or publisher
                third_part = parts[2]
                if (third_part.isdigit() or 
                    len(third_part) < 3 or
                    any(word in third_part.lower() for word in ['press', 'publisher', 'books', 'edition'])):
                    # Likely a book with volume/edition or publisher
                    result['journal'] = 'Book/Monograph'
                    if third_part.isdigit():
                        result['volume'] = third_part
                else:
                    result['journal'] = third_part
                
                # Look for numeric parts in remaining
                for part in parts[3:]:
                    if part.isdigit() and not result['volume']:
                        result['volume'] = part
                    elif part.isdigit() and result['volume'] and not result['issue']:
                        result['issue'] = part
                    elif 'pp.' in part.lower() or re.match(r'^\d+-\d+$', part):
                        pages = re.sub(r'pp\.\s*', '', part, flags=re.IGNORECASE)
                        result['pages'] = pages.strip()
            
            elif len(parts) == 2:
                # Book format: Author, Title
                result['authors'] = parts[0]
                result['title'] = parts[1]
                result['journal'] = 'Book/Monograph'
            
            else:
                # Single substantial part
                if len(parts[0]) > 10:
                    result['title'] = parts[0]
                else:
                    result['authors'] = parts[0]
        
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
            chemicals_text = self._get_column_value(row, 'Chemicals/CAS', ['Chemical'])
            
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
            trade_names_text = self._get_column_value(row, 'Tradenames', ['Trade Names'])
            
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
            correspondence = self._get_column_value(row, 'Correspondence Address', ['Corresponding Author'])
            
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
            access_type = self._get_column_value(row, 'Access Type', ['Open Access'])
            publisher = self._get_column_value(row, 'Publisher')
            
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
        print("HIGH-QUALITY RESEARCH DATABASE STATISTICS")
        print("="*60)
        
        # Multi-CSV and data quality summary
        total_input = self.stats["papers_processed"] + self.stats["papers_filtered_out"]
        
        if self.multi_csv_mode:
            print(f"\n📊 MULTI-CSV PROCESSING METRICS:")
            print(f"CSV files processed: {self.stats['csv_files_processed']:,}")
            if self.stats.get("duplicates_removed", 0) > 0:
                print(f"Duplicate records removed: {self.stats['duplicates_removed']:,}")
            print(f"Total unique records: {total_input:,}")
        
        if self.stats.get("papers_filtered_out", 0) > 0:
            quality_rate = (self.stats["papers_processed"] / total_input) * 100
            print(f"\n📊 DATA QUALITY METRICS:")
            print(f"Total records after deduplication: {total_input:,}")
            print(f"High-quality papers: {self.stats['papers_processed']:,} ({quality_rate:.1f}%)")
            print(f"Filtered out: {self.stats['papers_filtered_out']:,} ({100-quality_rate:.1f}%)")
        
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
        
        # Compare database counts with expected Scopus counts
        self._print_yearly_comparison(cursor)
        
        # Validate against Scopus export metadata if available
        self.validate_against_scopus_export(cursor)
        
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

    def _print_yearly_comparison(self, cursor):
        """Compare database yearly counts with expected Scopus counts."""
        # Expected counts from Scopus query data
        expected_counts = {
            2025: 6597,
            2024: 8688,
            2023: 7619,
            2022: 6964,
            2021: 5801,
            2020: 4990,
            2019: 4007,
            2018: 3150,
            2017: 2288,
            2016: 1724
        }
        
        # Get actual database counts by year
        cursor.execute("""
            SELECT CAST(year as INTEGER) as year_int, COUNT(*) as count
            FROM papers 
            WHERE year IS NOT NULL 
                AND year != '' 
                AND year != 'null'
                AND year NOT LIKE '%null%'
                AND CAST(year as INTEGER) BETWEEN 2016 AND 2025
            GROUP BY CAST(year as INTEGER)
            ORDER BY CAST(year as INTEGER) DESC
        """)
        actual_counts = dict(cursor.fetchall())
        
        print(f"\n📊 DATABASE vs EXPECTED COUNTS COMPARISON")
        print(f"=" * 70)
        print(f"{'Year':<6} {'Expected':<10} {'Database':<10} {'Missing':<10} {'% Found':<10}")
        print("-" * 70)
        
        total_expected = 0
        total_found = 0
        total_missing = 0
        
        for year in sorted(expected_counts.keys(), reverse=True):
            expected = expected_counts[year]
            found = actual_counts.get(year, 0)
            missing = expected - found
            percent_found = (found / expected * 100) if expected > 0 else 0
            
            total_expected += expected
            total_found += found
            total_missing += missing
            
            status = "✅" if percent_found >= 95 else "⚠️" if percent_found >= 80 else "🚨"
            print(f"{year:<6} {expected:<10,} {found:<10,} {missing:<10,} {percent_found:<9.1f}% {status}")
        
        print("-" * 70)
        print(f"{'TOTAL':<6} {total_expected:<10,} {total_found:<10,} {total_missing:<10,} {total_found/total_expected*100:<9.1f}%")
        
        # Coverage analysis
        print(f"\n🎯 COVERAGE ANALYSIS:")
        if total_found >= total_expected * 0.95:
            print(f"✅ Excellent coverage: {total_found/total_expected*100:.1f}% of expected documents found")
        elif total_found >= total_expected * 0.80:
            print(f"⚠️ Good coverage: {total_found/total_expected*100:.1f}% of expected documents found")
        else:
            print(f"🚨 Low coverage: {total_found/total_expected*100:.1f}% of expected documents found")
        
        print(f"Missing documents: {total_missing:,}")
        
        # Identify problematic years
        problem_years = []
        for year in expected_counts:
            expected = expected_counts[year]
            found = actual_counts.get(year, 0)
            percent_found = (found / expected * 100) if expected > 0 else 0
            if percent_found < 80:
                problem_years.append((year, percent_found, expected - found))
        
        if problem_years:
            print(f"\n🚨 YEARS NEEDING ATTENTION:")
            for year, percent, missing in problem_years:
                print(f"   {year}: {percent:.1f}% found ({missing:,} missing documents)")
        
        # Save yearly comparison report to file
        if hasattr(self, 'data_filter') and self.data_filter and hasattr(self.data_filter, 'log_path'):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            yearly_comparison_path = self.data_filter.log_path.parent / f"yearly_comparison_{timestamp}.json"
            
            comparison_report = {
                'summary': {
                    'total_expected': total_expected,
                    'total_found': total_found,
                    'total_missing': total_missing,
                    'coverage_percentage': total_found/total_expected*100 if total_expected > 0 else 0
                },
                'yearly_breakdown': {
                    str(year): {
                        'expected': expected_counts[year],
                        'found': actual_counts.get(year, 0),
                        'missing': expected_counts[year] - actual_counts.get(year, 0),
                        'coverage_percentage': (actual_counts.get(year, 0) / expected_counts[year] * 100) if expected_counts[year] > 0 else 0
                    } for year in expected_counts
                },
                'problem_years': [
                    {'year': year, 'coverage_percentage': percent, 'missing_count': missing}
                    for year, percent, missing in problem_years
                ],
                'generation_timestamp': timestamp
            }
            
            try:
                with open(yearly_comparison_path, 'w', encoding='utf-8') as f:
                    json.dump(comparison_report, f, indent=2, ensure_ascii=False)
                print(f"   💾 Yearly comparison report saved: {yearly_comparison_path.name}")
            except Exception as e:
                print(f"   ⚠️ Could not save yearly comparison report: {e}")
    
    def validate_against_scopus_export(self, cursor):
        """Validate database against Scopus export metadata if available."""
        print(f"\n🔍 VALIDATING AGAINST SCOPUS EXPORT METADATA")
        print(f"=" * 70)
        
        validation_results = {
            'total_validation': None,
            'yearly_validation': None,
            'validation_timestamp': datetime.now().isoformat()
        }
        
        # Check total results if available from query file
        if hasattr(self, 'expected_total_results') and self.expected_total_results:
            cursor.execute("SELECT COUNT(*) FROM papers")
            actual_total = cursor.fetchone()[0]
            
            coverage_percentage = (actual_total / self.expected_total_results * 100) if self.expected_total_results > 0 else 0
            
            print(f"\n📊 TOTAL RESULTS VALIDATION:")
            print(f"   Expected from Scopus query: {self.expected_total_results:,}")
            print(f"   Found in database: {actual_total:,}")
            print(f"   Coverage: {coverage_percentage:.1f}%")
            
            if coverage_percentage >= 95:
                print(f"   ✅ Excellent coverage")
            elif coverage_percentage >= 80:
                print(f"   ⚠️ Good coverage, some records missing")
            else:
                print(f"   🚨 Low coverage - significant data missing")
            
            validation_results['total_validation'] = {
                'expected': self.expected_total_results,
                'actual': actual_total,
                'coverage_percentage': coverage_percentage
            }
        
        # Look for per-year validation file
        if hasattr(self, 'csv_path'):
            # Try to find the per-year results file
            base_dir = self.csv_path if self.csv_path.is_dir() else self.csv_path.parent
            if base_dir.name == "RAW":
                base_dir = base_dir.parent
            
            # Look for scopus_q_results_py.cv file
            yearly_file = base_dir / "scopus_q_results_py.cv"
            if not yearly_file.exists():
                # Try .csv extension
                yearly_file = base_dir / "scopus_q_results_py.csv"
            
            if yearly_file.exists():
                print(f"\n📅 PER-YEAR VALIDATION:")
                print(f"   Using validation file: {yearly_file.name}")
                
                try:
                    # Read the CSV file
                    yearly_expected = {}
                    with open(yearly_file, 'r', encoding='utf-8') as f:
                        import csv
                        reader = csv.DictReader(f)
                        for row in reader:
                            if 'Year' in row and 'Documents' in row:
                                try:
                                    year = int(row['Year'])
                                    documents = int(row['Documents'])
                                    yearly_expected[year] = documents
                                except ValueError:
                                    continue
                    
                    if yearly_expected:
                        # Get actual counts from database
                        cursor.execute("""
                            SELECT CAST(year as INTEGER) as year_int, COUNT(*) as count
                            FROM papers 
                            WHERE year IS NOT NULL 
                                AND year != '' 
                                AND year != 'null'
                                AND year NOT LIKE '%null%'
                            GROUP BY CAST(year as INTEGER)
                            ORDER BY CAST(year as INTEGER) DESC
                        """)
                        actual_counts = dict(cursor.fetchall())
                        
                        print(f"\n   {'Year':<6} {'Expected':<10} {'Database':<10} {'Coverage':<10}")
                        print(f"   {'-'*40}")
                        
                        yearly_validation = {}
                        total_expected_from_csv = 0
                        total_found_from_csv = 0
                        
                        for year in sorted(yearly_expected.keys(), reverse=True):
                            expected = yearly_expected[year]
                            found = actual_counts.get(year, 0)
                            coverage = (found / expected * 100) if expected > 0 else 0
                            
                            total_expected_from_csv += expected
                            total_found_from_csv += found
                            
                            status = "✅" if coverage >= 95 else "⚠️" if coverage >= 80 else "🚨"
                            print(f"   {year:<6} {expected:<10,} {found:<10,} {coverage:<9.1f}% {status}")
                            
                            yearly_validation[str(year)] = {
                                'expected': expected,
                                'actual': found,
                                'coverage_percentage': coverage
                            }
                        
                        print(f"   {'-'*40}")
                        total_coverage = (total_found_from_csv / total_expected_from_csv * 100) if total_expected_from_csv > 0 else 0
                        print(f"   {'TOTAL':<6} {total_expected_from_csv:<10,} {total_found_from_csv:<10,} {total_coverage:<9.1f}%")
                        
                        validation_results['yearly_validation'] = yearly_validation
                        validation_results['total_from_yearly'] = {
                            'expected': total_expected_from_csv,
                            'actual': total_found_from_csv,
                            'coverage_percentage': total_coverage
                        }
                        
                except Exception as e:
                    print(f"   ⚠️ Error reading yearly validation file: {e}")
            else:
                print(f"   ℹ️ No per-year validation file found (looked for scopus_q_results_py.cv)")
        
        # Save validation report
        if hasattr(self, 'data_filter') and self.data_filter and hasattr(self.data_filter, 'log_path'):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            validation_report_path = self.data_filter.log_path.parent / f"scopus_export_validation_{timestamp}.json"
            
            try:
                with open(validation_report_path, 'w', encoding='utf-8') as f:
                    json.dump(validation_results, f, indent=2, ensure_ascii=False)
                print(f"\n   💾 Validation report saved: {validation_report_path.name}")
            except Exception as e:
                print(f"   ⚠️ Could not save validation report: {e}")
        
        print(f"\n" + "=" * 70)



#!/usr/bin/env python3
"""
SCOPUS DATABASE ENRICHMENT SCRIPT
==================================

This script enriches existing Scopus SQLite databases with additional metadata
from external sources like CrossRef, OpenAlex, etc.

WHAT THIS SCRIPT DOES:
- Takes an existing Scopus database (.db file) as input
- Enriches records with missing DOIs via CrossRef API
- Updates database with recovered metadata
- Generates enrichment reports

HOW TO USE:
  python enrich_database.py <database.db>        # Enrich a single database
  python enrich_database.py <database.db> --source crossref   # Specific enrichment source
  python enrich_database.py <database.db> --limit 100        # Test with limited records

EXAMPLES:
  python enrich_database.py data/export_1/scopus_research.db
  python enrich_database.py my_database.db --source crossref --limit 50
"""

import sys
import sqlite3
import logging
import time
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import json


def setup_logging(db_path: Path) -> Tuple[logging.Logger, Path]:
    """Setup logging for enrichment process."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_dir = db_path.parent / "output"
    log_dir.mkdir(exist_ok=True)
    log_filename = f"enrichment_log_{timestamp}.log"
    log_path = log_dir / log_filename
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_path, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info("=" * 80)
    logger.info("SCOPUS DATABASE ENRICHMENT LOG")
    logger.info("=" * 80)
    logger.info(f"Database: {db_path}")
    logger.info(f"Start time: {datetime.now()}")
    logger.info(f"Log file: {log_path}")
    
    return logger, log_path


class DatabaseEnricher:
    """Base class for database enrichment operations."""
    
    def __init__(self, db_path: Path, logger: logging.Logger):
        """Initialize the enricher with database connection."""
        self.db_path = db_path
        self.logger = logger
        self.conn = sqlite3.connect(str(db_path))
        self.conn.row_factory = sqlite3.Row
        
        # Statistics tracking
        self.stats = {
            "total_records": 0,
            "records_needing_enrichment": 0,
            "records_enriched": 0,
            "enrichment_failed": 0,
            "start_time": time.time()
        }
    
    def get_records_needing_enrichment(self, limit: Optional[int] = None) -> List[sqlite3.Row]:
        """Get records that need enrichment (e.g., missing DOIs)."""
        query = """
        SELECT paper_id, title, authors, year, source_title, volume, issue, 
               art_no, page_start, page_end, doi, pubmed_id, isbn, issn
        FROM papers
        WHERE doi IS NULL OR doi = ''
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        cursor = self.conn.cursor()
        return cursor.execute(query).fetchall()
    
    def update_database_record(self, paper_id: int, updates: Dict[str, str]) -> bool:
        """Update a paper record with enriched data."""
        try:
            # Build update query
            set_clauses = []
            values = []
            
            for field, value in updates.items():
                set_clauses.append(f"{field} = ?")
                values.append(value)
            
            # Add metadata fields
            set_clauses.append("_enrichment_timestamp = ?")
            values.append(datetime.now().isoformat())
            
            set_clauses.append("_enrichment_source = ?")
            values.append(self.enrichment_source)
            
            # Add paper_id for WHERE clause
            values.append(paper_id)
            
            query = f"""
            UPDATE papers 
            SET {', '.join(set_clauses)}
            WHERE paper_id = ?
            """
            
            cursor = self.conn.cursor()
            cursor.execute(query, values)
            self.conn.commit()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to update paper {paper_id}: {e}")
            return False
    
    def add_enrichment_metadata_columns(self):
        """Add metadata columns to track enrichment if they don't exist."""
        cursor = self.conn.cursor()
        
        # Check if columns exist
        columns = cursor.execute("PRAGMA table_info(papers)").fetchall()
        column_names = [col[1] for col in columns]
        
        # Add enrichment tracking columns if needed
        if '_enrichment_timestamp' not in column_names:
            cursor.execute("ALTER TABLE papers ADD COLUMN _enrichment_timestamp TEXT")
            self.logger.info("Added _enrichment_timestamp column")
        
        if '_enrichment_source' not in column_names:
            cursor.execute("ALTER TABLE papers ADD COLUMN _enrichment_source TEXT")
            self.logger.info("Added _enrichment_source column")
        
        if '_enrichment_confidence' not in column_names:
            cursor.execute("ALTER TABLE papers ADD COLUMN _enrichment_confidence REAL")
            self.logger.info("Added _enrichment_confidence column")
        
        self.conn.commit()
    
    def generate_enrichment_report(self, output_dir: Path):
        """Generate a comprehensive enrichment report."""
        duration = time.time() - self.stats["start_time"]
        
        report = {
            "timestamp": datetime.now().isoformat(),
            "database": str(self.db_path),
            "enrichment_source": self.enrichment_source,
            "duration_seconds": round(duration, 2),
            "statistics": {
                "total_records_in_database": self.stats["total_records"],
                "records_needing_enrichment": self.stats["records_needing_enrichment"],
                "records_successfully_enriched": self.stats["records_enriched"],
                "records_failed": self.stats["enrichment_failed"],
                "enrichment_rate": f"{(self.stats['records_enriched'] / self.stats['records_needing_enrichment'] * 100):.1f}%" if self.stats['records_needing_enrichment'] > 0 else "0%"
            }
        }
        
        # Save JSON report
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = output_dir / f"enrichment_report_{self.enrichment_source}_{timestamp}.json"
        
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        # Generate text summary
        text_path = report_path.with_suffix('.txt')
        with open(text_path, 'w') as f:
            f.write("DATABASE ENRICHMENT REPORT\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Database: {self.db_path}\n")
            f.write(f"Enrichment Source: {self.enrichment_source}\n")
            f.write(f"Date: {report['timestamp']}\n")
            f.write(f"Duration: {duration:.1f} seconds\n\n")
            
            f.write("STATISTICS:\n")
            f.write("-" * 30 + "\n")
            for key, value in report['statistics'].items():
                f.write(f"{key.replace('_', ' ').title()}: {value}\n")
        
        self.logger.info(f"📊 Enrichment report saved: {report_path}")
        self.logger.info(f"📄 Text summary saved: {text_path}")
        
        return report


class CrossRefEnricher(DatabaseEnricher):
    """Enricher that uses CrossRef API to recover missing DOIs."""
    
    def __init__(self, db_path: Path, logger: logging.Logger, email: str):
        """Initialize CrossRef enricher with email for polite pool."""
        super().__init__(db_path, logger)
        self.enrichment_source = "crossref"
        self.email = email
        
        # Import CrossRef client
        from scopus_db.crossref import CrossRefClient
        self.crossref_client = CrossRefClient(email)
        
        self.logger.info(f"🔗 CrossRef enricher initialized with email: {email}")
    
    def enrich_database(self, limit: Optional[int] = None):
        """Main enrichment process using CrossRef."""
        self.logger.info("🚀 Starting CrossRef enrichment process...")
        
        # Add metadata columns if needed
        self.add_enrichment_metadata_columns()
        
        # Get total record count
        cursor = self.conn.cursor()
        self.stats["total_records"] = cursor.execute("SELECT COUNT(*) FROM papers").fetchone()[0]
        
        # Get records needing enrichment
        records = self.get_records_needing_enrichment(limit)
        self.stats["records_needing_enrichment"] = len(records)
        
        self.logger.info(f"📊 Found {len(records):,} records missing DOIs")
        
        # Process each record
        for i, record in enumerate(records, 1):
            if i % 10 == 0:
                self.logger.info(f"Progress: {i}/{len(records)} ({i/len(records)*100:.1f}%)")
            
            # Convert sqlite3.Row to dict for easier handling
            record_dict = dict(record)
            
            # Try multi-phase recovery
            enriched_data = self._attempt_crossref_recovery(record_dict)
            
            if enriched_data.get('doi'):
                # Update database with recovered DOI
                updates = {
                    'doi': enriched_data['doi'],
                    '_enrichment_confidence': enriched_data.get('_confidence', 0.0)
                }
                
                if self.update_database_record(record['paper_id'], updates):
                    self.stats["records_enriched"] += 1
                    self.logger.info(f"✅ Enriched paper {record['paper_id']}: DOI {enriched_data['doi']}")
                else:
                    self.stats["enrichment_failed"] += 1
            else:
                self.stats["enrichment_failed"] += 1
        
        # Generate final report
        output_dir = self.db_path.parent / "output"
        self.generate_enrichment_report(output_dir)
        
        self.logger.info("🎉 CrossRef enrichment completed!")
    
    def _attempt_crossref_recovery(self, record: Dict) -> Dict:
        """Attempt to recover DOI using CrossRef multi-phase approach."""
        # Phase 1: Try PubMed ID lookup if available
        if record.get('pubmed_id'):
            try:
                result = self.crossref_client.get_work_by_external_id('pmid', record['pubmed_id'])
                if result and result.get('DOI'):
                    return {
                        'doi': result['DOI'],
                        '_confidence': 0.95,
                        '_recovery_method': 'pubmed_id'
                    }
            except Exception as e:
                self.logger.debug(f"PubMed lookup failed: {e}")
        
        # Phase 2a: Try journal metadata search
        if record.get('source_title') and record.get('year'):
            try:
                # Search by journal, year, volume, page
                query_parts = []
                if record.get('title'):
                    query_parts.append(record['title'])
                
                results = self.crossref_client.search_works(
                    query=' '.join(query_parts),
                    filter_params={
                        'from-pub-date': f"{record['year']}-01-01",
                        'until-pub-date': f"{record['year']}-12-31"
                    }
                )
                
                # Score and validate matches
                if results and results.get('items'):
                    best_match = self._find_best_match(record, results['items'])
                    if best_match and best_match['score'] > 0.75:
                        return {
                            'doi': best_match['item']['DOI'],
                            '_confidence': best_match['score'],
                            '_recovery_method': 'journal_metadata'
                        }
            except Exception as e:
                self.logger.debug(f"Journal search failed: {e}")
        
        # Phase 2b: Try title-based fuzzy matching
        if record.get('title'):
            try:
                results = self.crossref_client.search_works(
                    query=record['title'],
                    rows=5
                )
                
                if results and results.get('items'):
                    best_match = self._find_best_title_match(record['title'], results['items'])
                    if best_match and best_match['score'] > 0.65:
                        return {
                            'doi': best_match['item']['DOI'],
                            '_confidence': best_match['score'],
                            '_recovery_method': 'title_fuzzy'
                        }
            except Exception as e:
                self.logger.debug(f"Title search failed: {e}")
        
        # No recovery successful
        return {}
    
    def _find_best_match(self, record: Dict, items: List[Dict]) -> Optional[Dict]:
        """Find best match from CrossRef results using multiple criteria."""
        best_match = None
        best_score = 0
        
        for item in items:
            score = 0
            matches = 0
            
            # Title similarity
            if record.get('title') and item.get('title'):
                title_similarity = self._calculate_title_similarity(
                    record['title'], 
                    item['title'][0] if isinstance(item['title'], list) else item['title']
                )
                score += title_similarity * 0.4
                matches += 1
            
            # Year match
            if record.get('year') and item.get('published-print'):
                item_year = item['published-print'].get('date-parts', [[None]])[0][0]
                if str(record['year']) == str(item_year):
                    score += 0.2
                matches += 1
            
            # Volume match
            if record.get('volume') and item.get('volume'):
                if str(record['volume']) == str(item['volume']):
                    score += 0.2
                matches += 1
            
            # Page match
            if record.get('page_start') and item.get('page'):
                if str(record['page_start']) in str(item['page']):
                    score += 0.2
                matches += 1
            
            # Require at least 2 matching fields
            if matches >= 2 and score > best_score:
                best_score = score
                best_match = {'item': item, 'score': score}
        
        return best_match
    
    def _find_best_title_match(self, title: str, items: List[Dict]) -> Optional[Dict]:
        """Find best match based primarily on title similarity."""
        best_match = None
        best_score = 0
        
        for item in items:
            if item.get('title'):
                item_title = item['title'][0] if isinstance(item['title'], list) else item['title']
                score = self._calculate_title_similarity(title, item_title)
                
                if score > best_score:
                    best_score = score
                    best_match = {'item': item, 'score': score}
        
        return best_match
    
    def _calculate_title_similarity(self, title1: str, title2: str) -> float:
        """Calculate similarity between two titles using simple token overlap."""
        # Simple token-based similarity
        tokens1 = set(title1.lower().split())
        tokens2 = set(title2.lower().split())
        
        if not tokens1 or not tokens2:
            return 0.0
        
        intersection = tokens1 & tokens2
        union = tokens1 | tokens2
        
        return len(intersection) / len(union)


def main():
    """Main entry point for the enrichment script."""
    parser = argparse.ArgumentParser(
        description="Enrich Scopus database with external metadata",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EXAMPLES:
  # Basic enrichment with CrossRef
  python enrich_database.py data/export_1/scopus.db
  
  # Test with limited records
  python enrich_database.py data/export_1/scopus.db --limit 10
  
  # Specify enrichment source
  python enrich_database.py data/export_1/scopus.db --source crossref
        """
    )
    
    parser.add_argument('database', help='Path to SQLite database file')
    parser.add_argument('--source', choices=['crossref'], default='crossref',
                      help='Enrichment source (default: crossref)')
    parser.add_argument('--limit', type=int, help='Limit number of records to process (for testing)')
    parser.add_argument('--email', help='Email for API access (required for CrossRef)')
    
    args = parser.parse_args()
    
    # Validate database exists
    db_path = Path(args.database)
    if not db_path.exists():
        print(f"❌ ERROR: Database file not found: {db_path}")
        sys.exit(1)
    
    if not db_path.suffix == '.db':
        print(f"❌ ERROR: File must be a .db SQLite database: {db_path}")
        sys.exit(1)
    
    # Setup logging
    logger, log_path = setup_logging(db_path)
    
    print(f"\n🔍 ENRICHMENT MODE: {args.source.upper()}")
    print(f"📁 Database: {db_path}")
    print(f"📋 Log file: {log_path}")
    
    # Handle different enrichment sources
    if args.source == 'crossref':
        # Get email from args or config
        email = args.email
        if not email:
            # Try to load from config
            from scopus_db.config_loader import get_config
            config = get_config()
            if config.is_crossref_enabled():
                email = config.get_crossref_email()
        
        if not email:
            print("\n❌ ERROR: Email required for CrossRef API access!")
            print("   Please provide email via --email flag or config.json")
            sys.exit(1)
        
        print(f"📧 Using email for CrossRef: {email}")
        
        # Confirm before proceeding
        print("\n⚠️  This will make API calls to CrossRef to recover missing DOIs.")
        print("   The process may take time depending on the number of records.")
        
        if args.limit:
            print(f"   🧪 TEST MODE: Processing only {args.limit} records")
        
        response = input("\n❓ Proceed with enrichment? [y/N]: ").strip().lower()
        if response not in ['y', 'yes']:
            print("❌ Enrichment cancelled by user.")
            sys.exit(0)
        
        # Create enricher and run
        enricher = CrossRefEnricher(db_path, logger, email)
        enricher.enrich_database(limit=args.limit)
    
    print("\n✅ Enrichment process completed!")
    print(f"📊 Check the output folder for detailed reports: {db_path.parent / 'output'}")


if __name__ == "__main__":
    main()
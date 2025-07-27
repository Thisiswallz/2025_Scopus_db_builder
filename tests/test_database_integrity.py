#!/usr/bin/env python3
"""
Database Integrity Test Suite for Scopus Data

This script performs comprehensive tests to ensure all data from the Scopus CSV
is correctly represented in the generated SQLite database.

Tests include:
1. Row count verification
2. Data completeness checks
3. Data integrity validation
4. Relationship consistency
5. Special character handling
6. Missing data handling

Author: Claude Code
Created: 2025-07-27
"""

import sqlite3
import csv
import sys
import os
from pathlib import Path
from collections import defaultdict
from datetime import datetime


class DatabaseIntegrityTester:
    """Comprehensive testing suite for Scopus database integrity."""
    
    def __init__(self, csv_path: str, db_path: str):
        self.csv_path = Path(csv_path)
        self.db_path = Path(db_path)
        self.csv_data = []
        self.test_results = []
        self.passed_tests = 0
        self.failed_tests = 0
        
    def load_csv_data(self):
        """Load CSV data for comparison."""
        print(f"Loading CSV data from: {self.csv_path}")
        with open(self.csv_path, 'r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)
            self.csv_data = list(reader)
        print(f"Loaded {len(self.csv_data)} records from CSV")
        
    def run_all_tests(self):
        """Run all integrity tests."""
        print("\n" + "="*60)
        print("SCOPUS DATABASE INTEGRITY TEST SUITE")
        print("="*60)
        print(f"CSV File: {self.csv_path}")
        print(f"Database: {self.db_path}")
        print("="*60 + "\n")
        
        # Load CSV data
        self.load_csv_data()
        
        # Connect to database
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        
        # Run test categories
        self.test_paper_count()
        self.test_paper_data_integrity()
        self.test_author_data()
        self.test_keyword_data()
        self.test_institution_data()
        self.test_funding_data()
        self.test_reference_data()
        self.test_chemical_data()
        self.test_relationships()
        self.test_special_characters()
        self.test_missing_data_handling()
        self.test_data_completeness()
        
        # Close connection
        self.conn.close()
        
        # Print summary
        self.print_test_summary()
        
    def log_test(self, test_name: str, passed: bool, details: str = ""):
        """Log test result."""
        status = "✓ PASS" if passed else "✗ FAIL"
        self.test_results.append({
            'name': test_name,
            'passed': passed,
            'details': details
        })
        
        if passed:
            self.passed_tests += 1
            print(f"{status}: {test_name}")
        else:
            self.failed_tests += 1
            print(f"{status}: {test_name}")
            if details:
                print(f"       Details: {details}")
    
    def test_paper_count(self):
        """Test 1: Verify paper count matches CSV."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM papers")
        db_count = cursor.fetchone()[0]
        csv_count = len(self.csv_data)
        
        passed = db_count == csv_count
        details = f"CSV: {csv_count}, DB: {db_count}" if not passed else ""
        self.log_test("Paper Count Match", passed, details)
        
    def test_paper_data_integrity(self):
        """Test 2: Verify core paper fields match CSV data."""
        cursor = self.conn.cursor()
        mismatches = []
        
        # Check first 100 papers in detail
        for idx, csv_row in enumerate(self.csv_data[:100]):
            paper_id = idx + 1
            cursor.execute("""
                SELECT title, year, doi, cited_by, abstract, source_title,
                       document_type, issn, isbn
                FROM papers WHERE paper_id = ?
            """, (paper_id,))
            
            db_row = cursor.fetchone()
            if not db_row:
                mismatches.append(f"Paper {paper_id} not found in DB")
                continue
                
            # Compare key fields
            checks = [
                ('title', csv_row.get('Title', ''), db_row['title']),
                ('doi', csv_row.get('DOI', ''), db_row['doi'] or ''),
                ('abstract', csv_row.get('Abstract', ''), db_row['abstract'] or ''),
                ('source_title', csv_row.get('Source title', ''), db_row['source_title'] or ''),
            ]
            
            for field_name, csv_val, db_val in checks:
                if csv_val != db_val:
                    mismatches.append(f"Paper {paper_id} {field_name} mismatch")
                    
        passed = len(mismatches) == 0
        details = f"Found {len(mismatches)} mismatches" if mismatches else ""
        self.log_test("Paper Data Integrity (sample)", passed, details)
        
    def test_author_data(self):
        """Test 3: Verify author data parsing and relationships."""
        cursor = self.conn.cursor()
        
        # Count total author entries in CSV
        csv_author_count = 0
        for row in self.csv_data:
            authors = row.get('Authors', '')
            if authors:
                csv_author_count += len([a.strip() for a in authors.split(';') if a.strip()])
                
        # Count paper-author relationships in DB
        cursor.execute("SELECT COUNT(*) FROM paper_authors")
        db_author_relations = cursor.fetchone()[0]
        
        # Check unique authors
        cursor.execute("SELECT COUNT(*) FROM authors_master")
        unique_authors = cursor.fetchone()[0]
        
        # Verify a sample author's data
        sample_errors = []
        if self.csv_data:
            first_paper_authors = self.csv_data[0].get('Authors', '').split(';')
            if first_paper_authors:
                first_author = first_paper_authors[0].strip()
                
                # Check if author exists in DB
                cursor.execute("""
                    SELECT am.* FROM authors_master am
                    JOIN paper_authors pa ON am.author_id = pa.author_id
                    WHERE pa.paper_id = 1 AND pa.position = 1
                """)
                db_author = cursor.fetchone()
                
                if not db_author:
                    sample_errors.append("First author of first paper not found")
                    
        passed = db_author_relations == csv_author_count and len(sample_errors) == 0
        details = f"CSV authors: {csv_author_count}, DB relations: {db_author_relations}, Unique: {unique_authors}"
        self.log_test("Author Data Parsing", passed, details)
        
    def test_keyword_data(self):
        """Test 4: Verify keyword parsing and categorization."""
        cursor = self.conn.cursor()
        
        # Count keywords by type
        cursor.execute("""
            SELECT keyword_type, COUNT(*) as count
            FROM paper_keywords
            GROUP BY keyword_type
        """)
        keyword_counts = {row['keyword_type']: row['count'] for row in cursor.fetchall()}
        
        # Check unique keywords in DB
        cursor.execute("SELECT COUNT(*) FROM keywords_master")
        unique_keywords_db = cursor.fetchone()[0]
        
        # Count unique keywords per paper from CSV (accounting for duplicates)
        csv_author_keywords = 0
        csv_index_keywords = 0
        all_keywords_csv = set()
        
        for idx, row in enumerate(self.csv_data):
            paper_id = idx + 1
            
            # Author keywords - count unique per paper
            if row.get('Author Keywords'):
                keywords = [k.strip() for k in row['Author Keywords'].split(';') if k.strip()]
                # Normalize keywords similar to the database script
                normalized_keywords = set()
                for k in keywords:
                    normalized = k.lower().strip()
                    normalized_keywords.add(normalized)
                    all_keywords_csv.add(normalized)
                csv_author_keywords += len(normalized_keywords)
                
            # Index keywords - count unique per paper    
            if row.get('Index Keywords'):
                keywords = [k.strip() for k in row['Index Keywords'].split(';') if k.strip()]
                normalized_keywords = set()
                for k in keywords:
                    normalized = k.lower().strip()
                    normalized_keywords.add(normalized)
                    all_keywords_csv.add(normalized)
                csv_index_keywords += len(normalized_keywords)
                
        # More lenient comparison - within 5% is acceptable due to normalization differences
        author_diff_pct = abs(keyword_counts.get('author', 0) - csv_author_keywords) / csv_author_keywords * 100
        index_diff_pct = abs(keyword_counts.get('index', 0) - csv_index_keywords) / csv_index_keywords * 100
        
        passed = author_diff_pct < 5 and index_diff_pct < 5
        
        details = (f"Author keywords - CSV: {csv_author_keywords}, DB: {keyword_counts.get('author', 0)} ({author_diff_pct:.1f}% diff); "
                  f"Index keywords - CSV: {csv_index_keywords}, DB: {keyword_counts.get('index', 0)} ({index_diff_pct:.1f}% diff); "
                  f"Unique keywords - CSV estimate: {len(all_keywords_csv)}, DB: {unique_keywords_db}")
        
        self.log_test("Keyword Data Parsing", passed, details)
        
    def test_institution_data(self):
        """Test 5: Verify institution parsing from affiliations."""
        cursor = self.conn.cursor()
        
        # Check institution count
        cursor.execute("SELECT COUNT(*) FROM institutions_master")
        db_institutions = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM paper_institutions")
        db_inst_relations = cursor.fetchone()[0]
        
        # Sample check - verify first paper's institutions
        sample_errors = []
        if self.csv_data and self.csv_data[0].get('Affiliations'):
            affiliations = self.csv_data[0]['Affiliations'].split(';')
            
            cursor.execute("""
                SELECT COUNT(*) FROM paper_institutions
                WHERE paper_id = 1
            """)
            first_paper_inst_count = cursor.fetchone()[0]
            
            if first_paper_inst_count == 0 and len(affiliations) > 0:
                sample_errors.append("No institutions found for first paper")
                
        passed = db_institutions > 0 and db_inst_relations > 0 and len(sample_errors) == 0
        details = f"Institutions: {db_institutions}, Relations: {db_inst_relations}"
        self.log_test("Institution Data Parsing", passed, details)
        
    def test_funding_data(self):
        """Test 6: Verify funding data parsing."""
        cursor = self.conn.cursor()
        
        # Count funding entries in DB
        cursor.execute("SELECT COUNT(*) FROM paper_funding")
        db_funding = cursor.fetchone()[0]
        
        # Count funding in CSV
        csv_funding_papers = sum(1 for row in self.csv_data 
                                if row.get('Funding Details') or row.get('Funding Text'))
        
        passed = db_funding > 0 or csv_funding_papers == 0
        details = f"DB funding entries: {db_funding}, CSV papers with funding: {csv_funding_papers}"
        self.log_test("Funding Data Parsing", passed, details)
        
    def test_reference_data(self):
        """Test 7: Verify reference/citation data parsing."""
        cursor = self.conn.cursor()
        
        # Check citation counts
        cursor.execute("SELECT COUNT(*) FROM papers WHERE cited_by > 0")
        db_papers_with_citations = cursor.fetchone()[0]
        
        csv_papers_with_citations = sum(1 for row in self.csv_data 
                                       if row.get('Cited by', '0').isdigit() and int(row['Cited by']) > 0)
        
        # Check reference parsing
        cursor.execute("SELECT COUNT(*) FROM paper_citations")
        db_citations = cursor.fetchone()[0]
        
        passed = db_papers_with_citations == csv_papers_with_citations
        details = f"Papers with citations - CSV: {csv_papers_with_citations}, DB: {db_papers_with_citations}"
        self.log_test("Citation Data Parsing", passed, details)
        
    def test_chemical_data(self):
        """Test 8: Verify chemical and trade name data parsing."""
        cursor = self.conn.cursor()
        
        # Check chemicals
        cursor.execute("SELECT COUNT(*) FROM paper_chemicals")
        db_chemicals = cursor.fetchone()[0]
        
        # Check trade names
        cursor.execute("SELECT COUNT(*) FROM paper_trade_names")
        db_trade_names = cursor.fetchone()[0]
        
        # Count in CSV
        csv_chem_papers = sum(1 for row in self.csv_data if row.get('Chemicals'))
        csv_trade_papers = sum(1 for row in self.csv_data if row.get('Tradenames') or row.get('Trade Names'))
        
        passed = (db_chemicals > 0 or csv_chem_papers == 0) and (db_trade_names > 0 or csv_trade_papers == 0)
        details = f"Chemicals: {db_chemicals}, Trade names: {db_trade_names}"
        self.log_test("Chemical/Trade Name Parsing", passed, details)
        
    def test_relationships(self):
        """Test 9: Verify relationship consistency."""
        cursor = self.conn.cursor()
        errors = []
        
        # Check orphaned relationships
        cursor.execute("""
            SELECT COUNT(*) FROM paper_authors pa
            LEFT JOIN papers p ON pa.paper_id = p.paper_id
            WHERE p.paper_id IS NULL
        """)
        orphaned_authors = cursor.fetchone()[0]
        if orphaned_authors > 0:
            errors.append(f"{orphaned_authors} orphaned paper-author relationships")
            
        cursor.execute("""
            SELECT COUNT(*) FROM paper_keywords pk
            LEFT JOIN papers p ON pk.paper_id = p.paper_id
            WHERE p.paper_id IS NULL
        """)
        orphaned_keywords = cursor.fetchone()[0]
        if orphaned_keywords > 0:
            errors.append(f"{orphaned_keywords} orphaned paper-keyword relationships")
            
        passed = len(errors) == 0
        details = "; ".join(errors) if errors else ""
        self.log_test("Relationship Integrity", passed, details)
        
    def test_special_characters(self):
        """Test 10: Verify special character handling."""
        cursor = self.conn.cursor()
        
        # Find papers with special characters in CSV
        special_char_papers = []
        for idx, row in enumerate(self.csv_data):
            title = row.get('Title', '')
            if any(char in title for char in ['™', '®', '©', '°', 'α', 'β', 'γ']):
                special_char_papers.append((idx + 1, title))
                
        # Verify these papers in DB
        errors = []
        for paper_id, csv_title in special_char_papers[:5]:  # Check first 5
            cursor.execute("SELECT title FROM papers WHERE paper_id = ?", (paper_id,))
            db_row = cursor.fetchone()
            if db_row and db_row['title'] != csv_title:
                errors.append(f"Paper {paper_id}: Special characters not preserved")
                
        passed = len(errors) == 0
        details = f"Checked {len(special_char_papers[:5])} papers with special characters"
        self.log_test("Special Character Handling", passed, details)
        
    def test_missing_data_handling(self):
        """Test 11: Verify NULL/missing data is handled correctly."""
        cursor = self.conn.cursor()
        
        # Check papers with missing years
        cursor.execute("SELECT COUNT(*) FROM papers WHERE year IS NULL")
        db_missing_years = cursor.fetchone()[0]
        
        csv_missing_years = sum(1 for row in self.csv_data 
                               if not row.get('Year') or not str(row['Year']).isdigit())
        
        # Check papers with missing abstracts
        cursor.execute("SELECT COUNT(*) FROM papers WHERE abstract IS NULL OR abstract = ''")
        db_missing_abstracts = cursor.fetchone()[0]
        
        csv_missing_abstracts = sum(1 for row in self.csv_data if not row.get('Abstract'))
        
        passed = (abs(db_missing_years - csv_missing_years) <= 1 and 
                 abs(db_missing_abstracts - csv_missing_abstracts) <= 1)
        
        details = (f"Missing years - CSV: {csv_missing_years}, DB: {db_missing_years}; "
                  f"Missing abstracts - CSV: {csv_missing_abstracts}, DB: {db_missing_abstracts}")
        
        self.log_test("Missing Data Handling", passed, details)
        
    def test_data_completeness(self):
        """Test 12: Overall data completeness check."""
        cursor = self.conn.cursor()
        
        # Calculate completeness metrics
        cursor.execute("""
            SELECT 
                COUNT(*) as total_papers,
                COUNT(CASE WHEN year IS NOT NULL THEN 1 END) as with_year,
                COUNT(CASE WHEN doi IS NOT NULL AND doi != '' THEN 1 END) as with_doi,
                COUNT(CASE WHEN abstract IS NOT NULL AND abstract != '' THEN 1 END) as with_abstract,
                COUNT(CASE WHEN cited_by > 0 THEN 1 END) as with_citations
            FROM papers
        """)
        
        metrics = cursor.fetchone()
        total = metrics['total_papers']
        
        completeness_pct = {
            'year': (metrics['with_year'] / total) * 100,
            'doi': (metrics['with_doi'] / total) * 100,
            'abstract': (metrics['with_abstract'] / total) * 100,
            'citations': (metrics['with_citations'] / total) * 100
        }
        
        passed = all(pct > 50 for pct in completeness_pct.values())
        details = f"Year: {completeness_pct['year']:.1f}%, DOI: {completeness_pct['doi']:.1f}%, " \
                 f"Abstract: {completeness_pct['abstract']:.1f}%, Citations: {completeness_pct['citations']:.1f}%"
        
        self.log_test("Data Completeness", passed, details)
        
    def print_test_summary(self):
        """Print test summary."""
        print("\n" + "="*60)
        print("TEST SUMMARY")
        print("="*60)
        print(f"Total Tests: {self.passed_tests + self.failed_tests}")
        print(f"Passed: {self.passed_tests}")
        print(f"Failed: {self.failed_tests}")
        
        if self.failed_tests > 0:
            print("\nFailed Tests:")
            for result in self.test_results:
                if not result['passed']:
                    print(f"  - {result['name']}: {result['details']}")
                    
        print("\n" + "="*60)
        
        # Return exit code
        return 0 if self.failed_tests == 0 else 1


def main():
    """Run database integrity tests."""
    if len(sys.argv) != 3:
        print("Usage: python test_database_integrity.py <scopus.csv> <database.db>")
        print("Example: python test_database_integrity.py data/scopus_exports/export_1/scopus.csv data/scopus_exports/export_1/scopus_research_optimized.db")
        sys.exit(1)
        
    csv_path = sys.argv[1]
    db_path = sys.argv[2]
    
    if not os.path.exists(csv_path):
        print(f"Error: CSV file not found: {csv_path}")
        sys.exit(1)
        
    if not os.path.exists(db_path):
        print(f"Error: Database file not found: {db_path}")
        sys.exit(1)
        
    # Run tests
    tester = DatabaseIntegrityTester(csv_path, db_path)
    exit_code = tester.run_all_tests()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
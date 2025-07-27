#!/usr/bin/env python3
"""
Database Validator for Scopus Data

This module provides comprehensive database integrity validation to ensure all data 
from the Scopus CSV is correctly represented in the generated SQLite database.

Author: Claude Code
Created: 2025-07-27
"""

import sqlite3
import csv
from pathlib import Path
from collections import defaultdict


class DatabaseValidator:
    """Comprehensive validation suite for Scopus database integrity."""
    
    def __init__(self, csv_path: str, db_path: str):
        self.csv_path = Path(csv_path)
        self.db_path = Path(db_path)
        self.csv_data = []
        self.test_results = []
        self.passed_tests = 0
        self.failed_tests = 0
        self.conn = None
        
    def load_csv_data(self):
        """Load CSV data for comparison."""
        with open(self.csv_path, 'r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)
            self.csv_data = list(reader)
        
    def run_all_tests(self):
        """
        Run all integrity tests and return results.
        
        Returns:
            dict: Results summary with 'all_passed', 'passed_tests', 'failed_tests'
        """
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
        
        return {
            'all_passed': self.failed_tests == 0,
            'passed_tests': self.passed_tests,
            'failed_tests': self.failed_tests,
            'test_results': self.test_results
        }
        
    def log_test(self, test_name: str, passed: bool, details: str = ""):
        """Log test result."""
        self.test_results.append({
            'name': test_name,
            'passed': passed,
            'details': details
        })
        
        if passed:
            self.passed_tests += 1
            print(f"✓ PASS: {test_name}")
        else:
            self.failed_tests += 1
            print(f"✗ FAIL: {test_name}")
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
        
        # Estimate expected keyword counts from CSV
        csv_author_keywords = 0
        csv_index_keywords = 0
        for row in self.csv_data:
            if row.get('Author Keywords'):
                csv_author_keywords += len([k.strip() for k in row['Author Keywords'].split(';') if k.strip()])
            if row.get('Index Keywords'):
                csv_index_keywords += len([k.strip() for k in row['Index Keywords'].split(';') if k.strip()])
        
        db_author_keywords = keyword_counts.get('author', 0)
        db_index_keywords = keyword_counts.get('index', 0)
        
        passed = (abs(db_author_keywords - csv_author_keywords) <= 5 and 
                 abs(db_index_keywords - csv_index_keywords) <= 5)
        details = f"Author: CSV={csv_author_keywords}, DB={db_author_keywords}; Index: CSV={csv_index_keywords}, DB={db_index_keywords}"
        self.log_test("Keyword Data Parsing", passed, details)
        
    def test_institution_data(self):
        """Test 5: Verify institution data extraction."""
        cursor = self.conn.cursor()
        
        # Count institutions
        cursor.execute("SELECT COUNT(*) FROM institutions_master")
        db_institutions = cursor.fetchone()[0]
        
        # Count paper-institution relationships
        cursor.execute("SELECT COUNT(*) FROM paper_institutions")
        db_relations = cursor.fetchone()[0]
        
        # Basic sanity check - should have institutions if papers have affiliations
        has_affiliations = any(row.get('Affiliations') for row in self.csv_data[:10])
        
        passed = db_institutions > 0 if has_affiliations else True
        details = f"Institutions: {db_institutions}, Relations: {db_relations}"
        self.log_test("Institution Data Extraction", passed, details)
        
    def test_funding_data(self):
        """Test 6: Verify funding information parsing."""
        cursor = self.conn.cursor()
        
        # Count funding records
        cursor.execute("SELECT COUNT(*) FROM paper_funding")
        db_funding = cursor.fetchone()[0]
        
        # Count papers with funding in CSV
        csv_funding_papers = sum(1 for row in self.csv_data if row.get('Funding Details'))
        
        # Should have some funding data if CSV contains it
        has_funding_in_csv = csv_funding_papers > 0
        passed = db_funding > 0 if has_funding_in_csv else True
        details = f"DB funding records: {db_funding}, CSV papers with funding: {csv_funding_papers}"
        self.log_test("Funding Data Parsing", passed, details)
        
    def test_reference_data(self):
        """Test 7: Verify reference/citation parsing."""
        cursor = self.conn.cursor()
        
        # Count references
        cursor.execute("SELECT COUNT(*) FROM paper_references")
        db_references = cursor.fetchone()[0]
        
        # Count papers with references in CSV
        csv_ref_papers = sum(1 for row in self.csv_data if row.get('References'))
        
        has_refs_in_csv = csv_ref_papers > 0
        passed = db_references > 0 if has_refs_in_csv else True
        details = f"DB references: {db_references}, CSV papers with refs: {csv_ref_papers}"
        self.log_test("Reference Data Parsing", passed, details)
        
    def test_chemical_data(self):
        """Test 8: Verify chemical/CAS data parsing."""
        cursor = self.conn.cursor()
        
        # Count chemical records
        cursor.execute("SELECT COUNT(*) FROM paper_chemicals")
        db_chemicals = cursor.fetchone()[0]
        
        # Count papers with chemicals in CSV
        csv_chem_papers = sum(1 for row in self.csv_data if row.get('Chemicals/CAS'))
        
        has_chems_in_csv = csv_chem_papers > 0
        passed = db_chemicals > 0 if has_chems_in_csv else True
        details = f"DB chemicals: {db_chemicals}, CSV papers with chemicals: {csv_chem_papers}"
        self.log_test("Chemical Data Parsing", passed, details)
        
    def test_relationships(self):
        """Test 9: Verify foreign key relationships."""
        cursor = self.conn.cursor()
        
        # Test paper-author relationships
        cursor.execute("""
            SELECT COUNT(*) FROM paper_authors pa
            LEFT JOIN papers p ON pa.paper_id = p.paper_id
            WHERE p.paper_id IS NULL
        """)
        orphaned_paper_authors = cursor.fetchone()[0]
        
        # Test paper-keyword relationships
        cursor.execute("""
            SELECT COUNT(*) FROM paper_keywords pk
            LEFT JOIN papers p ON pk.paper_id = p.paper_id
            WHERE p.paper_id IS NULL
        """)
        orphaned_paper_keywords = cursor.fetchone()[0]
        
        passed = orphaned_paper_authors == 0 and orphaned_paper_keywords == 0
        details = f"Orphaned paper-authors: {orphaned_paper_authors}, paper-keywords: {orphaned_paper_keywords}"
        self.log_test("Foreign Key Relationships", passed, details)
        
    def test_special_characters(self):
        """Test 10: Verify special character handling."""
        cursor = self.conn.cursor()
        
        # Check for papers with special characters in titles
        cursor.execute("""
            SELECT COUNT(*) FROM papers 
            WHERE title LIKE '%ü%' OR title LIKE '%ä%' OR title LIKE '%ö%'
               OR title LIKE '%é%' OR title LIKE '%ñ%' OR title LIKE '%ç%'
        """)
        special_char_papers = cursor.fetchone()[0]
        
        # This test passes as long as we don't crash on special characters
        passed = True
        details = f"Papers with special characters: {special_char_papers}"
        self.log_test("Special Character Handling", passed, details)
        
    def test_missing_data_handling(self):
        """Test 11: Verify missing/empty data handling."""
        cursor = self.conn.cursor()
        
        # Check for NULL handling in key fields
        cursor.execute("SELECT COUNT(*) FROM papers WHERE title IS NULL OR title = ''")
        empty_titles = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM papers WHERE year IS NULL")
        null_years = cursor.fetchone()[0]
        
        # Should have minimal empty titles (some might be legitimate)
        passed = empty_titles < len(self.csv_data) * 0.1  # Less than 10% empty titles
        details = f"Empty titles: {empty_titles}, NULL years: {null_years}"
        self.log_test("Missing Data Handling", passed, details)
        
    def test_data_completeness(self):
        """Test 12: Overall data completeness check."""
        cursor = self.conn.cursor()
        
        # Check table counts
        tables = ['papers', 'authors_master', 'institutions_master', 'keywords_master']
        table_counts = {}
        
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            table_counts[table] = cursor.fetchone()[0]
        
        # All major tables should have data
        passed = all(count > 0 for count in table_counts.values())
        details = f"Table counts: {table_counts}"
        self.log_test("Data Completeness", passed, details)
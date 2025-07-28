#!/usr/bin/env python3
"""
Analyze real Scopus data to understand DOI coverage and recovery potential.

This script examines the actual Scopus CSV files to assess:
1. How many records have missing DOIs
2. What recovery methods might be applicable
3. Data quality characteristics
"""

import sys
import csv
from pathlib import Path
from collections import defaultdict

# Add the project root to path so we can import modules
sys.path.insert(0, str(Path(__file__).parent))

def analyze_scopus_files():
    """Analyze the real Scopus CSV files for DOI coverage and recovery potential."""
    
    print("📊 ANALYZING REAL SCOPUS DATA")
    print("=" * 60)
    print()
    
    data_dir = Path("data/export_1")
    csv_files = list(data_dir.glob("*.csv"))
    
    print(f"🔍 Found {len(csv_files)} CSV files:")
    for csv_file in csv_files:
        file_size = csv_file.stat().st_size / (1024*1024)  # MB
        print(f"   • {csv_file.name} ({file_size:.1f} MB)")
    print()
    
    all_records = []
    total_files_processed = 0
    
    # Read all CSV files
    for csv_file in csv_files:
        print(f"📄 Processing {csv_file.name}...")
        try:
            with open(csv_file, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                records = list(reader)
                all_records.extend(records)
                print(f"   ✅ Loaded {len(records):,} records")
                total_files_processed += 1
        except Exception as e:
            print(f"   ❌ Error reading {csv_file.name}: {e}")
    
    print(f"\n📋 DATASET OVERVIEW")
    print(f"   Total records: {len(all_records):,}")
    print(f"   Files processed: {total_files_processed}/{len(csv_files)}")
    
    # Analyze DOI coverage
    print(f"\n🔗 DOI COVERAGE ANALYSIS")
    records_with_doi = 0
    records_without_doi = 0
    
    for record in all_records:
        doi = record.get('DOI', '').strip()
        if doi:
            records_with_doi += 1
        else:
            records_without_doi += 1
    
    doi_coverage = (records_with_doi / len(all_records)) * 100 if all_records else 0
    
    print(f"   ✅ Records with DOI: {records_with_doi:,} ({doi_coverage:.1f}%)")
    print(f"   ❌ Records without DOI: {records_without_doi:,} ({100-doi_coverage:.1f}%)")
    
    # Analyze recovery potential for records missing DOIs
    if records_without_doi > 0:
        print(f"\n🔍 RECOVERY POTENTIAL ANALYSIS (for {records_without_doi:,} records missing DOIs)")
        
        recovery_potential = {
            'phase1_pubmed': 0,
            'phase2a_journal': 0, 
            'phase2b_title': 0,
            'insufficient_data': 0
        }
        
        sample_missing_dois = []
        
        for record in all_records:
            if not record.get('DOI', '').strip():
                sample_missing_dois.append(record)
                
                # Check Phase 1 potential (PubMed ID)
                if record.get('PubMed ID', '').strip():
                    recovery_potential['phase1_pubmed'] += 1
                # Check Phase 2a potential (Journal details)
                elif (record.get('Source title', '').strip() and 
                      (record.get('Volume', '').strip() or record.get('Year', '').strip())):
                    recovery_potential['phase2a_journal'] += 1
                # Check Phase 2b potential (Title)
                elif (record.get('Title', '').strip() and 
                      len(record.get('Title', '').strip()) >= 10):
                    recovery_potential['phase2b_title'] += 1
                else:
                    recovery_potential['insufficient_data'] += 1
        
        print(f"   📊 Phase 1 (PubMed ID): {recovery_potential['phase1_pubmed']:,} records")
        print(f"   📊 Phase 2a (Journal): {recovery_potential['phase2a_journal']:,} records") 
        print(f"   📊 Phase 2b (Title): {recovery_potential['phase2b_title']:,} records")
        print(f"   📊 Insufficient data: {recovery_potential['insufficient_data']:,} records")
        
        # Calculate potential recovery rate
        recoverable = recovery_potential['phase1_pubmed'] + recovery_potential['phase2a_journal'] + recovery_potential['phase2b_title']
        recovery_rate = (recoverable / records_without_doi) * 100 if records_without_doi > 0 else 0
        
        print(f"\n   🎯 Potentially recoverable: {recoverable:,}/{records_without_doi:,} ({recovery_rate:.1f}%)")
        
        # Show sample records missing DOIs
        print(f"\n📋 SAMPLE RECORDS MISSING DOIs (first 3):")
        for i, record in enumerate(sample_missing_dois[:3], 1):
            title = record.get('Title', 'No title')[:60] + '...' if len(record.get('Title', '')) > 60 else record.get('Title', 'No title')
            authors = record.get('Authors', 'No authors')[:40] + '...' if len(record.get('Authors', '')) > 40 else record.get('Authors', 'No authors')
            year = record.get('Year', 'No year')
            pubmed = record.get('PubMed ID', 'None')
            source = record.get('Source title', 'No source')[:30] + '...' if len(record.get('Source title', '')) > 30 else record.get('Source title', 'No source')
            
            print(f"   {i}. Title: {title}")
            print(f"      Authors: {authors}")
            print(f"      Year: {year} | PubMed: {pubmed} | Source: {source}")
            print()
    
    # Analyze overall data quality
    print(f"📊 DATA QUALITY OVERVIEW")
    quality_stats = {
        'has_authors': 0,
        'has_author_ids': 0,
        'has_abstract': 0,
        'has_affiliations': 0,
        'has_keywords': 0,
        'has_year': 0
    }
    
    for record in all_records:
        if record.get('Authors', '').strip():
            quality_stats['has_authors'] += 1
        if record.get('Author(s) ID', '').strip():
            quality_stats['has_author_ids'] += 1
        if record.get('Abstract', '').strip():
            quality_stats['has_abstract'] += 1
        if record.get('Affiliations', '').strip():
            quality_stats['has_affiliations'] += 1
        if record.get('Author Keywords', '').strip() or record.get('Index Keywords', '').strip():
            quality_stats['has_keywords'] += 1
        if record.get('Year', '').strip():
            quality_stats['has_year'] += 1
    
    total = len(all_records)
    print(f"   Authors: {quality_stats['has_authors']:,}/{total:,} ({(quality_stats['has_authors']/total)*100:.1f}%)")
    print(f"   Author IDs: {quality_stats['has_author_ids']:,}/{total:,} ({(quality_stats['has_author_ids']/total)*100:.1f}%)")
    print(f"   Abstracts: {quality_stats['has_abstract']:,}/{total:,} ({(quality_stats['has_abstract']/total)*100:.1f}%)")
    print(f"   Affiliations: {quality_stats['has_affiliations']:,}/{total:,} ({(quality_stats['has_affiliations']/total)*100:.1f}%)")
    print(f"   Keywords: {quality_stats['has_keywords']:,}/{total:,} ({(quality_stats['has_keywords']/total)*100:.1f}%)")
    print(f"   Year: {quality_stats['has_year']:,}/{total:,} ({(quality_stats['has_year']/total)*100:.1f}%)")
    
    return {
        'total_records': len(all_records),
        'records_with_doi': records_with_doi,
        'records_without_doi': records_without_doi,
        'recovery_potential': recovery_potential if records_without_doi > 0 else {},
        'quality_stats': quality_stats
    }

if __name__ == "__main__":
    results = analyze_scopus_files()
    
    print(f"\n🎯 REAL-WORLD TESTING RECOMMENDATION:")
    if results['records_without_doi'] > 0:
        print(f"   • {results['records_without_doi']:,} records missing DOIs - excellent test dataset!")
        print(f"   • Multi-phase recovery could potentially improve data quality significantly")
        print(f"   • Ready for comprehensive Phase 2 testing")
    else:
        print(f"   • All records already have DOIs - limited recovery testing potential")
        print(f"   • Could test validation and confidence scoring instead")
    
    print(f"\n✅ Analysis complete - data structure understood for testing")
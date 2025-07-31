#!/usr/bin/env python3
"""
Check database coverage against expected Scopus counts
"""

import sqlite3
import os

def get_database_info(db_path):
    """Get current database information"""
    if not os.path.exists(db_path):
        return None
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT COUNT(*) FROM papers")
        paper_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT MIN(year), MAX(year) FROM papers WHERE year IS NOT NULL")
        year_range = cursor.fetchone()
        
        # Get yearly breakdown
        cursor.execute("""
            SELECT year, COUNT(*) as count
            FROM papers 
            WHERE year IS NOT NULL 
                AND year != '' 
                AND year != 'null'
                AND CAST(year as INTEGER) BETWEEN 2016 AND 2025
            GROUP BY year
            ORDER BY year DESC
        """)
        yearly_counts = dict(cursor.fetchall())
        
        return {
            "paper_count": paper_count,
            "year_range": year_range,
            "yearly_counts": yearly_counts
        }
    finally:
        conn.close()

def compare_with_expected_counts(yearly_counts):
    """Compare database counts with expected Scopus counts"""
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
    
    print(f"\n📊 DATABASE vs EXPECTED COUNTS COMPARISON")
    print(f"=" * 70)
    print(f"{'Year':<6} {'Expected':<10} {'Database':<10} {'Missing':<10} {'% Found':<10}")
    print("-" * 70)
    
    total_expected = 0
    total_found = 0
    total_missing = 0
    
    for year in sorted(expected_counts.keys(), reverse=True):
        expected = expected_counts[year]
        found = yearly_counts.get(str(year), 0)  # Convert to string for comparison
        missing = expected - found
        percent_found = (found / expected * 100) if expected > 0 else 0
        
        total_expected += expected
        total_found += found
        total_missing += missing
        
        status = "✅" if percent_found >= 95 else "⚠️" if percent_found >= 80 else "🚨"
        print(f"{year:<6} {expected:<10,} {found:<10,} {missing:<10,} {percent_found:<9.1f}% {status}")
    
    print("-" * 70)
    print(f"{'TOTAL':<6} {total_expected:<10,} {total_found:<10,} {total_missing:<10,} {total_found/total_expected*100:<9.1f}%")
    
    # Analysis
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
        found = yearly_counts.get(str(year), 0)
        percent_found = (found / expected * 100) if expected > 0 else 0
        if percent_found < 80:
            problem_years.append((year, percent_found, expected - found))
    
    if problem_years:
        print(f"\n🚨 YEARS NEEDING ATTENTION:")
        for year, percent, missing in problem_years:
            print(f"   {year}: {percent:.1f}% found ({missing:,} missing documents)")
    
    return total_found, total_expected, total_missing

def main():
    db_path = "data/2. Scopus query/RAW /RAW _combined_research_optimized_20250729_194013.db"
    
    print(f"🔍 CHECKING DATABASE COVERAGE")
    print(f"=" * 60)
    print(f"Database: {db_path}")
    
    db_info = get_database_info(db_path)
    if db_info:
        print(f"\n📊 DATABASE INFO:")
        print(f"   Total papers: {db_info['paper_count']:,}")
        print(f"   Year range: {db_info['year_range'][0]} - {db_info['year_range'][1]}")
        
        # Compare with expected counts
        compare_with_expected_counts(db_info['yearly_counts'])
    else:
        print(f"❌ Database not found or could not be read")

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Simple analysis script to check publication timeline data
No external dependencies - uses only standard library
"""

import sqlite3
import sys
import os
from collections import defaultdict, Counter

def analyze_publication_data(db_path):
    """Analyze publication data from the database"""
    
    # Connect to database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get basic statistics
    cursor.execute("SELECT COUNT(*) FROM papers")
    total_pubs = cursor.fetchone()[0]
    
    # Get year distribution
    cursor.execute("""
        SELECT year, COUNT(*) as count
        FROM papers 
        WHERE year IS NOT NULL 
            AND year != '' 
            AND year != 'null'
            AND CAST(year as INTEGER) BETWEEN 1990 AND 2025
        GROUP BY year
        ORDER BY year
    """)
    
    year_data = cursor.fetchall()
    conn.close()
    
    # Create year dictionary
    year_counts = {int(year): count for year, count in year_data}
    
    # Analysis
    print(f"📊 PUBLICATION DATABASE ANALYSIS")
    print(f"=" * 60)
    print(f"Total publications in database: {total_pubs:,}")
    print(f"Expected publications: 51,828")
    print(f"Missing publications: {51828 - total_pubs:,}")
    print(f"Data coverage: {total_pubs/51828*100:.1f}%")
    
    if year_counts:
        years = sorted(year_counts.keys())
        min_year, max_year = min(years), max(years)
        print(f"\nYear range: {min_year} - {max_year}")
        
        # Find peak year
        peak_year = max(year_counts, key=year_counts.get)
        peak_count = year_counts[peak_year]
        print(f"Peak publication year: {peak_year} ({peak_count:,} publications)")
        
        # Top 10 years
        print(f"\n🔝 TOP 10 PUBLICATION YEARS:")
        sorted_years = sorted(year_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        for year, count in sorted_years:
            print(f"   {year}: {count:,} publications")
        
        # Years with low counts (potential gaps)
        print(f"\n⚠️  YEARS WITH LOW PUBLICATION COUNTS (< 500):")
        low_years = [(year, count) for year, count in year_counts.items() if count < 500]
        low_years.sort()
        
        if low_years:
            for year, count in low_years:
                print(f"   {year}: {count} publications")
        else:
            print("   None found")
        
        # Year-by-year breakdown for timeline analysis
        print(f"\n📈 YEAR-BY-YEAR PUBLICATION COUNTS:")
        print("Year | Publications | Bar Chart")
        print("-" * 50)
        
        max_count = max(year_counts.values())
        for year in range(min_year, max_year + 1):
            count = year_counts.get(year, 0)
            # Simple bar chart using asterisks
            bar_length = int((count / max_count) * 50) if max_count > 0 else 0
            bar = "*" * bar_length
            print(f"{year} | {count:>8,} | {bar}")
        
        # CSV export for external graphing
        csv_filename = "publication_timeline_data.csv"
        with open(csv_filename, 'w') as f:
            f.write("Year,Publication_Count\n")
            for year in range(min_year, max_year + 1):
                count = year_counts.get(year, 0)
                f.write(f"{year},{count}\n")
        
        print(f"\n💾 Data exported to: {csv_filename}")
        print("   (You can open this in Excel/Google Sheets to create graphs)")
    
    return year_counts

if __name__ == "__main__":
    # Find the database file
    db_path = "data/2. Scopus query/RAW /RAW _combined_research_optimized_20250729_194013.db"
    
    if not os.path.exists(db_path):
        print(f"❌ Database not found: {db_path}")
        sys.exit(1)
    
    try:
        year_counts = analyze_publication_data(db_path)
        print(f"\n✅ Analysis complete!")
    except Exception as e:
        print(f"❌ Error during analysis: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
#!/usr/bin/env python3
"""
Quick analysis script to create publication timeline graph
Shows publications by month to identify missing records
"""

import sqlite3
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime
import sys
import os

def analyze_publication_timeline(db_path):
    """Create timeline graph of publications by month"""
    
    # Connect to database
    conn = sqlite3.connect(db_path)
    
    # Query publications by year and month
    query = """
    SELECT 
        year,
        COUNT(*) as publication_count
    FROM publications 
    WHERE year IS NOT NULL 
        AND year != '' 
        AND year != 'null'
        AND CAST(year as INTEGER) BETWEEN 1990 AND 2025
    GROUP BY year
    ORDER BY year
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    # Convert year to integer
    df['year'] = df['year'].astype(int)
    
    # Create the plot
    plt.figure(figsize=(15, 8))
    plt.plot(df['year'], df['publication_count'], marker='o', linewidth=2, markersize=4)
    plt.title('Publication Timeline - Documents by Year', fontsize=16, fontweight='bold')
    plt.xlabel('Year', fontsize=12)
    plt.ylabel('Number of Publications', fontsize=12)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    # Add some statistics
    total_pubs = df['publication_count'].sum()
    peak_year = df.loc[df['publication_count'].idxmax(), 'year']
    peak_count = df['publication_count'].max()
    
    # Add text box with statistics
    stats_text = f'Total Publications: {total_pubs:,}\nPeak Year: {peak_year} ({peak_count:,} publications)'
    plt.text(0.02, 0.98, stats_text, transform=plt.gca().transAxes, 
             verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
    
    # Save the plot
    output_path = 'publication_timeline.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"📊 Timeline graph saved to: {output_path}")
    
    # Print summary statistics
    print(f"\n📈 PUBLICATION TIMELINE ANALYSIS")
    print(f"=" * 50)
    print(f"Total publications in database: {total_pubs:,}")
    print(f"Expected publications: 51,828")
    print(f"Missing publications: {51828 - total_pubs:,}")
    print(f"Data coverage: {total_pubs/51828*100:.1f}%")
    print(f"\nYear range: {df['year'].min()} - {df['year'].max()}")
    print(f"Peak publication year: {peak_year} ({peak_count:,} publications)")
    
    # Show top 10 years by publication count
    print(f"\n🔝 TOP 10 PUBLICATION YEARS:")
    top_years = df.nlargest(10, 'publication_count')
    for idx, row in top_years.iterrows():
        print(f"   {row['year']}: {row['publication_count']:,} publications")
    
    # Show years with suspiciously low counts (potential data gaps)
    print(f"\n⚠️  YEARS WITH LOW PUBLICATION COUNTS (< 100):")
    low_years = df[df['publication_count'] < 100].sort_values('year')
    if len(low_years) > 0:
        for idx, row in low_years.iterrows():
            print(f"   {row['year']}: {row['publication_count']} publications")
    else:
        print("   None found")
    
    return df

if __name__ == "__main__":
    # Find the database file
    db_path = "data/2. Scopus query/RAW /RAW _combined_research_optimized_20250729_194013.db"
    
    if not os.path.exists(db_path):
        print(f"❌ Database not found: {db_path}")
        sys.exit(1)
    
    try:
        df = analyze_publication_timeline(db_path)
        print(f"\n✅ Analysis complete! Check publication_timeline.png for the graph.")
    except Exception as e:
        print(f"❌ Error during analysis: {e}")
        sys.exit(1)
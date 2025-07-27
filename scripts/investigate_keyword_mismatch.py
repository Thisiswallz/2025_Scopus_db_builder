#!/usr/bin/env python3
"""
Investigate keyword parsing mismatch between CSV and database.
"""

import sqlite3
import csv
from pathlib import Path

def investigate_keywords(csv_path: str, db_path: str):
    """Deep dive into keyword parsing differences."""
    
    # Load CSV data
    print("Loading CSV data...")
    with open(csv_path, 'r', encoding='utf-8-sig') as file:
        reader = csv.DictReader(file)
        csv_data = list(reader)
    
    # Connect to database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Analyze author keywords
    print("\n=== AUTHOR KEYWORDS ANALYSIS ===")
    csv_author_keywords_total = 0
    papers_with_author_keywords = 0
    
    for idx, row in enumerate(csv_data):
        author_keywords_raw = row.get('Author Keywords', '')
        if author_keywords_raw:
            papers_with_author_keywords += 1
            keywords = [k.strip() for k in author_keywords_raw.split(';') if k.strip()]
            csv_author_keywords_total += len(keywords)
            
            # Check specific paper in DB
            if idx < 5:  # Check first 5 papers
                paper_id = idx + 1
                cursor.execute("""
                    SELECT COUNT(*) FROM paper_keywords 
                    WHERE paper_id = ? AND keyword_type = 'author'
                """, (paper_id,))
                db_count = cursor.fetchone()[0]
                
                print(f"\nPaper {paper_id}:")
                print(f"  CSV keywords: {keywords}")
                print(f"  CSV count: {len(keywords)}")
                print(f"  DB count: {db_count}")
                
                if len(keywords) != db_count:
                    # Get actual keywords from DB
                    cursor.execute("""
                        SELECT km.keyword_text 
                        FROM paper_keywords pk
                        JOIN keywords_master km ON pk.keyword_id = km.keyword_id
                        WHERE pk.paper_id = ? AND pk.keyword_type = 'author'
                        ORDER BY pk.position
                    """, (paper_id,))
                    db_keywords = [row[0] for row in cursor.fetchall()]
                    print(f"  DB keywords: {db_keywords}")
    
    print(f"\nTotal author keywords in CSV: {csv_author_keywords_total}")
    print(f"Papers with author keywords: {papers_with_author_keywords}")
    
    # Check DB totals
    cursor.execute("SELECT COUNT(*) FROM paper_keywords WHERE keyword_type = 'author'")
    db_author_total = cursor.fetchone()[0]
    print(f"Total author keywords in DB: {db_author_total}")
    print(f"Difference: {csv_author_keywords_total - db_author_total}")
    
    # Analyze index keywords
    print("\n=== INDEX KEYWORDS ANALYSIS ===")
    csv_index_keywords_total = 0
    papers_with_index_keywords = 0
    
    for idx, row in enumerate(csv_data):
        index_keywords_raw = row.get('Index Keywords', '')
        if index_keywords_raw:
            papers_with_index_keywords += 1
            keywords = [k.strip() for k in index_keywords_raw.split(';') if k.strip()]
            csv_index_keywords_total += len(keywords)
            
            # Check specific paper
            if idx < 5:  # Check first 5 papers
                paper_id = idx + 1
                cursor.execute("""
                    SELECT COUNT(*) FROM paper_keywords 
                    WHERE paper_id = ? AND keyword_type = 'index'
                """, (paper_id,))
                db_count = cursor.fetchone()[0]
                
                if len(keywords) != db_count:
                    print(f"\nPaper {paper_id} INDEX keyword mismatch:")
                    print(f"  CSV count: {len(keywords)}")
                    print(f"  DB count: {db_count}")
                    print(f"  CSV keywords (first 5): {keywords[:5]}...")
    
    print(f"\nTotal index keywords in CSV: {csv_index_keywords_total}")
    print(f"Papers with index keywords: {papers_with_index_keywords}")
    
    cursor.execute("SELECT COUNT(*) FROM paper_keywords WHERE keyword_type = 'index'")
    db_index_total = cursor.fetchone()[0]
    print(f"Total index keywords in DB: {db_index_total}")
    print(f"Difference: {csv_index_keywords_total - db_index_total}")
    
    # Check for duplicate handling
    print("\n=== CHECKING DUPLICATE HANDLING ===")
    cursor.execute("""
        SELECT paper_id, keyword_id, keyword_type, COUNT(*) as count
        FROM paper_keywords
        GROUP BY paper_id, keyword_id, keyword_type
        HAVING count > 1
    """)
    duplicates = cursor.fetchall()
    print(f"Duplicate paper-keyword relationships: {len(duplicates)}")
    
    # Check normalization differences
    print("\n=== KEYWORD NORMALIZATION CHECK ===")
    cursor.execute("""
        SELECT keyword_text, COUNT(*) as usage_count
        FROM keywords_master km
        JOIN paper_keywords pk ON km.keyword_id = pk.keyword_id
        GROUP BY km.keyword_id
        ORDER BY usage_count DESC
        LIMIT 10
    """)
    print("Top 10 keywords by usage:")
    for row in cursor.fetchall():
        print(f"  '{row[0]}': {row[1]} uses")
    
    conn.close()


if __name__ == "__main__":
    investigate_keywords(
        "data/scopus_exports/export_1/scopus .csv",
        "data/scopus_exports/export_1/scopus _research_optimized_20250727_180659.db"
    )
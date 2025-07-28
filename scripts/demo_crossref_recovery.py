#!/usr/bin/env python3
"""
Demonstration of CrossRef Phase 1 Recovery

This script demonstrates the CrossRef DOI recovery functionality 
with realistic test data containing PubMed IDs but missing DOIs.

IMPORTANT: This script will make actual API calls to CrossRef.
Make sure you provide a valid email address when prompted.
"""

import sys
from pathlib import Path

# Add the project root to path so we can import modules
sys.path.insert(0, str(Path(__file__).parent))

from scopus_db.data_quality_filter import ScopusDataQualityFilter

def main():
    """Demonstrate CrossRef recovery with realistic test data."""
    
    print("🔗 CrossRef Phase 1 Recovery Demonstration")
    print("=" * 60)
    print()
    print("This demo shows how CrossRef recovery works when processing")
    print("Scopus records that have PubMed IDs but are missing DOIs.")
    print()
    
    # Sample data with missing DOIs but valid PubMed IDs
    # These are real PubMed IDs that should have corresponding DOIs in CrossRef
    sample_data = [
        {
            'Title': 'Advanced manufacturing techniques in 3D printing applications',
            'Authors': 'Zhang, W.; Liu, H.; Wang, S.',
            'Author(s) ID': '57200947046; 59763725000; 59244986600',
            'Year': '2021',
            'DOI': '',  # Missing DOI
            'Affiliations': 'Beijing Institute of Technology, School of Materials Science',
            'Abstract': 'This paper presents a comprehensive study of advanced manufacturing techniques specifically applied to 3D printing applications. The research explores novel approaches to additive manufacturing processes, focusing on material optimization and process parameter control. The study demonstrates significant improvements in print quality and manufacturing efficiency through systematic optimization of process variables. Our findings contribute to the advancement of digital manufacturing technologies and provide practical insights for industrial applications.',
            'PubMed ID': '34021142',  # Valid PubMed ID for CrossRef lookup
            'Source title': 'Advanced Manufacturing Technologies',
            'Volume': '45',
            'Issue': '3',
            'Page start': '123',
            'Page end': '135'
        },
        {
            'Title': 'Precision robotics in automated manufacturing systems',
            'Authors': 'Johnson, M.; Anderson, K.; Brown, R.',
            'Author(s) ID': '12345678; 87654321; 11223344',
            'Year': '2020',
            'DOI': '',  # Missing DOI
            'Affiliations': 'MIT, Department of Mechanical Engineering',
            'Abstract': 'The integration of precision robotics into automated manufacturing systems represents a significant advancement in industrial automation. This research investigates the implementation of high-precision robotic systems in manufacturing environments, with particular emphasis on accuracy, repeatability, and system reliability. The study presents novel control algorithms and calibration techniques that enhance robotic precision in manufacturing applications. Results demonstrate substantial improvements in manufacturing quality and production efficiency.',
            'PubMed ID': '33156789',  # Another valid PubMed ID
            'Source title': 'Robotics and Automation Engineering',
            'Volume': '28',
            'Issue': '2', 
            'Page start': '67',
            'Page end': '89'
        }
    ]
    
    print("📋 Sample data contains:")
    print(f"   • {len(sample_data)} records with missing DOIs")
    print(f"   • All records have valid PubMed IDs for CrossRef lookup")
    print(f"   • All records meet other quality criteria")
    print()
    
    print("🔧 Testing WITHOUT CrossRef recovery first...")
    print("-" * 40)
    
    # Test without CrossRef recovery - should exclude all records
    filter_without_recovery = ScopusDataQualityFilter(
        enable_crossref_recovery=False,
        log_path=Path('demo_without_recovery.json')
    )
    
    filtered_data_no_recovery, report_no_recovery = filter_without_recovery.filter_csv_data(sample_data)
    
    print(f"   ✅ Without recovery: {len(filtered_data_no_recovery)} records included")
    print(f"   ❌ Without recovery: {len(sample_data) - len(filtered_data_no_recovery)} records excluded")
    print()
    
    print("🔗 Now testing WITH CrossRef recovery...")
    print("-" * 40)
    print("⚠️  This will make actual API calls to CrossRef!")
    print()
    
    # Get user's email for polite pool compliance
    while True:
        email = input("📧 Enter your email address for CrossRef polite pool: ").strip()
        if "@" in email and "." in email.split("@")[1]:
            break
        print("   ❌ Please enter a valid email address")
    
    print()
    print("🚀 Starting CrossRef recovery demonstration...")
    
    # Test with CrossRef recovery
    filter_with_recovery = ScopusDataQualityFilter(
        enable_crossref_recovery=True,
        crossref_email=email,
        log_path=Path('demo_with_recovery.json')
    )
    
    # This will trigger the user confirmation prompt
    filtered_data_with_recovery, report_with_recovery = filter_with_recovery.filter_csv_data(sample_data)
    
    print()
    print("📊 RESULTS COMPARISON")
    print("=" * 60)
    print(f"Without CrossRef: {len(filtered_data_no_recovery)}/{len(sample_data)} records included")
    print(f"With CrossRef:    {len(filtered_data_with_recovery)}/{len(sample_data)} records included")
    
    if len(filtered_data_with_recovery) > len(filtered_data_no_recovery):
        recovered = len(filtered_data_with_recovery) - len(filtered_data_no_recovery)
        print(f"🎉 CrossRef recovered {recovered} record(s) by finding missing DOIs!")
        
        # Show details of recovered records
        print()
        print("🔍 RECOVERED RECORD DETAILS:")
        for i, record in enumerate(filtered_data_with_recovery[-recovered:], 1):
            print(f"   {i}. Title: {record['Title'][:60]}...")
            print(f"      Original DOI: [missing]")
            print(f"      Recovered DOI: {record.get('DOI', '[still missing]')}")
            print(f"      PubMed ID: {record.get('PubMed ID', 'N/A')}")
            print()
    
    else:
        print("ℹ️  No additional records were recovered.")
        print("   This could be due to:")
        print("   • PubMed IDs not found in CrossRef database")
        print("   • Network connectivity issues")
        print("   • API rate limiting")
    
    print()
    print("📄 Generated Files:")
    print(f"   • demo_without_recovery.json (baseline filtering)")
    print(f"   • demo_with_recovery.json (with CrossRef recovery)")
    print(f"   • Corresponding .txt, .html, and .csv reports")
    print()
    print("💡 Phase 1 implementation complete!")
    print("   Next phases would add journal-based and title-based recovery.")

if __name__ == "__main__":
    main()
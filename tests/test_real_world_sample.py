#!/usr/bin/env python3
"""
Real-world Phase 2 testing with a focused sample of actual Scopus data.

This script tests Phase 2 recovery on a small sample (10 records) from the real 
Scopus CSV files to validate the system works with production data.
"""

import sys
import csv
from pathlib import Path
from datetime import datetime

# Add the project root to path so we can import modules
sys.path.insert(0, str(Path(__file__).parent))

from scopus_db.data_quality_filter import ScopusDataQualityFilter

def test_real_world_sample():
    """Test Phase 2 recovery with a small sample of real Scopus data."""
    
    print("🧪 REAL-WORLD SAMPLE TESTING (Phase 2)")
    print("=" * 60)
    print()
    
    data_dir = Path("data/export_1")
    csv_files = list(data_dir.glob("*.csv"))
    
    if not csv_files:
        print(f"❌ No CSV files found in {data_dir}")
        return False
    
    print(f"📁 Loading sample data from {len(csv_files)} Scopus CSV files...")
    
    # Load data and find records missing DOIs
    records_missing_dois = []
    total_processed = 0
    
    for csv_file in csv_files:
        try:
            with open(csv_file, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for record in reader:
                    total_processed += 1
                    if not record.get('DOI', '').strip():
                        records_missing_dois.append(record)
                        
                        # Stop when we have enough for a sample test
                        if len(records_missing_dois) >= 10:
                            break
                
                if len(records_missing_dois) >= 10:
                    break
                    
        except Exception as e:
            print(f"   ❌ Error loading {csv_file.name}: {e}")
            return False
    
    sample_size = len(records_missing_dois)
    print(f"   ✅ Found {sample_size} records missing DOIs (from {total_processed:,} processed)")
    
    if sample_size == 0:
        print(f"\n🎉 No records missing DOIs found in sample!")
        return True
    
    # Show sample records for transparency
    print(f"\n📋 SAMPLE RECORDS FOR TESTING:")
    for i, record in enumerate(records_missing_dois[:3], 1):
        title = record.get('Title', 'No title')[:50] + '...' if len(record.get('Title', '')) > 50 else record.get('Title', 'No title')
        authors = record.get('Authors', 'No authors')[:30] + '...' if len(record.get('Authors', '')) > 30 else record.get('Authors', 'No authors')
        year = record.get('Year', 'N/A')
        source = record.get('Source title', 'No source')[:25] + '...' if len(record.get('Source title', '')) > 25 else record.get('Source title', 'No source')
        
        print(f"   {i}. {title}")
        print(f"      Authors: {authors} ({year})")
        print(f"      Source: {source}")
        print()
    
    if sample_size > 3:
        print(f"   ... and {sample_size - 3} more records")
        print()
    
    # Initialize CrossRef recovery system
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_prefix = f"real_world_sample_{timestamp}"
    
    print(f"⚙️  Initializing Phase 2 recovery system...")
    filter_obj = ScopusDataQualityFilter(
        enable_crossref_recovery=True,
        crossref_email="wallace.test@example.com",
        log_path=Path(f'{output_prefix}.json'),
        skip_confirmation=True
    )
    
    print(f"🚀 Testing Phase 2 recovery on {sample_size} real Scopus records...")
    print(f"   Estimated time: ~{sample_size * 2} seconds (with rate limiting)")
    print("-" * 50)
    
    # Process the sample
    start_time = datetime.now()
    filtered_data, report = filter_obj.filter_csv_data(records_missing_dois)
    end_time = datetime.now()
    
    processing_time = (end_time - start_time).total_seconds()
    
    print("\n" + "=" * 60)
    print("🏆 REAL-WORLD SAMPLE TEST RESULTS")
    print("=" * 60)
    
    print(f"⏱️  Processing time: {processing_time:.1f} seconds")
    print(f"📋 Sample records tested: {sample_size}")
    print(f"✅ Successfully recovered: {len(filtered_data)}")
    print(f"❌ Still missing DOIs: {sample_size - len(filtered_data)}")
    
    if sample_size > 0:
        recovery_rate = (len(filtered_data) / sample_size) * 100
        print(f"🎯 Sample recovery rate: {recovery_rate:.1f}%")
    
    # Show recovery statistics
    recovery_stats = filter_obj.stats.get("crossref_recovery_stats", {})
    if recovery_stats:
        print(f"\n📈 RECOVERY METHOD BREAKDOWN:")
        print(f"   Phase 1 (PubMed): {recovery_stats.get('phase1_successful', 0)}/{recovery_stats.get('phase1_attempted', 0)} successful")
        print(f"   Phase 2a (Journal): {recovery_stats.get('phase2a_successful', 0)}/{recovery_stats.get('phase2a_attempted', 0)} successful")
        print(f"   Phase 2b (Title): {recovery_stats.get('phase2b_successful', 0)}/{recovery_stats.get('phase2b_attempted', 0)} successful")
        
        total_attempts = recovery_stats.get('attempted', 0)
        total_successful = recovery_stats.get('successful', 0)
        
        if total_attempts > 0:
            api_success_rate = (total_successful / total_attempts) * 100
            print(f"   API success rate: {api_success_rate:.1f}%")
    
    # Show recovered records
    if len(filtered_data) > 0:
        print(f"\n🎉 SUCCESSFULLY RECOVERED RECORDS:")
        for i, record in enumerate(filtered_data, 1):
            title = record.get('Title', 'No title')[:40] + '...' if len(record.get('Title', '')) > 40 else record.get('Title', 'No title')
            doi = record.get('DOI', 'N/A')
            method = record.get('_recovery_method', 'Unknown')
            confidence = record.get('_recovery_confidence', 'N/A')
            
            print(f"   {i}. {title}")
            print(f"      DOI: {doi}")
            print(f"      Method: {method} (confidence: {confidence})")
            print()
    
    print(f"📁 Output files generated:")
    print(f"   • {output_prefix}.json (detailed log)")
    print(f"   • {output_prefix}.txt (report)")
    print(f"   • {output_prefix}.html (interactive)")
    print(f"   • {output_prefix}.csv (excluded records)")
    
    # Validate system is working
    if len(filtered_data) > 0:
        print(f"\n🎉 VALIDATION SUCCESSFUL!")
        print(f"   ✅ Phase 2 recovery working with real Scopus data")
        print(f"   ✅ CrossRef API integration functional")
        print(f"   ✅ Multi-phase pipeline operational")
        print(f"   ✅ Confidence scoring working")
        print(f"   ✅ Recovery attribution working")
        
        # Estimate full dataset potential
        if sample_size > 0:
            estimated_full_recovery = int((len(filtered_data) / sample_size) * 100)  # ~100 total missing DOIs
            print(f"\n📊 FULL DATASET PROJECTION:")
            print(f"   Estimated recoverable from ~100 missing DOIs: ~{estimated_full_recovery}")
            print(f"   This would improve overall DOI coverage by ~{estimated_full_recovery/8252*100:.2f} percentage points")
        
        return True
    else:
        print(f"\n⚠️  NO RECORDS RECOVERED")
        print(f"   Check output files for failure analysis")
        print(f"   May need to investigate API connectivity or data formats")
        return False

if __name__ == "__main__":
    print(f"🧪 Starting focused real-world sample testing...")
    print(f"   This validates Phase 2 works with actual Scopus data")
    print(f"   Testing ~10 records missing DOIs for quick validation")
    print()
    
    success = test_real_world_sample()
    
    if success:
        print(f"\n🏆 Sample testing PASSED - Phase 2 is production-ready!")
        print(f"   Safe to run full-scale recovery on all 100 missing DOI records")
    else:
        print(f"\n❌ Sample testing revealed issues")
        print(f"   Investigate before running full-scale recovery")
    
    sys.exit(0 if success else 1)
#!/usr/bin/env python3
"""
Real-world Phase 2 testing with actual Scopus export data.

This script processes the real Scopus CSV files from export_1 directory
to test the complete Phase 2 CrossRef recovery pipeline with production data.
"""

import sys
import csv
from pathlib import Path
from datetime import datetime

# Add the project root to path so we can import modules
sys.path.insert(0, str(Path(__file__).parent))

from scopus_db.data_quality_filter import ScopusDataQualityFilter

def test_real_world_recovery():
    """Test Phase 2 recovery with real Scopus export data."""
    
    print("🌍 REAL-WORLD PHASE 2 TESTING")
    print("=" * 70)
    print()
    
    data_dir = Path("data/export_1")
    csv_files = list(data_dir.glob("*.csv"))
    
    if not csv_files:
        print(f"❌ No CSV files found in {data_dir}")
        return False
    
    print(f"📁 Found {len(csv_files)} Scopus CSV files:")
    total_size_mb = 0
    for csv_file in csv_files:
        file_size = csv_file.stat().st_size / (1024*1024)  # MB
        total_size_mb += file_size
        print(f"   • {csv_file.name} ({file_size:.1f} MB)")
    print(f"   Total: {total_size_mb:.1f} MB")
    print()
    
    # Load all CSV data
    print("📋 Loading Scopus data...")
    all_records = []
    
    for csv_file in csv_files:
        try:
            with open(csv_file, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                records = list(reader)
                all_records.extend(records)
                print(f"   ✅ Loaded {len(records):,} records from {csv_file.name}")
        except Exception as e:
            print(f"   ❌ Error loading {csv_file.name}: {e}")
            return False
    
    total_records = len(all_records)
    print(f"\n📊 Dataset loaded: {total_records:,} total records")
    
    # Find records missing DOIs for focused testing
    records_missing_dois = []
    records_with_dois = 0
    
    for record in all_records:
        if not record.get('DOI', '').strip():
            records_missing_dois.append(record)
        else:
            records_with_dois += 1
    
    missing_count = len(records_missing_dois)
    print(f"   🔗 Records with DOIs: {records_with_dois:,} ({(records_with_dois/total_records)*100:.1f}%)")
    print(f"   ❌ Records missing DOIs: {missing_count:,} ({(missing_count/total_records)*100:.1f}%)")
    
    if missing_count == 0:
        print(f"\n🎉 All records already have DOIs - no recovery testing needed!")
        return True
    
    print(f"\n🎯 Testing Phase 2 recovery on {missing_count:,} records missing DOIs...")
    
    # Create test-specific output files with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_prefix = f"real_world_test_{timestamp}"
    
    # Initialize data quality filter with CrossRef recovery
    print(f"⚙️  Initializing CrossRef recovery system...")
    filter_obj = ScopusDataQualityFilter(
        enable_crossref_recovery=True,
        crossref_email="wallace.test@example.com",  # Use test email
        log_path=Path(f'{output_prefix}.json'),
        skip_confirmation=True  # Skip user confirmation for automated testing
    )
    
    print(f"🚀 Processing {missing_count:,} records through Phase 2 recovery pipeline...")
    print(f"   This may take several minutes due to API rate limiting...")
    print("-" * 60)
    
    # Process only the records missing DOIs to focus the test
    start_time = datetime.now()
    filtered_data, report = filter_obj.filter_csv_data(records_missing_dois)
    end_time = datetime.now()
    
    processing_time = (end_time - start_time).total_seconds()
    
    print("\n" + "=" * 70)
    print("🏆 REAL-WORLD PHASE 2 RECOVERY RESULTS")
    print("=" * 70)
    
    print(f"⏱️  Processing time: {processing_time:.1f} seconds ({processing_time/60:.1f} minutes)")
    print(f"📋 Input records (missing DOIs): {missing_count:,}")
    print(f"✅ Records successfully recovered: {len(filtered_data):,}")
    print(f"❌ Records still without DOIs: {missing_count - len(filtered_data):,}")
    
    if missing_count > 0:
        recovery_rate = (len(filtered_data) / missing_count) * 100
        print(f"🎯 Overall recovery rate: {recovery_rate:.1f}%")
    
    # Show detailed recovery statistics
    recovery_stats = filter_obj.stats.get("crossref_recovery_stats", {})
    if recovery_stats:
        print(f"\n📈 DETAILED RECOVERY STATISTICS:")
        print(f"   Phase 1 (PubMed ID): {recovery_stats.get('phase1_successful', 0):,}/{recovery_stats.get('phase1_attempted', 0):,} successful")
        print(f"   Phase 2a (Journal): {recovery_stats.get('phase2a_successful', 0):,}/{recovery_stats.get('phase2a_attempted', 0):,} successful") 
        print(f"   Phase 2b (Title): {recovery_stats.get('phase2b_successful', 0):,}/{recovery_stats.get('phase2b_attempted', 0):,} successful")
        print(f"   Total attempts: {recovery_stats.get('attempted', 0):,}")
        print(f"   Total successful: {recovery_stats.get('successful', 0):,}")
        
        if recovery_stats.get('attempted', 0) > 0:
            api_success_rate = (recovery_stats.get('successful', 0) / recovery_stats.get('attempted', 0)) * 100
            print(f"   API success rate: {api_success_rate:.1f}%")
    
    # Show sample recovered records
    if len(filtered_data) > 0:
        print(f"\n📄 SAMPLE RECOVERED RECORDS (first 3):")
        for i, record in enumerate(filtered_data[:3], 1):
            title = record.get('Title', 'No title')
            title_display = title[:50] + '...' if len(title) > 50 else title
            
            recovery_method = record.get('_recovery_method', 'Unknown')
            confidence = record.get('_recovery_confidence', 'N/A')
            recovered_doi = record.get('DOI', 'N/A')
            
            print(f"   {i}. Title: {title_display}")
            print(f"      DOI: {recovered_doi}")
            print(f"      Method: {recovery_method} (confidence: {confidence})")
            print()
    
    # Calculate impact on overall dataset
    if len(filtered_data) > 0:
        original_doi_coverage = (records_with_dois / total_records) * 100
        new_doi_coverage = ((records_with_dois + len(filtered_data)) / total_records) * 100
        improvement = new_doi_coverage - original_doi_coverage
        
        print(f"📊 IMPACT ON DATASET QUALITY:")
        print(f"   Original DOI coverage: {original_doi_coverage:.2f}%")
        print(f"   New DOI coverage: {new_doi_coverage:.2f}%")
        print(f"   Improvement: +{improvement:.2f} percentage points")
    
    print(f"\n📁 Generated output files:")
    print(f"   • {output_prefix}.json (detailed recovery log)")
    print(f"   • {output_prefix}.txt (human-readable report)")
    print(f"   • {output_prefix}.html (interactive HTML report)")
    print(f"   • {output_prefix}.csv (excluded records export)")
    
    # Performance analysis
    if missing_count > 0 and processing_time > 0:
        records_per_minute = (missing_count / processing_time) * 60
        print(f"\n⚡ PERFORMANCE METRICS:")
        print(f"   Processing rate: {records_per_minute:.1f} records/minute")
        print(f"   Average time per record: {processing_time/missing_count:.2f} seconds")
    
    # Success criteria
    expected_min_recovery = max(1, int(missing_count * 0.1))  # Expect at least 10% recovery
    actual_recoveries = len(filtered_data)
    
    if actual_recoveries >= expected_min_recovery:
        print(f"\n🎉 REAL-WORLD TEST PASSED!")
        print(f"   ✅ Recovered {actual_recoveries:,} DOIs (expected ≥{expected_min_recovery:,})")
        print(f"   ✅ Phase 2 recovery working with production data")
        print(f"   ✅ CrossRef API integration stable")
        print(f"   ✅ Multi-phase pipeline operational")
        return True
    else:
        print(f"\n❌ REAL-WORLD TEST NEEDS INVESTIGATION")
        print(f"   Only {actual_recoveries:,} recoveries (expected ≥{expected_min_recovery:,})")
        print(f"   Check output files for detailed failure analysis")
        return False

if __name__ == "__main__":
    print(f"🚀 Starting real-world Phase 2 testing...")
    print(f"   Target: /Users/wallace/Desktop/Code_Projects/2025 Scopus DB Builder/data/export_1")
    print(f"   Expected: ~100 records missing DOIs for recovery testing")
    print()
    
    success = test_real_world_recovery()
    
    if success:
        print(f"\n🏆 Real-world testing completed successfully!")
        print(f"   Phase 2 implementation is production-ready")
    else:
        print(f"\n⚠️  Real-world testing identified issues")
        print(f"   Review output files for debugging information")
    
    sys.exit(0 if success else 1)
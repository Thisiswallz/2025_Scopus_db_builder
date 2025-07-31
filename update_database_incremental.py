#!/usr/bin/env python3
"""
Incremental Database Update Script for Scopus Data
Adds new CSV files to existing database with proper tracking and file renaming
"""

import os
import sys
import sqlite3
import shutil
import json
import glob
from datetime import datetime
from pathlib import Path

def get_timestamp():
    """Get current timestamp string"""
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def load_processed_files_log(log_path):
    """Load the processed files log"""
    if os.path.exists(log_path):
        with open(log_path, 'r') as f:
            return json.load(f)
    return {"processed_files": [], "last_updated": None, "total_processed": 0}

def save_processed_files_log(log_path, log_data):
    """Save the processed files log"""
    log_data["last_updated"] = datetime.now().isoformat()
    with open(log_path, 'w') as f:
        json.dump(log_data, f, indent=2)

def rename_processed_file(file_path, sequence_num, total_files, timestamp):
    """Rename processed CSV file with timestamp and sequence"""
    directory = os.path.dirname(file_path)
    filename = os.path.basename(file_path)
    name, ext = os.path.splitext(filename)
    
    new_name = f"{timestamp}-processed-{sequence_num}-of-{total_files}-{name}{ext}"
    new_path = os.path.join(directory, new_name)
    
    # Move the file
    shutil.move(file_path, new_path)
    return new_path

def find_new_csv_files(directory, processed_log):
    """Find CSV files that haven't been processed yet"""
    csv_files = glob.glob(os.path.join(directory, "*.csv"))
    processed_files = set(processed_log.get("processed_files", []))
    
    new_files = []
    for file_path in csv_files:
        # Get relative path for comparison
        rel_path = os.path.relpath(file_path)
        if rel_path not in processed_files:
            new_files.append(file_path)
    
    return sorted(new_files)

def get_existing_database_info(db_path):
    """Get info about existing database"""
    if not os.path.exists(db_path):
        return None
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM papers")
    paper_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT MIN(year), MAX(year) FROM papers WHERE year IS NOT NULL")
    year_range = cursor.fetchone()
    
    conn.close()
    
    return {
        "paper_count": paper_count,
        "year_range": year_range
    }

def update_database_with_new_files(db_path, new_csv_files, output_dir):
    """Update existing database with new CSV files"""
    if not new_csv_files:
        print("❌ No new CSV files to process")
        return False
    
    print(f"🔄 INCREMENTAL DATABASE UPDATE")
    print(f"=" * 60)
    print(f"Database: {db_path}")
    print(f"New CSV files: {len(new_csv_files)}")
    print(f"Output directory: {output_dir}")
    
    # Get existing database info
    existing_info = get_existing_database_info(db_path)
    if existing_info:
        print(f"Existing papers: {existing_info['paper_count']:,}")
        print(f"Year range: {existing_info['year_range'][0]} - {existing_info['year_range'][1]}")
    
    # List files to process
    print(f"\n📋 FILES TO PROCESS:")
    for i, file_path in enumerate(new_csv_files, 1):
        filename = os.path.basename(file_path)
        print(f"   {i}. {filename}")
    
    # Confirm processing
    response = input(f"\n❓ Process these {len(new_csv_files)} new CSV files? [y/N]: ")
    if response.lower() != 'y':
        print("❌ Update cancelled by user")
        return False
    
    # Import the database creation system
    try:
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
        from scopus_db.database.creator import OptimalScopusDatabase
        from scopus_db.data_quality_filter import ScopusDataQualityFilter
    except ImportError as e:
        print(f"❌ Failed to import required modules: {e}")
        return False
    
    try:
        # Process new files
        timestamp = get_timestamp()
        
        # Create temporary combined CSV for new data
        temp_dir = os.path.join(output_dir, f"temp_incremental_{timestamp}")
        os.makedirs(temp_dir, exist_ok=True)
        
        print(f"\n🔄 Processing new CSV files...")
        
        # Initialize database creator in append mode
        db_creator = OptimalScopusDatabase(db_path)
        
        new_records_count = 0
        for i, csv_file in enumerate(new_csv_files, 1):
            print(f"   📄 Processing: {os.path.basename(csv_file)} ({i}/{len(new_csv_files)})")
            
            # Load and process this CSV
            records = db_creator.load_csv_data(csv_file)
            print(f"      Records loaded: {len(records):,}")
            
            # Apply data quality filtering
            filter_system = ScopusDataQualityFilter()
            filtered_records = filter_system.filter_data(records)
            
            excluded_count = len(records) - len(filtered_records)
            if excluded_count > 0:
                print(f"      Records excluded: {excluded_count}")
            
            # Add to database (with deduplication)
            added_count = db_creator.add_records_to_existing_database(filtered_records)
            new_records_count += added_count
            print(f"      New unique records added: {added_count}")
        
        # Generate updated reports
        print(f"\n📊 Generating updated reports...")
        
        # Get final database stats
        final_info = get_existing_database_info(db_path)
        
        print(f"\n✅ INCREMENTAL UPDATE COMPLETE!")
        print(f"=" * 60)
        print(f"New records processed: {sum(len(db_creator.load_csv_data(f)) for f in new_csv_files):,}")
        print(f"New unique records added: {new_records_count:,}")
        print(f"Total papers in database: {final_info['paper_count']:,}")
        print(f"Year range: {final_info['year_range'][0]} - {final_info['year_range'][1]}")
        
        # Cleanup temp directory
        shutil.rmtree(temp_dir, ignore_errors=True)
        
        return True
        
    except Exception as e:
        print(f"❌ Error during incremental update: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main incremental update function"""
    # Configuration
    base_dir = "data/2. Scopus query"
    additional_csv_dir = os.path.join(base_dir, "Additional RAW")
    existing_db_path = "data/2. Scopus query/RAW /RAW _combined_research_optimized_20250729_194013.db"
    output_dir = os.path.join(base_dir, "RAW ", "output")
    log_path = os.path.join(output_dir, "processed_files_log.json")
    
    print(f"🔍 SCOPUS DATABASE INCREMENTAL UPDATE")
    print(f"=" * 60)
    print(f"Looking for new CSV files in: {additional_csv_dir}")
    print(f"Existing database: {existing_db_path}")
    
    # Check if directories exist
    if not os.path.exists(additional_csv_dir):
        print(f"❌ Directory not found: {additional_csv_dir}")
        return False
    
    if not os.path.exists(existing_db_path):
        print(f"❌ Existing database not found: {existing_db_path}")
        return False
    
    # Load processed files log
    processed_log = load_processed_files_log(log_path)
    print(f"Previously processed files: {len(processed_log.get('processed_files', []))}")
    
    # Find new CSV files
    new_csv_files = find_new_csv_files(additional_csv_dir, processed_log)
    
    if not new_csv_files:
        print(f"✅ No new CSV files found to process")
        return True
    
    print(f"New CSV files found: {len(new_csv_files)}")
    
    # Update database with new files
    success = update_database_with_new_files(existing_db_path, new_csv_files, output_dir)
    
    if success:
        # Rename processed files and update log
        timestamp = get_timestamp()
        total_files = len(new_csv_files)
        
        print(f"\n📝 Renaming processed files...")
        for i, csv_file in enumerate(new_csv_files, 1):
            try:
                new_path = rename_processed_file(csv_file, i, total_files, timestamp)
                print(f"   ✅ Renamed: {os.path.basename(csv_file)} → {os.path.basename(new_path)}")
                
                # Add to processed log
                processed_log["processed_files"].append(os.path.relpath(new_path))
                processed_log["total_processed"] = processed_log.get("total_processed", 0) + 1
                
            except Exception as e:
                print(f"   ❌ Failed to rename {csv_file}: {e}")
        
        # Save updated log
        save_processed_files_log(log_path, processed_log)
        print(f"   ✅ Updated processed files log")
        
        print(f"\n🎉 Incremental update completed successfully!")
        return True
    
    return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
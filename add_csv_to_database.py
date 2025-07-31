#!/usr/bin/env python3
"""
Add Additional CSV Files to Existing Scopus Database
Simple script to process new CSV files and add them to existing database
"""

import os
import sys
import sqlite3
import shutil
import json
import glob
import csv
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
        if rel_path not in processed_files and not os.path.basename(file_path).startswith("processed-"):
            new_files.append(file_path)
    
    return sorted(new_files)

def count_csv_records(csv_file):
    """Count records in CSV file"""
    try:
        with open(csv_file, 'r', encoding='utf-8-sig', newline='') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header
            return sum(1 for row in reader)
    except Exception as e:
        print(f"Warning: Could not count records in {csv_file}: {e}")
        return 0

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
            SELECT CAST(year as INTEGER) as year_int, COUNT(*) as count
            FROM papers 
            WHERE year IS NOT NULL 
                AND year != '' 
                AND year != 'null'
                AND year NOT LIKE '%null%'
                AND CAST(year as INTEGER) BETWEEN 2016 AND 2025
            GROUP BY CAST(year as INTEGER)
            ORDER BY CAST(year as INTEGER) DESC
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
        found = yearly_counts.get(year, 0)  # Use integer key
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
        found = yearly_counts.get(year, 0)  # Use integer key
        percent_found = (found / expected * 100) if expected > 0 else 0
        if percent_found < 80:
            problem_years.append((year, percent_found, expected - found))
    
    if problem_years:
        print(f"\n🚨 YEARS NEEDING ATTENTION:")
        for year, percent, missing in problem_years:
            print(f"   {year}: {percent:.1f}% found ({missing:,} missing documents)")
    
    return total_found, total_expected, total_missing


def main():
    """Main function to add CSV files to existing database"""
    import sys
    
    # Get keyword from command line argument
    if len(sys.argv) < 2:
        print("❌ Usage: python3 add_csv_to_database.py <keyword>")
        print("   Example: python3 add_csv_to_database.py 3DP")
        return False
    
    keyword = sys.argv[1]
    
    # Configuration
    csv_dir = "data/2. Scopus query/RAW"  # CSV files to process
    parent_dir = "data/2. Scopus query"  # Database will be created here
    timestamp = get_timestamp()
    run_output_dir = os.path.join(parent_dir, "output", f"Run_{timestamp}")  # Timestamped output subfolder
    
    # Create timestamped output directory
    os.makedirs(run_output_dir, exist_ok=True)
    
    # Look for existing master database
    existing_master_db = os.path.join(parent_dir, f"master-{keyword}.db")
    log_path = os.path.join(run_output_dir, "processed_files_log.json")
    
    print(f"🔍 ADDING NEW CSV FILES TO SCOPUS DATABASE")
    print(f"=" * 60)
    print(f"Keyword: {keyword}")
    print(f"CSV directory: {csv_dir}")
    print(f"Master database: {existing_master_db}")
    print(f"Run output directory: {run_output_dir}")
    
    # Check if paths exist
    if not os.path.exists(csv_dir):
        print(f"❌ Directory not found: {csv_dir}")
        return False
    
    # Load processed files log
    processed_log = load_processed_files_log(log_path)
    print(f"Previously processed files: {len(processed_log.get('processed_files', []))}")
    
    # Find new CSV files
    new_csv_files = find_new_csv_files(csv_dir, processed_log)
    
    if not new_csv_files:
        print(f"✅ No new CSV files found to process")
        return True
    
    print(f"\n📋 NEW CSV FILES FOUND: {len(new_csv_files)}")
    for i, csv_file in enumerate(new_csv_files, 1):
        filename = os.path.basename(csv_file)
        record_count = count_csv_records(csv_file)
        print(f"   {i}. {filename} ({record_count:,} records)")
    
    # Get current database info if exists
    if os.path.exists(existing_master_db):
        db_info = get_database_info(existing_master_db)
        if db_info:
            print(f"\n📊 EXISTING MASTER DATABASE:")
            print(f"   Papers: {db_info['paper_count']:,}")
            print(f"   Years: {db_info['year_range'][0]} - {db_info['year_range'][1]}")
            
            # Compare with expected counts
            compare_with_expected_counts(db_info['yearly_counts'])
    else:
        print(f"\n📊 No existing master database found - will create new one")
    
    # Auto-confirm processing for batch mode
    print(f"\n🚀 Auto-processing {len(new_csv_files)} CSV files...")
    print("   (No confirmation required for batch processing)")
    
    try:
        # Use timestamp already created for output directory
        
        # Use multi-CSV mode to enable proper deduplication
        print(f"\n🚀 Processing {len(new_csv_files)} CSV files with deduplication...")
        print(f"   Database will be created in: {parent_dir}")
        print(f"   Using multi-CSV mode to enable automatic deduplication")
        print(f"   This will remove duplicate records across all CSV files")
        
        # Use absolute paths to avoid directory change issues
        original_dir = os.getcwd()
        abs_script_path = os.path.abspath("1_create_database.py")
        abs_csv_dir = os.path.abspath(csv_dir)
        
        # Check for query file in the parent directory
        query_file_path = os.path.join(parent_dir, "scopus_q_2025_07.json")
        if not os.path.exists(query_file_path):
            # Try alternative naming patterns
            for pattern in ["scopus_q_*.json", "scopus_query_*.json", "query_*.json"]:
                matches = glob.glob(os.path.join(parent_dir, pattern))
                if matches:
                    query_file_path = matches[0]  # Use the first match
                    break
            else:
                query_file_path = None
        
        # Build command with optional query file
        if query_file_path and os.path.exists(query_file_path):
            abs_query_file = os.path.abspath(query_file_path)
            cmd = f'cd "{parent_dir}" && python3 "{abs_script_path}" "{abs_csv_dir}" "{keyword}" "{abs_query_file}"'
            print(f"   🔍 Using query file: {os.path.basename(query_file_path)}")
        else:
            cmd = f'cd "{parent_dir}" && python3 "{abs_script_path}" "{abs_csv_dir}" "{keyword}"'
        
        print(f"\n🔧 Running database creation command...")
        result = os.system(cmd)
        
        # No need to change back since we didn't change directories
        
        if result != 0:
            print(f"❌ Database creation failed")
            return False
        
        # Move all output files from the temp location to the run output directory
        print(f"\n📂 Moving output files to: {run_output_dir}")
        
        # Look for output files created by the database creation process
        temp_output_dir = os.path.join(parent_dir, "output")
        if os.path.exists(temp_output_dir):
            # Move all files from temp output to run output
            for filename in os.listdir(temp_output_dir):
                src_path = os.path.join(temp_output_dir, filename)
                dst_path = os.path.join(run_output_dir, filename)
                if os.path.isfile(src_path):
                    shutil.move(src_path, dst_path)
                    print(f"   📄 Moved: {filename}")
        
        # Also move any validation reports from parent directory to run output
        for filename in os.listdir(parent_dir):
            if filename.startswith("database_validation_report_") or filename.startswith("database_creation_debug_"):
                src_path = os.path.join(parent_dir, filename)
                dst_path = os.path.join(run_output_dir, filename)
                shutil.move(src_path, dst_path)
                print(f"   📄 Moved: {filename}")
        
        # Move logs from logs/ directory to run output
        logs_dir = os.path.join(parent_dir, "logs")
        if os.path.exists(logs_dir):
            for filename in os.listdir(logs_dir):
                if filename.startswith("database_creation_debug_"):
                    src_path = os.path.join(logs_dir, filename)
                    dst_path = os.path.join(run_output_dir, filename)
                    shutil.move(src_path, dst_path)
                    print(f"   📄 Moved log: {filename}")
            
            # Remove empty logs directory if it only contained our debug logs
            try:
                if not os.listdir(logs_dir):  # Check if directory is empty
                    os.rmdir(logs_dir)
                    print(f"   🗑️ Removed empty logs directory")
            except OSError:
                pass  # Directory not empty, leave it
        
        # The system creates a new database in the timestamped run folder
        print(f"\n✅ Database created successfully!")
        print(f"   Look for the database in: {run_output_dir}")
        
        # Find the new database in the run output directory
        new_db_files = glob.glob(os.path.join(run_output_dir, f"master_{timestamp}*.db"))
        if new_db_files:
            new_db_path = new_db_files[0]
            db_filename = os.path.basename(new_db_path)
            
            print(f"   📁 Database created: {db_filename}")
            print(f"   📂 Location: {run_output_dir}")
            
            # Create a symlink in parent directory for easy access (optional)
            latest_link = os.path.join(parent_dir, f"latest-{keyword}.db")
            if os.path.exists(latest_link) or os.path.islink(latest_link):
                os.remove(latest_link)
            try:
                os.symlink(new_db_path, latest_link)
                print(f"   🔗 Created symlink: latest-{keyword}.db → {db_filename}")
            except Exception as e:
                print(f"   ℹ️  Could not create symlink (not critical): {e}")
            
            print(f"\n📊 ANALYZING NEW DATABASE: {db_filename}")
            new_db_info = get_database_info(new_db_path)
            if new_db_info:
                print(f"   Papers in database: {new_db_info['paper_count']:,}")
                print(f"   Year range: {new_db_info['year_range'][0]} - {new_db_info['year_range'][1]}")
                
                # Show yearly breakdown for new data
                if new_db_info['yearly_counts']:
                    print(f"\n📈 DATA BY YEAR:")
                    for year in sorted(new_db_info['yearly_counts'].keys(), reverse=True):
                        count = new_db_info['yearly_counts'][year]
                        print(f"   {year}: {count:,} papers")
                
                # Compare with expected counts
                print(f"\n🎯 COVERAGE ANALYSIS:")
                compare_with_expected_counts(new_db_info['yearly_counts'])
        else:
            print(f"⚠️ Could not find the new database file in: {run_output_dir}")
        
        # Clean up any temporary database files (multi-CSV mode creates fewer temp files)
        print(f"\n🧹 Cleaning up temporary files...")
        temp_files_removed = 0
        for pattern in ["*_research_optimized_*.db-journal"]:
            for temp_file in glob.glob(os.path.join(parent_dir, pattern)):
                try:
                    os.remove(temp_file)
                    print(f"   🗑️ Removed: {os.path.basename(temp_file)}")
                    temp_files_removed += 1
                except Exception as e:
                    print(f"   ⚠️ Could not remove {os.path.basename(temp_file)}: {e}")
        
        if temp_files_removed == 0:
            print(f"   ✨ No temporary files to clean up")
        
        # Rename processed files and update log
        print(f"\n📝 Renaming processed files...")
        total_files = len(new_csv_files)
        
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
        
        print(f"\n🎉 CSV files processed and renamed successfully!")
        print(f"   Check the output directory for the new database and reports")
        
        return True
        
    except Exception as e:
        print(f"❌ Error during processing: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
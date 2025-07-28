#!/usr/bin/env python3
"""
Data Folder Organization Script

This script helps organize existing Scopus export folders into the recommended
structure with raw_scopus/, output/, and database files properly separated.
"""

import shutil
import sys
from pathlib import Path
from typing import List


def organize_export_folder(export_path: Path) -> bool:
    """
    Organize a single export folder into the recommended structure.
    
    Args:
        export_path: Path to the export folder to organize
        
    Returns:
        True if organization was successful, False otherwise
    """
    if not export_path.exists():
        print(f"❌ Export folder does not exist: {export_path}")
        return False
    
    if not export_path.is_dir():
        print(f"❌ Path is not a directory: {export_path}")
        return False
    
    print(f"📁 Organizing: {export_path}")
    
    # Check if already organized
    raw_scopus_dir = export_path / "raw_scopus"
    output_dir = export_path / "output"
    
    if raw_scopus_dir.exists() and any(raw_scopus_dir.glob("*.csv")):
        print(f"✅ Already organized - has raw_scopus/ with CSV files")
        return True
    
    # Create directories
    raw_scopus_dir.mkdir(exist_ok=True)
    output_dir.mkdir(exist_ok=True)
    print(f"   📂 Created: raw_scopus/ and output/ directories")
    
    # Find files to move
    csv_files = list(export_path.glob("*.csv"))
    md_files = list(export_path.glob("*.md"))
    txt_files = list(export_path.glob("*.txt"))
    
    # Find output files to move
    quality_files = []
    for pattern in ["data_quality_exclusions_*", "*_exclusions_*", "*_report_*"]:
        quality_files.extend(export_path.glob(pattern))
    
    db_files = list(export_path.glob("*.db"))
    
    print(f"   📄 Found {len(csv_files)} CSV files to move to raw_scopus/")
    print(f"   📋 Found {len(md_files + txt_files)} documentation files to move to raw_scopus/")
    print(f"   📊 Found {len(quality_files)} output files to move to output/")
    print(f"   🗄️ Found {len(db_files)} database files (staying in main directory)")
    
    # Move CSV and documentation files to raw_scopus/
    files_moved = 0
    for file_list, description in [(csv_files, "CSV"), (md_files, "Markdown"), (txt_files, "Text")]:
        for file_path in file_list:
            try:
                dest_path = raw_scopus_dir / file_path.name
                if not dest_path.exists():
                    shutil.move(str(file_path), str(dest_path))
                    files_moved += 1
                    print(f"      ✅ Moved {description}: {file_path.name}")
                else:
                    print(f"      ⚠️  Skipped {description} (already exists): {file_path.name}")
            except Exception as e:
                print(f"      ❌ Error moving {file_path.name}: {e}")
    
    # Move output files to output/
    for file_path in quality_files:
        try:
            dest_path = output_dir / file_path.name
            if not dest_path.exists():
                shutil.move(str(file_path), str(dest_path))
                files_moved += 1
                print(f"      ✅ Moved output file: {file_path.name}")
            else:
                print(f"      ⚠️  Skipped output file (already exists): {file_path.name}")
        except Exception as e:
            print(f"      ❌ Error moving {file_path.name}: {e}")
    
    print(f"   🎉 Organization complete! Moved {files_moved} files")
    return True


def find_export_folders(data_dir: Path) -> List[Path]:
    """Find potential export folders in the data directory."""
    export_folders = []
    
    if not data_dir.exists():
        return export_folders
    
    # Look for directories that contain CSV files
    for item in data_dir.iterdir():
        if item.is_dir():
            csv_files = list(item.glob("*.csv"))
            if csv_files:
                export_folders.append(item)
    
    return sorted(export_folders)


def main():
    """Main function to organize data folders."""
    print("📁 SCOPUS DATA FOLDER ORGANIZER")
    print("=" * 50)
    print()
    
    # Check command line arguments
    if len(sys.argv) > 1:
        # Specific folder provided
        export_path = Path(sys.argv[1])
        success = organize_export_folder(export_path)
        sys.exit(0 if success else 1)
    
    # Look for export folders in data/ directory
    data_dir = Path("data")
    if not data_dir.exists():
        print("❌ No data/ directory found")
        print("   Create a data/ directory and place your Scopus export folders inside")
        sys.exit(1)
    
    export_folders = find_export_folders(data_dir)
    
    if not export_folders:
        print("❌ No export folders found in data/ directory")
        print("   Export folders should contain CSV files")
        sys.exit(1)
    
    print(f"🔍 Found {len(export_folders)} export folders:")
    for folder in export_folders:
        csv_count = len(list(folder.glob("*.csv")))
        print(f"   • {folder} ({csv_count} CSV files)")
    print()
    
    # Ask user for confirmation
    while True:
        response = input("❓ Organize all export folders? [y/N]: ").strip().lower()
        if response in ['y', 'yes']:
            break
        elif response in ['n', 'no', '']:
            print("❌ Organization cancelled")
            sys.exit(0)
        else:
            print("   ⚠️  Please enter 'y' for yes or 'n' for no")
    
    # Organize all folders
    success_count = 0
    for folder in export_folders:
        if organize_export_folder(folder):
            success_count += 1
        print()
    
    print("=" * 50)
    print(f"🎉 ORGANIZATION COMPLETE")
    print(f"   ✅ Successfully organized: {success_count}/{len(export_folders)} folders")
    
    if success_count == len(export_folders):
        print(f"   🚀 Ready for database creation!")
        print(f"   📋 Usage: python create_database.py data/export_name/")
    else:
        print(f"   ⚠️  Some folders had issues - check output above")
    
    sys.exit(0 if success_count == len(export_folders) else 1)


if __name__ == "__main__":
    main()
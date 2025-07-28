#!/usr/bin/env python3
"""
SCOPUS DATABASE CREATOR - BEGINNER-FRIENDLY SCRIPT
==================================================

This script converts Scopus CSV files into optimized SQLite databases for research.
It's designed to be easy to use, even if you're new to programming!

WHAT THIS SCRIPT DOES:
- Takes Scopus CSV files (exported from Scopus.com) as input
- Converts them into organized SQLite database files
- Automatically filters out low-quality data (always enabled for best results)
- Can handle single files OR entire directories with multiple CSV files

HOW TO USE:
  python create_database.py <scopus.csv>        # For a single CSV file
  python create_database.py <directory>         # For a folder with multiple CSV files

EXAMPLES:
  python create_database.py my_scopus_data.csv
  python create_database.py data/scopus_exports/
"""

# Import required Python modules
# sys: Lets us access command-line arguments (what the user types after the script name)
# Path: Makes it easier to work with file and folder paths
# OptimalScopusDatabase: The main class that does the heavy lifting of creating databases
import sys
from pathlib import Path
from scopus_db import OptimalScopusDatabase


def find_scopus_csv_files(directory_path: Path) -> list:
    """
    HELPER FUNCTION: Find CSV files in a directory
    
    This function looks through a folder and finds all the CSV files that look like
    Scopus data files. It's smart enough to skip temporary files and backup files.
    
    Args:
        directory_path: A Path object pointing to the folder to search
        
    Returns:
        A sorted list of CSV files found in the directory
    """
    # Create an empty list to store the CSV files we find
    csv_files = []
    
    # Look through all files in the directory that end with ".csv"
    # The glob("*.csv") function finds all files ending in .csv
    for csv_file in directory_path.glob("*.csv"):
        
        # Skip files we don't want to process:
        # - Files starting with "~" (temporary files)
        # - Files starting with "." (hidden files)
        # - Files with "exclusion" in the name (our own log files)
        # - Files ending with "_backup.csv" (backup files)
        should_skip_file = (
            csv_file.name.startswith("~") or           # Temporary files
            csv_file.name.startswith(".") or           # Hidden files  
            "exclusion" in csv_file.name.lower() or    # Exclusion log files
            csv_file.name.endswith("_backup.csv")      # Backup files
        )
        
        if should_skip_file:
            continue  # Skip this file and move to the next one
            
        # If we get here, it's a good CSV file - add it to our list
        csv_files.append(csv_file)
    
    # Return the list of files, sorted alphabetically for consistency
    return sorted(csv_files)


def main():
    """
    MAIN FUNCTION: This is where the program starts running
    
    This function handles the user input, decides what to do, and coordinates
    the database creation process. It's like the "control center" of the script.
    """
    
    # STEP 1: Check if the user provided the right number of arguments
    # sys.argv is a list of command-line arguments:
    #   sys.argv[0] = the script name (create_database.py)  
    #   sys.argv[1] = the first argument (should be CSV file or directory)
    # We need exactly 2 items: script name + 1 argument
    if len(sys.argv) != 2:
        # If wrong number of arguments, show help message and exit
        print("❌ ERROR: You need to provide exactly one argument!")
        print("")
        print("CORRECT USAGE:")
        print("  python create_database.py <scopus.csv|directory>")
        print("")
        print("EXAMPLES:")
        print("  # Process a single CSV file:")
        print("  python create_database.py data/scopus_exports/export_1/scopus.csv")
        print("")
        print("  # Process all CSV files in a directory:")
        print("  python create_database.py data/scopus_exports/export_1/")
        print("")
        print("WHAT HAPPENS WHEN YOU RUN THIS:")
        print("📁 Multi-CSV Processing:")
        print("   - Combines all CSV files in directory into single database")
        print("   - Automatically detects and removes duplicate records")
        print("   - Reports source files and deduplication statistics")
        print("")
        print("🔍 Data Quality Filtering (automatic):")
        print("   - Automatically filters out low-quality research entries")
        print("   - Generates detailed exclusion log showing what was filtered")
        print("   - Ensures your database contains only high-quality research data")
        print("")
        print("TIP: Make sure your file path doesn't have spaces, or put it in quotes!")
        sys.exit(1)  # Exit the program with error code 1
    
    # STEP 2: Get the file/directory path from the command line argument
    # Path() creates a "Path object" that makes it easier to work with files and folders
    input_path = Path(sys.argv[1])
    
    # Let the user know that data quality filtering is always on
    print("✅ Data quality filtering ENABLED - ensuring high-quality research data")
    print("   (This automatically removes low-quality entries for better results)")
    print("")
    
    # STEP 3: Determine if the user gave us a directory or a single file
    # We handle these two cases differently
    
    if input_path.is_dir():
        # CASE A: User provided a DIRECTORY (folder) path
        print("🔍 DIRECTORY MODE: Looking for CSV files in the directory...")
        
        # Use our helper function to find all CSV files in the directory
        csv_files = find_scopus_csv_files(input_path)
        
        # Check if we actually found any CSV files
        if not csv_files:
            print(f"❌ ERROR: No CSV files found in directory '{input_path}'")
            print("   POSSIBLE SOLUTIONS:")
            print("   - Make sure the directory path is correct")
            print("   - Check that the directory contains Scopus export CSV files")
            print("   - CSV files should end with '.csv' extension")
            sys.exit(1)
        
        # Show the user what we found
        print(f"📁 MULTI-CSV PROCESSING MODE ACTIVATED")
        print(f"   📍 Directory: {input_path}")
        print(f"   📊 CSV files found: {len(csv_files)}")
        print("   📋 Files to be processed:")
        
        # List all the files we found (with numbers for clarity)
        for i, csv_file in enumerate(csv_files, 1):
            print(f"      {i}. {csv_file.name}")
        print()
        
        # SAFETY CHECK: Ask user for confirmation before processing
        # This prevents accidentally processing the wrong files
        print("⚠️  IMPORTANT: All these files will be combined into ONE database!")
        while True:
            response = input("❓ Proceed with processing these CSV files? [y/N]: ").strip().lower()
            if response in ['y', 'yes']:
                print("✅ User confirmed - starting processing...")
                break
            elif response in ['n', 'no', '']:
                print("❌ Processing cancelled by user.")
                print("   No files were processed. You can run the script again anytime.")
                sys.exit(0)  # Exit cleanly (no error)
            else:
                print("   ⚠️  Please enter 'y' for yes or 'n' for no (or just press Enter for no).")
        
        # Create the database creator object for multiple CSV files
        # This is the object that will do all the heavy lifting
        print("🔧 Setting up database creator for multiple CSV files...")
        db_creator = OptimalScopusDatabase(
            csv_path=input_path,              # The directory path
            enable_data_filtering=True,       # Always filter for quality
            csv_files=csv_files              # The list of CSV files we found
        )
        
    elif input_path.is_file() and input_path.suffix.lower() == '.csv':
        # CASE B: User provided a SINGLE CSV FILE path
        print("📄 SINGLE FILE MODE: Processing one CSV file...")
        print(f"   📍 File: {input_path}")
        print(f"   📊 File size: {input_path.stat().st_size / (1024*1024):.1f} MB")
        print()
        
        # SAFETY CHECK: Ask user for confirmation before processing
        # This gives them a chance to double-check they have the right file
        print("ℹ️  This file will be converted into a SQLite database.")
        while True:
            response = input("❓ Proceed with processing this CSV file? [y/N]: ").strip().lower()
            if response in ['y', 'yes']:
                print("✅ User confirmed - starting processing...")
                break
            elif response in ['n', 'no', '']:
                print("❌ Processing cancelled by user.")
                print("   No files were processed. You can run the script again anytime.")
                sys.exit(0)  # Exit cleanly (no error)  
            else:
                print("   ⚠️  Please enter 'y' for yes or 'n' for no (or just press Enter for no).")
        
        # Create the database creator object for a single CSV file
        print("🔧 Setting up database creator for single CSV file...")
        db_creator = OptimalScopusDatabase(
            csv_path=input_path,              # The CSV file path
            enable_data_filtering=True        # Always filter for quality
        )
        
    else:
        # CASE C: User provided something that's not a CSV file or directory
        print(f"❌ ERROR: '{input_path}' is not a valid CSV file or directory!")
        print("")
        print("🤔 WHAT WENT WRONG:")
        if input_path.exists():
            if input_path.is_file():
                print(f"   - You provided a file, but it doesn't end with '.csv'")
                print(f"   - File extension found: '{input_path.suffix}'")
                print(f"   - Scopus files should end with '.csv'")
            else:
                print(f"   - The path exists but it's neither a file nor a directory")
        else:
            print(f"   - The path doesn't exist (check for typos)")
        print("")
        print("✅ WHAT YOU CAN PROVIDE:")
        print("   - A single CSV file: python create_database.py scopus.csv")
        print("   - A directory with CSV files: python create_database.py export_folder/")
        print("")
        print("💡 TIP: Use quotes around paths with spaces: 'my folder/file.csv'")
        sys.exit(1)
    
    # STEP 4: Actually create the database!
    # This is where the magic happens - we have 3 phases:
    
    print("\n🚀 STARTING DATABASE CREATION PROCESS...")
    print("    This might take a few minutes depending on your data size.")
    print()
    
    # PHASE 1: Set up the database structure (tables, indexes, etc.)
    print("📋 PHASE 1: Creating database structure (tables and indexes)...")
    db_creator.create_optimal_schema()
    print("   ✅ Database structure created successfully!")
    
    # PHASE 2: Read the CSV data and insert it into the database
    # This is usually the longest part - it reads all your CSV data,
    # filters it for quality, and organizes it into the database
    print("📊 PHASE 2: Processing CSV data and populating database...")
    print("   (This is the longest step - please be patient!)")
    db_creator.process_csv_to_optimal_db()
    print("   ✅ Data processing completed successfully!")
    
    # PHASE 3: Clean up and close the database connection
    print("🔒 PHASE 3: Finalizing and closing database...")
    db_creator.conn.close()  # Always close database connections when done!
    print("   ✅ Database finalized and closed!")
    
    # STEP 5: Tell the user where to find their new database
    print("\n🎉 SUCCESS! Your Scopus database has been created!")
    print("=" * 60)
    print(f"📁 Database location: {db_creator.db_path}")
    print(f"📝 Quality filter log: {db_creator.data_filter.log_path}")
    print("=" * 60)
    print("")
    print("💡 WHAT'S NEXT:")
    print("   - You can open the .db file with any SQLite browser")
    print("   - Check the exclusion log to see what data was filtered out")
    print("   - The database is ready for research analysis!")
    print("")
    print("🔍 QUICK TIPS:")
    print("   - Database file size should be smaller than the original CSV")
    print("   - The exclusion log shows exactly what was filtered and why")
    print("   - You can use tools like DB Browser for SQLite to explore your data")


# This is a Python idiom that means "only run main() if this script is run directly"
# It prevents main() from running if someone imports this file as a module
if __name__ == "__main__":
    main()
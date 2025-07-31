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
import logging
import time
import os
from datetime import datetime
from pathlib import Path
from scopus_db import OptimalScopusDatabase


def setup_detailed_logging(base_dir=None, run_folder=None):
    """Setup detailed logging for debugging database creation process."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"database_creation_debug_{timestamp}.log"
    
    # Determine output directory - prefer run_folder, then base_dir/output, then current directory
    if run_folder:
        log_path = run_folder / log_filename
    elif base_dir:
        output_dir = base_dir / "output"
        output_dir.mkdir(exist_ok=True)
        log_path = output_dir / log_filename
    else:
        # Fallback to logs directory in current directory
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        log_path = log_dir / log_filename
    
    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_path, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info("=" * 80)
    logger.info("SCOPUS DATABASE CREATION - DETAILED DEBUG LOG")
    logger.info("=" * 80)
    logger.info(f"Log file: {log_path}")
    logger.info(f"Start time: {datetime.now()}")
    logger.info(f"Command: {' '.join(sys.argv)}")
    
    return logger, log_path


def move_log_to_run_folder(current_log_path, run_folder, logger):
    """Move the log file to the proper run folder location."""
    try:
        new_log_path = run_folder / current_log_path.name
        
        # Close current file handler and move the file
        for handler in logger.handlers[:]:
            if isinstance(handler, logging.FileHandler):
                handler.close()
                logger.removeHandler(handler)
        
        # Move the log file
        import shutil
        shutil.move(str(current_log_path), str(new_log_path))
        
        # Add new file handler
        new_handler = logging.FileHandler(new_log_path, encoding='utf-8')
        new_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        new_handler.setFormatter(formatter)
        logger.addHandler(new_handler)
        
        logger.info(f"📋 Log file moved to run folder: {new_log_path}")
        return new_log_path
        
    except Exception as e:
        logger.warning(f"⚠️ Could not move log file to run folder: {e}")
        return current_log_path


def log_stage_start(logger, stage_name, details=None):
    """Log the start of a processing stage with timing."""
    logger.info("=" * 60)
    logger.info(f"🚀 STARTING STAGE: {stage_name}")
    if details:
        logger.info(f"   Details: {details}")
    logger.info(f"   Stage start time: {datetime.now()}")
    return time.time()


def log_stage_end(logger, stage_name, start_time, success=True, details=None):
    """Log the end of a processing stage with timing."""
    duration = time.time() - start_time
    status = "✅ COMPLETED" if success else "❌ FAILED"
    logger.info(f"{status} STAGE: {stage_name}")
    logger.info(f"   Duration: {duration:.2f} seconds")
    if details:
        logger.info(f"   Details: {details}")
    logger.info("=" * 60)


def find_scopus_csv_files(directory_path: Path) -> list:
    """
    HELPER FUNCTION: Find CSV files in a directory or raw_scopus subdirectory
    
    This function looks through a folder and finds all the CSV files that look like
    Scopus data files. It checks both the main directory and the raw_scopus/ subfolder
    for organized data structures.
    
    Args:
        directory_path: A Path object pointing to the folder to search
        
    Returns:
        A sorted list of CSV files found in the directory
    """
    # Create an empty list to store the CSV files we find
    csv_files = []
    
    # Check if this is an organized export folder with raw_scopus/ subfolder
    raw_scopus_dir = directory_path / "raw_scopus"
    if raw_scopus_dir.exists() and raw_scopus_dir.is_dir():
        print(f"📁 Found organized structure - looking in raw_scopus/ folder")
        search_dir = raw_scopus_dir
    else:
        # Use the main directory if no raw_scopus/ folder exists
        search_dir = directory_path
    
    # Look through all files in the search directory that end with ".csv"
    # The glob("*.csv") function finds all files ending in .csv
    for csv_file in search_dir.glob("*.csv"):
        
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
    
    # Check for environment variables first
    csv_dir_env = os.getenv('SCOPUS_CSV_DIR')
    keyword_env = os.getenv('SCOPUS_KEYWORD')
    query_file_env = os.getenv('SCOPUS_QUERY_FILE')
    
    # Parse command line arguments with environment variable fallback
    query_file = None
    
    if len(sys.argv) == 4:
        # Three arguments provided: directory, keyword, query_file
        csv_directory = sys.argv[1]
        keyword = sys.argv[2]
        query_file = sys.argv[3]
    elif len(sys.argv) == 3:
        # Two arguments provided
        csv_directory = sys.argv[1]
        keyword = sys.argv[2]
        query_file = query_file_env  # Use env var if available
    elif len(sys.argv) == 2 and csv_dir_env:
        # One argument + env variable for directory
        csv_directory = csv_dir_env
        keyword = sys.argv[1]
        query_file = query_file_env
    elif len(sys.argv) == 1 and csv_dir_env and keyword_env:
        # Both from environment variables
        csv_directory = csv_dir_env
        keyword = keyword_env
        query_file = query_file_env
    else:
        # Show help message
        print("❌ ERROR: Invalid arguments!")
        print("")
        print("USAGE OPTIONS:")
        print("  1. python 1_create_database.py <csv_directory> <keyword> <query_file>")
        print("  2. python 1_create_database.py <csv_directory> <keyword>")
        print("  3. python 1_create_database.py <keyword>  (with SCOPUS_CSV_DIR env var)")
        print("  4. python 1_create_database.py  (with env vars)")
        print("")
        print("EXAMPLES:")
        print("  # All 3 parameters (recommended):")
        print("  python 1_create_database.py 'data/2. Scopus query/RAW' 3DP 'data/2. Scopus query/scopus_q_2025_07.json'")
        print("")
        print("  # Just CSV directory and keyword:")
        print("  python 1_create_database.py 'data/2. Scopus query/RAW' 3DP")
        print("")
        print("  # With environment variables:")
        print("  export SCOPUS_CSV_DIR='data/2. Scopus query/RAW'")
        print("  export SCOPUS_QUERY_FILE='data/2. Scopus query/scopus_q_2025_07.json'")
        print("  python 1_create_database.py 3DP")
        print("")
        print("  # All from environment:")
        print("  export SCOPUS_CSV_DIR='data/2. Scopus query/RAW'")
        print("  export SCOPUS_KEYWORD='3DP'")
        print("  export SCOPUS_QUERY_FILE='data/2. Scopus query/scopus_q_2025_07.json'")
        print("  python 1_create_database.py")
        print("")
        print("🔍 Features:")
        print("   - Multi-CSV processing with automatic deduplication")
        print("   - Data quality filtering")
        print("   - Master database naming: master-{timestamp}.db")
        print("   - Timestamped output organization")
        print("   - Optional Scopus query tracking")
        sys.exit(1)
    
    # STEP 2: Create Path object from the determined CSV directory
    # Path() creates a "Path object" that makes it easier to work with files and folders
    input_path = Path(csv_directory)
    
    # STEP 3: Determine base directory for outputs (database, logs, reports)
    if input_path.is_dir():
        base_dir = input_path.parent  # Use parent directory for outputs and database
    else:
        print(f"❌ ERROR: '{input_path}' is not a valid directory!")
        print("   The first argument must be a directory containing CSV files.")
        sys.exit(1)
    
    # STEP 4: Setup detailed logging for debugging with proper output directory
    logger, log_path = setup_detailed_logging(base_dir)
    overall_start_time = time.time()
    
    try:
        logger.info("🔧 INITIALIZATION: Setting up database creation process")
        logger.info(f"📂 Working directory: {Path.cwd()}")
        logger.info(f"📄 Input path: {input_path}")
        logger.info(f"📁 Base output directory: {base_dir}")
        logger.info(f"📋 Debug log location: {log_path}")
        
        # Let the user know that data quality filtering is always on
        print("✅ Data quality filtering ENABLED - ensuring high-quality research data")
        print("   (This automatically removes low-quality entries for better results)")
        print("")
    
        # STEP 5: Process CSV files in directory with keyword-based database naming
        print("🔍 MULTI-CSV PROCESSING: Looking for CSV files in the directory...")
        
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
        print(f"📁 PROCESSING SETUP")
        print(f"   📍 Directory: {input_path}")
        print(f"   🔖 Keyword: {keyword}")
        print(f"   📊 CSV files found: {len(csv_files)}")
        if query_file:
            print(f"   🔍 Query file: {query_file}")
        print("   📋 Files to be processed:")
        
        # List all the files we found (with numbers for clarity)
        for i, csv_file in enumerate(csv_files, 1):
            print(f"      {i}. {csv_file.name}")
        print()
        
        # AUTO-PROCESSING: Files will be automatically processed
        print("⚠️  IMPORTANT: All these files will be combined into ONE database!")
        print(f"   Database will be created in: output/Run_[timestamp]/master_[timestamp].db")
        print("✅ Auto-processing enabled - starting database creation...")
        
        # Create the database creator object for multiple CSV files
        # This is the object that will do all the heavy lifting
        print("🔧 Setting up database creator for multiple CSV files...")
        db_creator = OptimalScopusDatabase(
            csv_path=input_path,              # The directory path
            enable_data_filtering=True,       # Always filter for quality
            csv_files=csv_files,             # The list of CSV files we found
            keyword=keyword,                 # Keyword for master database naming
            query_file=query_file            # Optional Scopus query file
        )
        
        # Move log file to the proper run folder
        run_folder = db_creator.db_path.parent  # Get the run folder path
        log_path = move_log_to_run_folder(log_path, run_folder, logger)
        
        
        # STEP 6: Load and display configuration
        from scopus_db.config_loader import get_config
        config = get_config()
        config.print_database_creation_summary()
        
        # STEP 7: Actually create the database!
        # This is where the magic happens - we have 3 phases:
        
        print("\n🚀 STARTING DATABASE CREATION PROCESS...")
        print("    This might take a few minutes depending on your data size.")
        print()
        
        # PHASE 1: Set up the database structure (tables, indexes, etc.)
        print("📋 PHASE 1: Creating database structure (tables and indexes)...")
        phase1_start = log_stage_start(logger, "DATABASE SCHEMA CREATION", 
                                      f"Creating tables and indexes for {db_creator.db_path}")
        try:
            db_creator.create_optimal_schema()
            log_stage_end(logger, "DATABASE SCHEMA CREATION", phase1_start, True, 
                         "Schema successfully created with all tables and indexes")
            print("   ✅ Database structure created successfully!")
        except Exception as e:
            log_stage_end(logger, "DATABASE SCHEMA CREATION", phase1_start, False, 
                         f"Schema creation failed: {str(e)}")
            logger.error(f"❌ PHASE 1 FAILED: {str(e)}")
            raise
        
        # PHASE 2: Read the CSV data and insert it into the database
        # This is usually the longest part - it reads all your CSV data,
        # filters it for quality, organizes it into the database, and validates population
        print("📊 PHASE 2: Processing CSV data and populating database...")
        print("   (This is the longest step - please be patient!)")
        
        # Get CSV file info for logging
        if hasattr(db_creator, 'csv_files') and db_creator.csv_files:
            csv_info = f"Processing {len(db_creator.csv_files)} CSV files from {input_path}"
        else:
            csv_info = f"Processing single CSV file: {input_path}"
        
        phase2_start = log_stage_start(logger, "CSV DATA PROCESSING & POPULATION", csv_info)
        try:
            db_creator.process_csv_to_optimal_db()
            log_stage_end(logger, "CSV DATA PROCESSING & POPULATION", phase2_start, True,
                         "All CSV data processed, filtered, and database populated with validation")
            print("   ✅ Data processing and validation completed!")
        except Exception as e:
            log_stage_end(logger, "CSV DATA PROCESSING & POPULATION", phase2_start, False, 
                         f"Data processing failed: {str(e)}")
            logger.error(f"❌ PHASE 2 FAILED: {str(e)}")
            raise
        
        # PHASE 3: Clean up and close the database connection
        print("🔒 PHASE 3: Finalizing and closing database...")
        phase3_start = log_stage_start(logger, "DATABASE FINALIZATION", 
                                      "Closing database connection and cleanup")
        try:
            if hasattr(db_creator, 'conn') and db_creator.conn:
                db_creator.conn.close()  # Always close database connections when done!
            log_stage_end(logger, "DATABASE FINALIZATION", phase3_start, True,
                         "Database connection closed successfully")
            print("   ✅ Database finalized and closed!")
        except Exception as e:
            log_stage_end(logger, "DATABASE FINALIZATION", phase3_start, False, 
                         f"Database finalization failed: {str(e)}")
            logger.error(f"❌ PHASE 3 FAILED: {str(e)}")
            raise
        
        # STEP 8: Tell the user where to find their new database and validation report
        total_duration = time.time() - overall_start_time
        logger.info("=" * 80)
        logger.info("🎉 DATABASE CREATION COMPLETED SUCCESSFULLY!")
        logger.info(f"📊 Total processing time: {total_duration:.2f} seconds ({total_duration/60:.1f} minutes)")
        logger.info(f"📁 Database location: {db_creator.db_path}")
        logger.info(f"📝 Quality filter log: {db_creator.data_filter.log_path}")
        logger.info(f"🔍 Validation report: Available in output/ directory")
        logger.info(f"📋 Debug log saved to: {log_path}")
        logger.info("=" * 80)
        
        print("\n🎉 SUCCESS! Your Scopus database has been created and validated!")
        print("=" * 70)
        print(f"📁 Database location: {db_creator.db_path}")
        print(f"📝 Quality filter log: {db_creator.data_filter.log_path}")
        print(f"🔍 Database validation report: Available in output/ directory")
        print(f"📋 Detailed debug log: {log_path}")
        print("=" * 70)
        print("")
        print("📋 DATA QUALITY REPORTS GENERATED:")
        print(f"   🔧 Technical log (JSON): {db_creator.data_filter.log_path}")
        print(f"   📄 Human-readable report: {db_creator.data_filter.log_path.with_suffix('.txt')}")
        print(f"   🌐 Interactive HTML report: {db_creator.data_filter.log_path.with_suffix('.html')}")
        print(f"   📊 Excluded records CSV: {db_creator.data_filter.log_path.with_suffix('.csv')}")
        print("")
        print("💡 WHAT'S NEXT:")
        print("   - Open the HTML report for an easy-to-understand quality summary")
        print("   - Check the CSV file to review excluded records with original data")
        print("   - Use the text report for detailed filtering explanations")
        print("   - You can open the .db file with any SQLite browser")
        print("   - The database is ready for research analysis!")
        print("")
        print("🔍 QUICK TIPS:")
        print("   - Start with the HTML report - it's the most user-friendly")
        print("   - The CSV export contains original data for all excluded records")
        print("   - Review excluded records to verify filtering was correct")
        print("   - Database file size should be smaller than the original CSV")
        print("   - You can use tools like DB Browser for SQLite to explore your data")
        
    except Exception as e:
        # Catch any unexpected errors and log them before exiting
        total_duration = time.time() - overall_start_time
        logger.error("=" * 80)
        logger.error("❌ DATABASE CREATION FAILED!")
        logger.error(f"💥 Error: {str(e)}")
        logger.error(f"⏱️ Runtime before failure: {total_duration:.2f} seconds ({total_duration/60:.1f} minutes)")
        logger.error(f"📋 Full debug log saved to: {log_path}")
        logger.error("=" * 80)
        
        print(f"\n❌ ERROR: Database creation failed!")
        print(f"💥 Details: {str(e)}")
        print(f"📋 Check the debug log for details: {log_path}")
        sys.exit(1)


# This is a Python idiom that means "only run main() if this script is run directly"
# It prevents main() from running if someone imports this file as a module
if __name__ == "__main__":
    main()
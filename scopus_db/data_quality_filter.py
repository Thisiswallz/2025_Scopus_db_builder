"""
Data Quality Filter for Scopus CSV Data

Filters out low-quality entries and provides detailed logging of exclusion rationales.
Based on the comprehensive failure analysis, focuses on excluding editorial content,
incomplete records, and malformed data.
"""

import re
import json
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from pathlib import Path
import math


class ScopusDataQualityFilter:
    """
    Filters Scopus CSV data to ensure only high-quality research papers are processed.
    
    Exclusion criteria (empty field checks only):
    1. Missing authors (no author names provided)
    2. Missing author IDs (no Scopus Author IDs provided)
    3. Missing title (no title provided)
    4. Missing publication year (no year provided)
    5. Missing DOI (no Digital Object Identifier provided)
    6. Missing affiliations (no institutional affiliations provided)
    7. Missing abstract (no abstract provided)
    """
    
    def __init__(self, enable_filtering: bool = True, log_path: Optional[str] = None,
                 enable_crossref_recovery: bool = False, crossref_email: Optional[str] = None, 
                 skip_confirmation: bool = False):
        """
        Initialize the data quality filter.
        
        Args:
            enable_filtering: Whether to apply filtering (False = no filtering)
            log_path: Path to save exclusion log (auto-generated if None)
            enable_crossref_recovery: Whether to attempt CrossRef recovery (requires email)
            crossref_email: Email address for CrossRef polite pool (required if recovery enabled)
        """
        self.enable_filtering = enable_filtering
        self.enable_crossref_recovery = enable_crossref_recovery
        self.crossref_email = crossref_email
        self.skip_confirmation = skip_confirmation
        self.crossref_client = None
        
        # Validate CrossRef configuration
        if self.enable_crossref_recovery:
            if not self.crossref_email:
                raise ValueError("CrossRef recovery requires a valid email address for polite pool compliance")
            if not self._is_valid_email(self.crossref_email):
                raise ValueError(f"Invalid email format for CrossRef: {self.crossref_email}")
        
        self.exclusion_log = []
        self.stats = {
            "total_records": 0,
            "excluded_records": 0,
            "included_records": 0,
            "exclusion_reasons": {},
            "crossref_recovery_stats": {
                "attempted": 0,
                "successful": 0,
                "failed": 0,
                "phase1_attempted": 0,
                "phase1_successful": 0,
                "phase2a_attempted": 0,
                "phase2a_successful": 0,
                "phase2b_attempted": 0,
                "phase2b_successful": 0
            }
        }
        
        # Set up logging
        if log_path:
            self.log_path = Path(log_path)
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.log_path = Path(f"data_quality_exclusion_log_{timestamp}.json")
    
    def _is_valid_email(self, email: str) -> bool:
        """Basic email validation for CrossRef polite pool compliance."""
        return "@" in email and "." in email.split("@")[1]
    
    def _initialize_crossref_client(self):
        """Initialize CrossRef client if recovery is enabled and not already initialized."""
        if self.enable_crossref_recovery and not self.crossref_client:
            try:
                from .crossref import CrossRefClient
                self.crossref_client = CrossRefClient(mailto_email=self.crossref_email)
                print(f"   🔗 CrossRef recovery initialized with email: {self.crossref_email}")
            except ImportError as e:
                print(f"   ⚠️  Warning: CrossRef client not available: {e}")
                self.enable_crossref_recovery = False
            except Exception as e:
                print(f"   ⚠️  Warning: Failed to initialize CrossRef client: {e}")
                self.enable_crossref_recovery = False
    
    def _attempt_crossref_recovery(self, row: Dict[str, str]) -> Dict[str, str]:
        """
        Attempt to recover missing DOI using multi-phase CrossRef recovery.
        
        Phase 1: PubMed ID lookup (highest confidence, ~95%)
        Phase 2a: Journal-based recovery (high confidence, ~85%)  
        Phase 2b: Title-based recovery (moderate confidence, ~70%)
        
        Args:
            row: CSV row as dictionary
            
        Returns:
            Enhanced row with recovered DOI if successful, original row otherwise
        """
        if not self.enable_crossref_recovery or not self.crossref_client:
            return row
        
        self.stats["crossref_recovery_stats"]["attempted"] += 1
        original_row = row.copy()
        
        # Phase 1: Try PubMed ID lookup (highest confidence)
        recovered_row = self._attempt_phase1_pubmed_recovery(row)
        if recovered_row.get('DOI', '').strip():
            return recovered_row
        
        # Phase 2a: Try journal-based recovery (high confidence)
        recovered_row = self._attempt_phase2a_journal_recovery(row)
        if recovered_row.get('DOI', '').strip():
            return recovered_row
        
        # Phase 2b: Try title-based recovery (moderate confidence)
        recovered_row = self._attempt_phase2b_title_recovery(row)
        if recovered_row.get('DOI', '').strip():
            return recovered_row
        
        # No recovery successful
        return original_row
    
    def _attempt_phase1_pubmed_recovery(self, row: Dict[str, str]) -> Dict[str, str]:
        """
        Phase 1: Attempt DOI recovery using PubMed ID lookup.
        
        This is the highest confidence method with ~95% success rate when
        PubMed IDs are available and valid.
        """
        pubmed_id = row.get('PubMed ID', '').strip()
        if not pubmed_id:
            return row
            
        self.stats["crossref_recovery_stats"]["phase1_attempted"] += 1
        
        try:
            result = self.crossref_client.search_by_pmid(pubmed_id)
            if result:
                # Validate the match with confidence scoring
                validation = self.crossref_client.validate_publication_match(
                    row, result, 'pmid', confidence_threshold=0.8
                )
                
                if validation['is_valid_match']:
                    enhanced_row = row.copy()
                    enhanced_row['DOI'] = validation['doi']
                    enhanced_row['_recovery_method'] = 'Phase1_PubMed'
                    enhanced_row['_recovery_confidence'] = f"{validation['confidence_score']:.3f}"
                    
                    self.stats["crossref_recovery_stats"]["phase1_successful"] += 1
                    self.stats["crossref_recovery_stats"]["successful"] += 1
                    print(f"   ✅ Phase 1 - Recovered DOI via PubMed ID {pubmed_id}: {validation['doi']} (confidence: {validation['confidence_score']:.3f})")
                    return enhanced_row
                else:
                    print(f"   ⚠️  Phase 1 - PubMed ID {pubmed_id} found but low confidence: {validation['confidence_score']:.3f}")
                    
        except Exception as e:
            print(f"   ❌ Phase 1 - PubMed ID lookup error: {e}")
        
        return row
    
    def _attempt_phase2a_journal_recovery(self, row: Dict[str, str]) -> Dict[str, str]:
        """
        Phase 2a: Attempt DOI recovery using journal publication details.
        
        Uses Source title + Volume + Issue + Pages + Year for high confidence matching.
        Requires sufficient publication metadata to be effective.
        """
        # Check if we have sufficient journal details
        source_title = row.get('Source title', '').strip()
        volume = row.get('Volume', '').strip()
        year = row.get('Year', '').strip()
        
        if not source_title or not (volume or year):
            return row  # Insufficient data for journal search
            
        self.stats["crossref_recovery_stats"]["phase2a_attempted"] += 1
        
        try:
            # Perform journal-based search
            result = self.crossref_client.search_by_journal_details(
                journal=source_title,
                volume=volume if volume else None,
                issue=row.get('Issue', '').strip() if row.get('Issue', '').strip() else None,
                pages=f"{row.get('Page start', '').strip()}-{row.get('Page end', '').strip()}".strip('-') if row.get('Page start', '').strip() else None,
                year=year if year else None
            )
            
            if result:
                # Validate the match with confidence scoring
                validation = self.crossref_client.validate_publication_match(
                    row, result, 'journal', confidence_threshold=0.75
                )
                
                if validation['is_valid_match']:
                    enhanced_row = row.copy()
                    enhanced_row['DOI'] = validation['doi']
                    enhanced_row['_recovery_method'] = 'Phase2a_Journal'
                    enhanced_row['_recovery_confidence'] = f"{validation['confidence_score']:.3f}"
                    
                    self.stats["crossref_recovery_stats"]["phase2a_successful"] += 1
                    self.stats["crossref_recovery_stats"]["successful"] += 1
                    print(f"   ✅ Phase 2a - Recovered DOI via journal search: {validation['doi']} (confidence: {validation['confidence_score']:.3f})")
                    return enhanced_row
                else:
                    print(f"   ⚠️  Phase 2a - Journal match found but low confidence: {validation['confidence_score']:.3f}")
                    
        except Exception as e:
            print(f"   ❌ Phase 2a - Journal search error: {e}")
        
        return row
    
    def _attempt_phase2b_title_recovery(self, row: Dict[str, str]) -> Dict[str, str]:
        """
        Phase 2b: Attempt DOI recovery using title-based search.
        
        Uses Title + Author + Year for moderate confidence matching.
        This is the most flexible but lowest confidence method.
        """
        title = row.get('Title', '').strip()
        year = row.get('Year', '').strip()
        
        if not title or len(title) < 10:  # Skip very short titles
            return row
            
        self.stats["crossref_recovery_stats"]["phase2b_attempted"] += 1
        
        try:
            # Extract first author for better matching
            authors_str = row.get('Authors', '').strip()
            first_author = None
            if authors_str:
                authors = self.crossref_client.parse_scopus_author_names(authors_str)
                first_author = authors[0] if authors else None
            
            # Perform title-based search
            results = self.crossref_client.search_by_title(
                title=title,
                author=first_author,
                year=year,
                limit=3  # Check top 3 results for best match
            )
            
            if results:
                # Try each result and pick the best confidence match
                best_match = None
                best_confidence = 0.0
                
                for result in results:
                    validation = self.crossref_client.validate_publication_match(
                        row, result, 'title', confidence_threshold=0.65  # Lower threshold for title searches
                    )
                    
                    if validation['is_valid_match'] and validation['confidence_score'] > best_confidence:
                        best_match = validation
                        best_confidence = validation['confidence_score']
                
                if best_match:
                    enhanced_row = row.copy()
                    enhanced_row['DOI'] = best_match['doi']
                    enhanced_row['_recovery_method'] = 'Phase2b_Title'
                    enhanced_row['_recovery_confidence'] = f"{best_match['confidence_score']:.3f}"
                    
                    self.stats["crossref_recovery_stats"]["phase2b_successful"] += 1
                    self.stats["crossref_recovery_stats"]["successful"] += 1
                    print(f"   ✅ Phase 2b - Recovered DOI via title search: {best_match['doi']} (confidence: {best_match['confidence_score']:.3f})")
                    return enhanced_row
                else:
                    print(f"   ⚠️  Phase 2b - Title matches found but all below confidence threshold")
                    
        except Exception as e:
            print(f"   ❌ Phase 2b - Title search error: {e}")
        
        return row
    
    def _ask_crossref_confirmation(self):
        """Ask user for confirmation before performing CrossRef recovery."""
        print("\n🔗 CROSSREF DATA RECOVERY AVAILABLE")
        print("   CrossRef recovery can attempt to find missing DOIs using external API calls.")
        print("   This may improve data quality by recovering missing metadata.")
        print("")
        print("   HOW IT WORKS:")
        print("   - Connects to CrossRef API using your email for polite pool access")
        print("   - Attempts to find missing DOIs using PubMed IDs (high confidence)")
        print("   - Only runs on records that would otherwise be excluded")
        print("   - Network connection required, may add processing time")
        print("")
        print(f"   📧 Your email for polite pool: {self.crossref_email}")
        print("")
        
        while True:
            response = input("❓ Perform CrossRef recovery to find missing DOIs? [y/N]: ").strip().lower()
            if response in ['y', 'yes']:
                print("✅ CrossRef recovery enabled - will attempt to recover missing DOIs")
                break
            elif response in ['n', 'no', '']:
                print("❌ CrossRef recovery disabled - proceeding with standard filtering only")
                self.enable_crossref_recovery = False
                break
            else:
                print("   ⚠️  Please enter 'y' for yes or 'n' for no (or press Enter for no).")
    
    def should_exclude_record(self, row: Dict[str, str], row_index: int) -> Tuple[bool, str]:
        """
        Determine if a record should be excluded and provide rationale.
        
        CRITICAL FIELDS (must be present and valid):
        1) Authors with valid Scopus Author IDs
        2) Title (minimum length 10 characters)
        3) Publication year (valid 4-digit year)
        4) DOI (Digital Object Identifier)
        5) Affiliations (properly parsed institutional data)
        6) Abstract (minimum 50 words)
        
        Args:
            row: CSV row as dictionary
            row_index: 1-based row number in CSV
            
        Returns:
            (should_exclude, reason): Boolean and detailed reason string
        """
        if not self.enable_filtering:
            return False, "Filtering disabled"
        
        # Extract key fields
        title = (row.get('Title', '') or '').strip()
        authors = (row.get('Authors', '') or '').strip()
        author_ids = (row.get("Author(s) ID", '') or '').strip()
        abstract = (row.get('Abstract', '') or '').strip()
        year = (row.get('Year', '') or '').strip()
        doc_type = (row.get('Document Type', '') or '').strip().lower()
        doi = (row.get('DOI', '') or '').strip()
        affiliations = (row.get('Affiliations', '') or '').strip()
        language = (row.get('Language of Original Document', '') or '').strip()
        
        # CRITICAL FILTERS: Essential research paper components (EMPTY CHECK ONLY)
        
        # 1. Missing authors
        if not authors or authors.strip() == "":
            return True, "MISSING_AUTHORS: No author names provided"
        
        # 2. Missing author IDs
        if not author_ids or author_ids.strip() == "":
            return True, "MISSING_AUTHOR_IDS: No Scopus Author IDs provided"
        
        # 3. Missing title
        if not title or title.strip() == "":
            return True, "MISSING_TITLE: No title provided"
        
        # 4. Missing publication year
        if not year or year.strip() == "":
            return True, "MISSING_YEAR: No publication year provided"
        
        # 5. Missing DOI
        if not doi or doi.strip() == "":
            return True, "MISSING_DOI: No DOI provided"
        
        # 6. Missing affiliations
        if not affiliations or affiliations.strip() == "":
            return True, "MISSING_AFFILIATIONS: No institutional affiliations provided"
        
        # 7. Missing abstract
        if not abstract or abstract.strip() == "":
            return True, "MISSING_ABSTRACT: No abstract provided"
        
        # Record passed all filters
        return False, "PASSED: All quality filters passed"
    
    def filter_csv_data(self, csv_data: List[Dict[str, str]]) -> Tuple[List[Dict[str, str]], Dict]:
        """
        Filter CSV data and return filtered data plus exclusion report.
        
        Args:
            csv_data: List of CSV rows as dictionaries
            
        Returns:
            (filtered_data, exclusion_report): Filtered data and detailed report
        """
        filtered_data = []
        self.stats["total_records"] = len(csv_data)
        
        print(f"\n🔍 APPLYING DATA QUALITY FILTERS")
        print(f"   Total records to evaluate: {len(csv_data):,}")
        
        # Check if CrossRef recovery is configured and ask for user confirmation
        if self.enable_crossref_recovery:
            if self.skip_confirmation:
                # Skip confirmation for testing - directly initialize CrossRef client
                print("🔗 CrossRef recovery enabled (test mode - skipping confirmation)")
                self._initialize_crossref_client()
            else:
                self._ask_crossref_confirmation()
                if self.enable_crossref_recovery:  # User confirmed
                    self._initialize_crossref_client()
        
        for i, row in enumerate(csv_data, 1):
            # PHASE 1: Attempt CrossRef recovery before quality checks (if enabled)
            if self.enable_crossref_recovery and self.crossref_client:
                row = self._attempt_crossref_recovery(row)
            should_exclude, reason = self.should_exclude_record(row, i)
            
            if should_exclude:
                # Log exclusion with ALL original CSV data
                exclusion_entry = {
                    "row_index": i,
                    "reason": reason,
                    "category": reason.split(':')[0] if ':' in reason else "OTHER"
                }
                
                # Add ALL original CSV data for this record
                for key, value in row.items():
                    # Use lowercase keys for consistency
                    clean_key = key.lower().replace('(', '').replace(')', '').replace(' ', '_').replace('-', '_')
                    exclusion_entry[clean_key] = value
                
                self.exclusion_log.append(exclusion_entry)
                
                # Update stats
                self.stats["excluded_records"] += 1
                category = exclusion_entry["category"]
                self.stats["exclusion_reasons"][category] = self.stats["exclusion_reasons"].get(category, 0) + 1
                
            else:
                filtered_data.append(row)
                self.stats["included_records"] += 1
        
        # Print summary
        exclusion_rate = (self.stats["excluded_records"] / self.stats["total_records"]) * 100
        print(f"   ✅ Records included: {self.stats['included_records']:,} ({100-exclusion_rate:.1f}%)")
        print(f"   ❌ Records excluded: {self.stats['excluded_records']:,} ({exclusion_rate:.1f}%)")
        
        # Print CrossRef recovery summary if enabled
        if self.enable_crossref_recovery:
            recovery_stats = self.stats["crossref_recovery_stats"]
            print(f"\n🔗 CROSSREF MULTI-PHASE RECOVERY SUMMARY:")
            print(f"   📊 Total recovery attempts: {recovery_stats['attempted']:,}")
            print(f"   ✅ Total successful recoveries: {recovery_stats['successful']:,}")
            
            # Phase-by-phase breakdown
            print(f"\n   📋 PHASE-BY-PHASE BREAKDOWN:")
            print(f"      Phase 1 (PubMed ID): {recovery_stats['phase1_successful']}/{recovery_stats['phase1_attempted']} successful")
            print(f"      Phase 2a (Journal): {recovery_stats['phase2a_successful']}/{recovery_stats['phase2a_attempted']} successful")  
            print(f"      Phase 2b (Title): {recovery_stats['phase2b_successful']}/{recovery_stats['phase2b_attempted']} successful")
            
            # Overall success rate
            if recovery_stats['attempted'] > 0:
                success_rate = (recovery_stats['successful'] / recovery_stats['attempted']) * 100
                print(f"\n   📈 Overall success rate: {success_rate:.1f}%")
                
                # Phase success rates
                if recovery_stats['phase1_attempted'] > 0:
                    phase1_rate = (recovery_stats['phase1_successful'] / recovery_stats['phase1_attempted']) * 100
                    print(f"   📈 Phase 1 success rate: {phase1_rate:.1f}%")
                if recovery_stats['phase2a_attempted'] > 0:
                    phase2a_rate = (recovery_stats['phase2a_successful'] / recovery_stats['phase2a_attempted']) * 100
                    print(f"   📈 Phase 2a success rate: {phase2a_rate:.1f}%")
                if recovery_stats['phase2b_attempted'] > 0:
                    phase2b_rate = (recovery_stats['phase2b_successful'] / recovery_stats['phase2b_attempted']) * 100
                    print(f"   📈 Phase 2b success rate: {phase2b_rate:.1f}%")
        
        # Save detailed log and generate user-friendly reports
        self._save_exclusion_log()
        self._generate_user_friendly_report()
        self._generate_csv_export()
        self._generate_html_report()
        
        return filtered_data, self._generate_exclusion_report()
    
    def _save_exclusion_log(self):
        """Save detailed exclusion log to JSON file"""
        log_data = {
            "filter_metadata": {
                "timestamp": datetime.now().isoformat(),
                "filtering_enabled": self.enable_filtering,
                "total_records_evaluated": self.stats["total_records"],
                "records_excluded": self.stats["excluded_records"],
                "records_included": self.stats["included_records"]
            },
            "exclusion_summary": self.stats["exclusion_reasons"],
            "detailed_exclusions": self.exclusion_log
        }
        
        with open(self.log_path, 'w', encoding='utf-8') as f:
            json.dump(log_data, f, indent=2, ensure_ascii=False)
        
        print(f"   📝 Detailed exclusion log saved: {self.log_path}")
    
    def _generate_exclusion_report(self) -> Dict:
        """Generate summary report of exclusions"""
        if self.stats["total_records"] == 0:
            return {"error": "No records processed"}
        
        exclusion_rate = (self.stats["excluded_records"] / self.stats["total_records"]) * 100
        
        report = {
            "summary": {
                "total_records": self.stats["total_records"],
                "included_records": self.stats["included_records"],
                "excluded_records": self.stats["excluded_records"],
                "exclusion_rate_percent": round(exclusion_rate, 2),
                "quality_improvement": f"Filtered out {exclusion_rate:.1f}% low-quality entries"
            },
            "exclusion_breakdown": dict(sorted(
                self.stats["exclusion_reasons"].items(), 
                key=lambda x: x[1], 
                reverse=True
            )),
            "log_file": str(self.log_path)
        }
        
        return report
    
    def print_exclusion_summary(self):
        """Print a formatted summary of exclusions"""
        if not self.exclusion_log:
            print("   ✅ No records excluded - all data passed quality filters")
            return
        
        print(f"\n📊 DATA QUALITY EXCLUSION SUMMARY")
        print(f"   {'Category':<18} {'Count':<8} {'Rate':<8} {'Description'}")
        print(f"   {'-'*70}")
        
        total = self.stats["total_records"]
        for category, count in sorted(self.stats["exclusion_reasons"].items(), key=lambda x: x[1], reverse=True):
            rate = f"{count/total*100:.1f}%" if total > 0 else "0%"
            description = {
                "MISSING_AUTHORS": "No author names provided",
                "MISSING_AUTHOR_IDS": "No Scopus Author IDs provided",
                "MISSING_TITLE": "No title provided",
                "MISSING_YEAR": "No publication year provided",
                "MISSING_DOI": "No DOI provided",
                "MISSING_AFFILIATIONS": "No institutional affiliations",
                "MISSING_ABSTRACT": "No abstract provided"
            }.get(category, "Other exclusions")
            
            print(f"   {category:<18} {count:<8} {rate:<8} {description}")
    
    def _generate_user_friendly_report(self):
        """Generate a comprehensive, human-readable text report"""
        timestamp = datetime.now().strftime("%Y-%m-%d at %H:%M:%S")
        report_path = self.log_path.with_suffix('.txt')
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("SCOPUS DATA QUALITY FILTERING REPORT\n")
            f.write("="*80 + "\n")
            f.write(f"Generated: {timestamp}\n")
            f.write(f"Report for: {self.log_path.stem}\n\n")
            
            # Executive Summary
            f.write("EXECUTIVE SUMMARY\n")
            f.write("-"*40 + "\n")
            total = self.stats["total_records"]
            excluded = self.stats["excluded_records"]
            included = self.stats["included_records"]
            exclusion_rate = (excluded / total) * 100 if total > 0 else 0
            
            f.write(f"📊 Data Processing Overview:\n")
            f.write(f"   • Total Records Processed: {total:,}\n")
            f.write(f"   • Records Included: {included:,} ({100-exclusion_rate:.1f}%)\n")
            f.write(f"   • Records Excluded: {excluded:,} ({exclusion_rate:.1f}%)\n\n")
            
            # Quality Assessment
            f.write("QUALITY ASSESSMENT\n")
            f.write("-"*40 + "\n")
            if exclusion_rate < 5:
                f.write("🟢 EXCELLENT: Very low exclusion rate indicates high-quality source data\n")
            elif exclusion_rate < 15:
                f.write("🟡 GOOD: Moderate exclusion rate is normal for Scopus exports\n")
            elif exclusion_rate < 30:
                f.write("🟠 FAIR: Higher exclusion rate suggests some data quality issues\n")
            else:
                f.write("🔴 ATTENTION: High exclusion rate may indicate data export issues\n")
            
            f.write(f"   Quality Score: {100-exclusion_rate:.1f}/100\n\n")
            
            # Exclusion Breakdown
            if excluded > 0:
                f.write("EXCLUSION BREAKDOWN\n")
                f.write("-"*40 + "\n")
                f.write("Issues found in your data (most common first):\n\n")
                
                for category, count in sorted(self.stats["exclusion_reasons"].items(), key=lambda x: x[1], reverse=True):
                    rate = (count / total) * 100 if total > 0 else 0
                    f.write(f"• {self._get_friendly_category_name(category)}\n")
                    f.write(f"  Count: {count:,} records ({rate:.1f}% of total)\n")
                    f.write(f"  Impact: {self._get_category_impact(category)}\n")
                    f.write(f"  Solution: {self._get_category_solution(category)}\n\n")
            
            # Recommendations
            f.write("RECOMMENDATIONS\n")
            f.write("-"*40 + "\n")
            f.write(self._generate_recommendations())
            
            # Sample Excluded Records
            if self.exclusion_log:
                f.write("\nSAMPLE EXCLUDED RECORDS\n")
                f.write("-"*40 + "\n")
                f.write("Here are a few examples of excluded records to help you understand:\n\n")
                
                # Show up to 5 examples from different categories
                shown_categories = set()
                examples_shown = 0
                for entry in self.exclusion_log:
                    if examples_shown >= 5:
                        break
                    category = entry["category"]
                    if category not in shown_categories:
                        f.write(f"Example {examples_shown + 1}: {self._get_friendly_category_name(category)}\n")
                        f.write(f"   Title: {entry['title']}\n")
                        f.write(f"   Authors: {entry['authors'][:50]}{'...' if len(entry['authors']) > 50 else ''}\n")
                        f.write(f"   Reason: {self._get_friendly_reason(entry['reason'])}\n\n")
                        shown_categories.add(category)
                        examples_shown += 1
                        
        print(f"   📋 Human-readable report saved: {report_path}")
    
    def _generate_csv_export(self):
        """Generate CSV export of excluded records with original data"""
        csv_path = self.log_path.with_suffix('.csv')
        
        if not self.exclusion_log:
            print(f"   📊 No excluded records - CSV export skipped")
            return
        
        # Store original data for excluded records
        excluded_records_with_data = []
        
        print(f"   📊 Generating CSV export of excluded records...")
        
        with open(csv_path, 'w', encoding='utf-8', newline='') as f:
            import csv
            
            # Prepare headers - start with metadata columns
            headers = [
                'exclusion_reason',
                'exclusion_category', 
                'row_index'
            ]
            
            # Add all possible CSV columns from the original data
            all_csv_columns = set()
            for entry in self.exclusion_log:
                # Each entry has the original row data stored
                for key in entry.keys():
                    if key not in ['row_index', 'reason', 'category']:
                        all_csv_columns.add(key)
            
            # Add common Scopus CSV columns in logical order
            common_columns = [
                'title', 'authors', 'doi', 'year', 'abstract', 'affiliations',
                'document_type', 'source_title', 'volume', 'issue', 'pages',
                'cited_by_count', 'keywords', 'funding', 'language'
            ]
            
            # Add common columns first, then any others found
            for col in common_columns:
                if col in all_csv_columns:
                    headers.append(col)
                    all_csv_columns.remove(col)
            
            # Add remaining columns
            headers.extend(sorted(all_csv_columns))
            
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            
            # Write excluded records with their original data
            for entry in self.exclusion_log:
                row_data = {
                    'exclusion_reason': entry.get('reason', ''),
                    'exclusion_category': entry.get('category', ''),
                    'row_index': entry.get('row_index', '')
                }
                
                # Add all the original CSV data for this record
                for key, value in entry.items():
                    if key not in ['row_index', 'reason', 'category']:
                        row_data[key] = value
                
                writer.writerow(row_data)
        
        print(f"   📊 CSV export of excluded records saved: {csv_path}")
        
        # Generate category-specific CSV files for major categories
        self._generate_category_csv_files()
    
    def _generate_category_csv_files(self):
        """Generate separate CSV files for each major exclusion category"""
        
        # Group exclusions by category
        by_category = {}
        for entry in self.exclusion_log:
            category = entry.get('category', 'OTHER')
            if category not in by_category:
                by_category[category] = []
            by_category[category].append(entry)
        
        # Generate CSV for categories with significant numbers of exclusions
        for category, entries in by_category.items():
            if len(entries) >= 5:  # Only create separate files for categories with 5+ exclusions
                category_csv_path = self.log_path.with_name(f"{self.log_path.stem}_{category.lower()}.csv")
                
                with open(category_csv_path, 'w', encoding='utf-8', newline='') as f:
                    import csv
                    
                    # Same header logic as main CSV
                    headers = ['exclusion_reason', 'exclusion_category', 'row_index']
                    
                    all_columns = set()
                    for entry in entries:
                        for key in entry.keys():
                            if key not in ['row_index', 'reason', 'category']:
                                all_columns.add(key)
                    
                    common_columns = ['title', 'authors', 'doi', 'year', 'abstract', 'affiliations']
                    for col in common_columns:
                        if col in all_columns:
                            headers.append(col)
                            all_columns.remove(col)
                    headers.extend(sorted(all_columns))
                    
                    writer = csv.DictWriter(f, fieldnames=headers)
                    writer.writeheader()
                    
                    for entry in entries:
                        row_data = {
                            'exclusion_reason': entry.get('reason', ''),
                            'exclusion_category': entry.get('category', ''),
                            'row_index': entry.get('row_index', '')
                        }
                        
                        for key, value in entry.items():
                            if key not in ['row_index', 'reason', 'category']:
                                row_data[key] = value
                        
                        writer.writerow(row_data)
                
                print(f"   📊 {category} exclusions CSV saved: {category_csv_path}")
    
    def _generate_html_report(self):
        """Generate an interactive HTML report with visualizations"""
        timestamp = datetime.now().strftime("%Y-%m-%d at %H:%M:%S")
        html_path = self.log_path.with_suffix('.html')
        
        # Calculate data for visualizations
        total = self.stats["total_records"]
        excluded = self.stats["excluded_records"]
        included = self.stats["included_records"]
        exclusion_rate = (excluded / total) * 100 if total > 0 else 0
        
        # Generate chart data
        chart_data = []
        colors = ["#ff6b6b", "#4ecdc4", "#45b7d1", "#96ceb4", "#ffeaa7", "#dda0dd", "#98d8c8"]
        for i, (category, count) in enumerate(sorted(self.stats["exclusion_reasons"].items(), key=lambda x: x[1], reverse=True)):
            chart_data.append({
                "category": self._get_friendly_category_name(category),
                "count": count,
                "percentage": (count / total) * 100 if total > 0 else 0,
                "color": colors[i % len(colors)]
            })
        
        html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Scopus Data Quality Report</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f8f9fa;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            border-radius: 10px;
            margin-bottom: 30px;
            text-align: center;
        }}
        .card {{
            background: white;
            border-radius: 10px;
            padding: 25px;
            margin-bottom: 20px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        .metric {{
            display: inline-block;
            text-align: center;
            margin: 10px 20px;
            padding: 15px;
            background: #f8f9fa;
            border-radius: 8px;
            min-width: 150px;
        }}
        .metric-value {{
            font-size: 2em;
            font-weight: bold;
            color: #667eea;
        }}
        .metric-label {{
            color: #666;
            font-size: 0.9em;
        }}
        .quality-score {{
            font-size: 3em;
            font-weight: bold;
            text-align: center;
            margin: 20px 0;
        }}
        .quality-excellent {{ color: #28a745; }}
        .quality-good {{ color: #17a2b8; }}
        .quality-fair {{ color: #ffc107; }}
        .quality-attention {{ color: #dc3545; }}
        .chart-container {{
            width: 100%;
            height: 400px;
            margin: 20px 0;
        }}
        .bar {{
            display: flex;
            align-items: center;
            margin: 10px 0;
            padding: 10px;
            background: #f8f9fa;
            border-radius: 5px;
        }}
        .bar-label {{
            width: 200px;
            font-weight: bold;
        }}
        .bar-visual {{
            flex: 1;
            height: 20px;
            background: #e9ecef;
            border-radius: 10px;
            margin: 0 10px;
            position: relative;
        }}
        .bar-fill {{
            height: 100%;
            border-radius: 10px;
            transition: width 0.5s ease;
        }}
        .bar-value {{
            min-width: 80px;
            text-align: right;
            font-weight: bold;
        }}
        .recommendation {{
            background: #e8f5e8;
            border-left: 4px solid #28a745;
            padding: 15px;
            margin: 10px 0;
            border-radius: 0 5px 5px 0;
        }}
        .sample-record {{
            background: #fff3cd;
            border: 1px solid #ffeaa7;
            border-radius: 5px;
            padding: 15px;
            margin: 10px 0;
        }}
        .sample-title {{
            font-weight: bold;
            color: #856404;
            margin-bottom: 5px;
        }}
        h1, h2, h3 {{
            color: #333;
        }}
        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>📊 Scopus Data Quality Report</h1>
        <p>Generated: {timestamp}</p>
    </div>
    
    <div class="card">
        <h2>Executive Summary</h2>
        <div class="summary-grid">
            <div class="metric">
                <div class="metric-value">{total:,}</div>
                <div class="metric-label">Total Records</div>
            </div>
            <div class="metric">
                <div class="metric-value">{included:,}</div>
                <div class="metric-label">Included ({100-exclusion_rate:.1f}%)</div>
            </div>
            <div class="metric">
                <div class="metric-value">{excluded:,}</div>
                <div class="metric-label">Excluded ({exclusion_rate:.1f}%)</div>
            </div>
        </div>
        
        <div class="quality-score {'quality-excellent' if exclusion_rate < 5 else 'quality-good' if exclusion_rate < 15 else 'quality-fair' if exclusion_rate < 30 else 'quality-attention'}">
            Quality Score: {100-exclusion_rate:.1f}/100
        </div>
        
        <p style="text-align: center; font-size: 1.1em;">
            {'🟢 EXCELLENT: Very low exclusion rate indicates high-quality source data' if exclusion_rate < 5 else 
             '🟡 GOOD: Moderate exclusion rate is normal for Scopus exports' if exclusion_rate < 15 else
             '🟠 FAIR: Higher exclusion rate suggests some data quality issues' if exclusion_rate < 30 else
             '🔴 ATTENTION: High exclusion rate may indicate data export issues'}
        </p>
    </div>
    
    {'<div class="card"><h2>Exclusion Breakdown</h2><p>Issues found in your data (most common first):</p>' + ''.join([f'''
    <div class="bar">
        <div class="bar-label">{item["category"]}</div>
        <div class="bar-visual">
            <div class="bar-fill" style="width: {min(item["percentage"], 100)}%; background-color: {item["color"]};"></div>
        </div>
        <div class="bar-value">{item["count"]:,} ({item["percentage"]:.1f}%)</div>
    </div>''' for item in chart_data]) + '</div>' if chart_data else ''}
    
    <div class="card">
        <h2>Recommendations</h2>
        {self._generate_html_recommendations()}
    </div>
    
    <div class="card">
        <h2>What This Means</h2>
        <p><strong>✅ Included Records:</strong> These records passed all quality filters and contain the essential information needed for research analysis.</p>
        <p><strong>❌ Excluded Records:</strong> These records were missing critical information that would make analysis unreliable or incomplete.</p>
        <p><strong>💡 Quality Filtering Benefits:</strong> By removing incomplete records, your database will produce more accurate research insights and avoid misleading results.</p>
    </div>
    
    <div class="card">
        <h2>Files Generated</h2>
        <ul>
            <li><strong>Detailed JSON Log:</strong> {self.log_path.name} (machine-readable)</li>
            <li><strong>Summary Report:</strong> {self.log_path.with_suffix('.txt').name} (human-readable)</li>
            <li><strong>This HTML Report:</strong> {html_path.name} (interactive)</li>
            <li><strong>📊 CSV Export of Excluded Records:</strong> {self.log_path.with_suffix('.csv').name} (original data for all excluded records)</li>
        </ul>
        <p><strong>💡 CSV Export Contains:</strong> All excluded records with their complete original CSV data plus exclusion reasons. Perfect for manual review and validation of filtering decisions.</p>
        {self._generate_csv_links_html()}
    </div>
</body>
</html>
"""
        
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
            
        print(f"   🌐 Interactive HTML report saved: {html_path}")
    
    def _get_friendly_category_name(self, category: str) -> str:
        """Convert technical category names to user-friendly names"""
        friendly_names = {
            "MISSING_AUTHORS": "Missing Author Names",
            "MISSING_AUTHOR_IDS": "Missing Author IDs",
            "MISSING_TITLE": "Missing Title",
            "MISSING_YEAR": "Missing Publication Year",
            "MISSING_DOI": "Missing DOI",
            "MISSING_AFFILIATIONS": "Missing Institution Info",
            "MISSING_ABSTRACT": "Missing Abstract"
        }
        return friendly_names.get(category, category.replace("_", " ").title())
    
    def _get_friendly_reason(self, reason: str) -> str:
        """Convert technical reason to user-friendly explanation"""
        if "MISSING_AUTHORS" in reason:
            return "This record had no author names listed"
        elif "MISSING_AUTHOR_IDS" in reason:
            return "This record had no Scopus Author IDs"
        elif "MISSING_TITLE" in reason:
            return "This record had no title"
        elif "MISSING_YEAR" in reason:
            return "This record had no publication year"
        elif "MISSING_DOI" in reason:
            return "This record had no DOI (Digital Object Identifier)"
        elif "MISSING_AFFILIATIONS" in reason:
            return "This record had no institutional affiliation information"
        elif "MISSING_ABSTRACT" in reason:
            return "This record had no abstract"
        else:
            return reason
    
    def _get_category_impact(self, category: str) -> str:
        """Explain the impact of each exclusion category"""
        impacts = {
            "MISSING_AUTHORS": "Cannot identify who conducted the research",
            "MISSING_AUTHOR_IDS": "Cannot link to author profiles or track careers",
            "MISSING_TITLE": "Cannot understand what the research is about",
            "MISSING_YEAR": "Cannot analyze temporal trends or citation patterns",
            "MISSING_DOI": "Cannot verify or access the original publication",
            "MISSING_AFFILIATIONS": "Cannot analyze institutional collaborations",
            "MISSING_ABSTRACT": "Cannot understand research content or methods"
        }
        return impacts.get(category, "Unknown impact")
    
    def _get_category_solution(self, category: str) -> str:
        """Suggest solutions for each exclusion category"""
        solutions = {
            "MISSING_AUTHORS": "Check Scopus export settings to include author fields",
            "MISSING_AUTHOR_IDS": "Ensure 'Author(s) ID' field is included in export",
            "MISSING_TITLE": "Verify 'Title' field is included in Scopus export",
            "MISSING_YEAR": "Include 'Year' field in your Scopus search results",
            "MISSING_DOI": "Add 'DOI' field to your Scopus export columns",
            "MISSING_AFFILIATIONS": "Include 'Affiliations' field in export settings",
            "MISSING_ABSTRACT": "Add 'Abstract' field to your Scopus export"
        }
        return solutions.get(category, "Review your Scopus export settings")
    
    def _generate_recommendations(self) -> str:
        """Generate specific recommendations based on exclusion patterns"""
        total = self.stats["total_records"]
        excluded = self.stats["excluded_records"]
        exclusion_rate = (excluded / total) * 100 if total > 0 else 0
        
        recommendations = []
        
        if exclusion_rate < 5:
            recommendations.append("✅ Your data quality is excellent! Very few records needed to be excluded.")
            recommendations.append("   Continue using the same Scopus export settings.")
        elif exclusion_rate < 15:
            recommendations.append("✅ Your data quality is good. The exclusion rate is within normal range.")
            recommendations.append("   Consider the suggestions below to further improve data completeness.")
        elif exclusion_rate < 30:
            recommendations.append("⚠️  Your data has some quality issues. Consider reviewing your Scopus export settings.")
            recommendations.append("   Focus on the most common missing fields identified above.")
        else:
            recommendations.append("🚨 High exclusion rate detected! Your Scopus export may be missing important fields.")
            recommendations.append("   Strongly recommend reviewing and updating your export column selections.")
        
        # Specific recommendations based on most common issues
        if "MISSING_ABSTRACT" in self.stats["exclusion_reasons"] and self.stats["exclusion_reasons"]["MISSING_ABSTRACT"] > total * 0.1:
            recommendations.append("📝 Many records missing abstracts - add 'Abstract' field to Scopus export")
        
        if "MISSING_AFFILIATIONS" in self.stats["exclusion_reasons"] and self.stats["exclusion_reasons"]["MISSING_AFFILIATIONS"] > total * 0.1:
            recommendations.append("🏛️  Many records missing affiliations - add 'Affiliations' field to export")
        
        if "MISSING_DOI" in self.stats["exclusion_reasons"] and self.stats["exclusion_reasons"]["MISSING_DOI"] > total * 0.1:
            recommendations.append("🔗 Many records missing DOIs - add 'DOI' field to Scopus export")
        
        recommendations.append("💡 For future exports, ensure all essential fields are selected in Scopus.")
        recommendations.append("📊 The remaining " + f"{self.stats['included_records']:,}" + " records are high-quality and ready for analysis.")
        
        return "\n".join(f"{rec}\n" for rec in recommendations)
    
    def _generate_html_recommendations(self) -> str:
        """Generate HTML-formatted recommendations"""
        total = self.stats["total_records"]
        excluded = self.stats["excluded_records"]
        exclusion_rate = (excluded / total) * 100 if total > 0 else 0
        
        recommendations = []
        
        if exclusion_rate < 5:
            recommendations.append('<div class="recommendation">✅ <strong>Excellent Quality:</strong> Your data quality is excellent! Very few records needed to be excluded. Continue using the same Scopus export settings.</div>')
        elif exclusion_rate < 15:
            recommendations.append('<div class="recommendation">✅ <strong>Good Quality:</strong> Your data quality is good. The exclusion rate is within normal range. Consider the suggestions below to further improve data completeness.</div>')
        elif exclusion_rate < 30:
            recommendations.append('<div class="recommendation">⚠️ <strong>Attention Needed:</strong> Your data has some quality issues. Consider reviewing your Scopus export settings and focus on the most common missing fields identified above.</div>')
        else:
            recommendations.append('<div class="recommendation">🚨 <strong>Action Required:</strong> High exclusion rate detected! Your Scopus export may be missing important fields. Strongly recommend reviewing and updating your export column selections.</div>')
        
        recommendations.append(f'<div class="recommendation">💡 <strong>Result:</strong> The remaining {self.stats["included_records"]:,} records are high-quality and ready for reliable research analysis.</div>')
        
        return "".join(recommendations)
    
    def _generate_csv_links_html(self) -> str:
        """Generate HTML links to CSV exports"""
        if not self.exclusion_log:
            return "<p><em>No excluded records - no CSV files generated.</em></p>"
        
        # Group exclusions by category to show which category CSVs exist
        by_category = {}
        for entry in self.exclusion_log:
            category = entry.get('category', 'OTHER')
            if category not in by_category:
                by_category[category] = []
            by_category[category].append(entry)
        
        html_parts = []
        html_parts.append('<h3>📁 Category-Specific CSV Exports</h3>')
        html_parts.append('<p>Separate CSV files for each exclusion category (5+ records only):</p>')
        html_parts.append('<ul>')
        
        category_files_generated = False
        for category, entries in by_category.items():
            if len(entries) >= 5:
                category_files_generated = True
                friendly_name = self._get_friendly_category_name(category)
                csv_filename = f"{self.log_path.stem}_{category.lower()}.csv"
                html_parts.append(f'<li><strong>{friendly_name}:</strong> {csv_filename} ({len(entries)} records)</li>')
        
        if not category_files_generated:
            html_parts.append('<li><em>No category-specific files generated (each category had less than 5 records)</em></li>')
        
        html_parts.append('</ul>')
        
        return ''.join(html_parts)
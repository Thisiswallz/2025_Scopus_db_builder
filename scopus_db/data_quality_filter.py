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


class ScopusDataQualityFilter:
    """
    Filters Scopus CSV data to ensure only high-quality research papers are processed.
    
    Exclusion criteria based on failure analysis:
    1. Editorial content (corrections, errata, graphical abstracts)
    2. Missing critical fields (authors, title)
    3. Conference announcements without research content
    4. Malformed or incomplete records
    """
    
    def __init__(self, enable_filtering: bool = True, log_path: Optional[str] = None):
        """
        Initialize the data quality filter.
        
        Args:
            enable_filtering: Whether to apply filtering (False = no filtering)
            log_path: Path to save exclusion log (auto-generated if None)
        """
        self.enable_filtering = enable_filtering
        self.exclusion_log = []
        self.stats = {
            "total_records": 0,
            "excluded_records": 0,
            "included_records": 0,
            "exclusion_reasons": {}
        }
        
        # Set up logging
        if log_path:
            self.log_path = Path(log_path)
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.log_path = Path(f"data_quality_exclusion_log_{timestamp}.json")
    
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
        
        for i, row in enumerate(csv_data, 1):
            should_exclude, reason = self.should_exclude_record(row, i)
            
            if should_exclude:
                # Log exclusion with relevant data
                exclusion_entry = {
                    "row_index": i,
                    "title": (row.get('Title', '') or '')[:100] + "..." if len(row.get('Title', '')) > 100 else row.get('Title', ''),
                    "authors": row.get('Authors', ''),
                    "document_type": row.get('Document Type', ''),
                    "reason": reason,
                    "category": reason.split(':')[0] if ':' in reason else "OTHER"
                }
                
                # Add specific data that caused exclusion
                category = exclusion_entry["category"]
                if category == "MISSING_AFFILIATIONS":
                    exclusion_entry["affiliations"] = row.get('Affiliations', '')
                elif category == "MISSING_DOI":
                    exclusion_entry["doi"] = row.get('DOI', '')
                elif category == "MISSING_ABSTRACT":
                    exclusion_entry["abstract"] = row.get('Abstract', '')
                elif category == "MISSING_AUTHORS":
                    exclusion_entry["authors_data"] = row.get('Authors', '')
                elif category == "MISSING_AUTHOR_IDS":
                    exclusion_entry["author_ids"] = row.get('Author(s) ID', '')
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
        
        # Save detailed log
        self._save_exclusion_log()
        
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
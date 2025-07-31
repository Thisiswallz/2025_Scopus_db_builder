"""
Data Quality Filter for Scopus CSV Data (Simplified - No CrossRef)

Filters out low-quality entries and provides detailed logging of exclusion rationales.
Focuses on excluding editorial content, incomplete records, and malformed data.
"""

import re
import json
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from pathlib import Path


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
    
    def __init__(self, enable_filtering: bool = True, log_path: Optional[str] = None):
        """
        Initialize the data quality filter.
        
        Args:
            enable_filtering: Whether to apply filtering (False = no filtering)
            log_path: Path to save exclusion log (auto-generated if None)
        """
        self.enable_filtering = enable_filtering
        
        # Setup logging
        if log_path:
            self.log_path = Path(log_path)
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.log_path = Path(f"data_quality_exclusions_{timestamp}.json")
        
        # Initialize statistics
        self.stats = {
            "total_records": 0,
            "excluded_records": 0,
            "included_records": 0,
            "exclusion_reasons": {}
        }
        
        # Store detailed exclusion records
        self.exclusion_log = []
        
        # Required fields for quality check
        self.required_fields = [
            "authors",
            "author_ids", 
            "title",
            "year",
            "affiliations",
            "abstract"
        ]
    
    def should_exclude_record(self, row: Dict[str, str], row_index: int) -> Tuple[bool, str]:
        """
        Determine if a record should be excluded based on data quality criteria.
        
        Args:
            row: CSV row as dictionary
            row_index: Row number for logging
            
        Returns:
            (should_exclude, reason): Tuple of exclusion decision and reason
        """
        if not self.enable_filtering:
            return False, ""
        
        # Check for required fields
        for field in self.required_fields:
            value = self._get_field_value(row, field)
            if not value or value.strip() == "":
                reason = f"MISSING_{field.upper()}: No {field.replace('_', ' ')} provided"
                return True, reason
        
        return False, ""
    
    def _get_field_value(self, row: Dict[str, str], field_name: str) -> str:
        """
        Get field value handling various CSV header formats.
        
        Args:
            row: CSV row dictionary
            field_name: Field name to look for
            
        Returns:
            Field value or empty string if not found
        """
        # Direct match
        if field_name in row:
            return row[field_name]
        
        # Case-insensitive search
        for key in row.keys():
            if field_name.lower() in key.lower():
                return row[key]
        
        # Handle specific field variations
        field_variations = {
            "authors": ["Authors", "Author(s)", "Author Names"],
            "author_ids": ["Author(s) ID", "Author IDs", "Scopus Author ID"],
            "title": ["Title", "Article Title", "Document Title"],
            "year": ["Year", "Publication Year", "Pub Year"],
            "affiliations": ["Affiliations", "Author Affiliations", "Institution(s)"],
            "abstract": ["Abstract", "Summary", "Description"]
        }
        
        if field_name in field_variations:
            for variant in field_variations[field_name]:
                if variant in row:
                    return row[variant]
                # Case-insensitive variant search
                for key in row.keys():
                    if variant.lower() == key.lower():
                        return row[key]
        
        return ""
    
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
        
        # Progress logging setup
        import logging
        logger = logging.getLogger(__name__)
        
        for i, row in enumerate(csv_data, 1):
            # Progress logging every 100 records
            if i % 100 == 0:
                logger.info(f"📊 Processing record {i:,} of {len(csv_data):,} ({i/len(csv_data)*100:.1f}%)")
            
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
        
        # Create exclusion report
        exclusion_rate = (self.stats["excluded_records"] / self.stats["total_records"] * 100) if self.stats["total_records"] > 0 else 0
        
        report = {
            "timestamp": datetime.now().isoformat(),
            "summary": {
                "total_records": self.stats["total_records"],
                "included_records": self.stats["included_records"],
                "excluded_records": self.stats["excluded_records"],
                "exclusion_rate": f"{exclusion_rate:.1f}%",
                "quality_improvement": f"Filtered out {exclusion_rate:.1f}% low-quality entries"
            },
            "exclusion_breakdown": self.stats["exclusion_reasons"],
            "exclusions": self.exclusion_log,
            "log_file": str(self.log_path)
        }
        
        # Save the report
        self._save_exclusion_report(report)
        
        return filtered_data, report
    
    def _save_exclusion_report(self, report: Dict):
        """Save exclusion report in multiple formats."""
        # Save JSON report
        with open(self.log_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        # Save human-readable text report
        text_path = self.log_path.with_suffix('.txt')
        with open(text_path, 'w', encoding='utf-8') as f:
            f.write("SCOPUS DATA QUALITY EXCLUSION REPORT\n")
            f.write("=" * 70 + "\n\n")
            f.write(f"Generated: {report['timestamp']}\n")
            f.write(f"Total Records Processed: {report['summary']['total_records']:,}\n")
            f.write(f"Records Included: {report['summary']['included_records']:,}\n")
            f.write(f"Records Excluded: {report['summary']['excluded_records']:,}\n")
            f.write(f"Exclusion Rate: {report['summary']['exclusion_rate']}\n\n")
            
            f.write("EXCLUSION BREAKDOWN BY CATEGORY:\n")
            f.write("-" * 50 + "\n")
            for category, count in sorted(report['exclusion_breakdown'].items()):
                percentage = (count / report['summary']['excluded_records'] * 100) if report['summary']['excluded_records'] > 0 else 0
                f.write(f"{category:<30} {count:>6,} ({percentage:>5.1f}%)\n")
            
            f.write("\n\nDETAILED EXCLUSION LOG:\n")
            f.write("-" * 70 + "\n")
            for entry in report['exclusions'][:100]:  # First 100 for brevity
                f.write(f"\nRow {entry['row_index']}: {entry['reason']}\n")
                if 'title' in entry:
                    f.write(f"  Title: {entry.get('title', 'N/A')[:100]}...\n")
                if 'authors' in entry:
                    f.write(f"  Authors: {entry.get('authors', 'N/A')[:100]}...\n")
        
        # Save CSV export of excluded records
        self._save_csv_export(report)
        
        # Generate HTML report
        self._generate_html_report(report)
        
        exclusion_rate = (self.stats["excluded_records"] / self.stats["total_records"] * 100) if self.stats["total_records"] > 0 else 0
        print(f"   ✅ Records included: {report['summary']['included_records']:,} ({100-exclusion_rate:.1f}%)")
        print(f"   ❌ Records excluded: {report['summary']['excluded_records']:,} ({exclusion_rate:.1f}%)")
        print(f"   📝 Detailed exclusion log saved: {self.log_path}")
        print(f"   📋 Human-readable report saved: {text_path}")
    
    def _save_csv_export(self, report: Dict):
        """Save excluded records as CSV for easy review."""
        csv_path = self.log_path.with_suffix('.csv')
        
        if not report['exclusions']:
            return
        
        import csv
        
        # Get all unique field names from excluded records
        all_fields = set()
        for record in report['exclusions']:
            all_fields.update(record.keys())
        
        # Order fields logically
        ordered_fields = ['row_index', 'reason', 'category']
        data_fields = sorted([f for f in all_fields if f not in ordered_fields])
        fieldnames = ordered_fields + data_fields
        
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(report['exclusions'])
        
        print(f"   📊 CSV export of excluded records saved: {csv_path}")
        
        # Also create category-specific CSV files for major exclusion categories
        for category in report['exclusion_breakdown']:
            if report['exclusion_breakdown'][category] >= 10:  # Only for categories with 10+ exclusions
                category_records = [r for r in report['exclusions'] if r['category'] == category]
                category_csv_path = self.log_path.with_suffix(f'.{category.lower()}.csv')
                
                with open(category_csv_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(category_records)
                
                print(f"   📊 {category} exclusions CSV saved: {category_csv_path}")
    
    def _generate_html_report(self, report: Dict):
        """Generate an interactive HTML report."""
        html_path = self.log_path.with_suffix('.html')
        
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Scopus Data Quality Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
        .container {{ max-width: 1200px; margin: 0 auto; background-color: white; padding: 20px; box-shadow: 0 0 10px rgba(0,0,0,0.1); }}
        h1 {{ color: #333; text-align: center; }}
        .summary {{ background-color: #e8f4f8; padding: 15px; border-radius: 8px; margin: 20px 0; }}
        .stats {{ display: flex; justify-content: space-around; flex-wrap: wrap; }}
        .stat-box {{ background-color: white; padding: 20px; margin: 10px; border-radius: 8px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); text-align: center; }}
        .stat-number {{ font-size: 36px; font-weight: bold; color: #2196F3; }}
        .stat-label {{ color: #666; margin-top: 5px; }}
        table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
        th {{ background-color: #2196F3; color: white; }}
        tr:nth-child(even) {{ background-color: #f9f9f9; }}
        .category-chart {{ margin: 20px 0; }}
        .bar {{ background-color: #2196F3; color: white; padding: 5px; margin: 2px 0; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 Scopus Data Quality Exclusion Report</h1>
        <p style="text-align: center; color: #666;">Generated: {report['timestamp']}</p>
        
        <div class="summary">
            <h2>Summary Statistics</h2>
            <div class="stats">
                <div class="stat-box">
                    <div class="stat-number">{report['summary']['total_records']:,}</div>
                    <div class="stat-label">Total Records</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">{report['summary']['included_records']:,}</div>
                    <div class="stat-label">✅ Included</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">{report['summary']['excluded_records']:,}</div>
                    <div class="stat-label">❌ Excluded</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">{report['summary']['exclusion_rate']}</div>
                    <div class="stat-label">Exclusion Rate</div>
                </div>
            </div>
        </div>
        
        <h2>Exclusion Categories</h2>
        <table>
            <tr>
                <th>Category</th>
                <th>Count</th>
                <th>Percentage</th>
                <th>Visual</th>
            </tr>
"""
        
        # Add category breakdown
        total_excluded = report['summary']['excluded_records']
        for category, count in sorted(report['exclusion_breakdown'].items(), key=lambda x: x[1], reverse=True):
            percentage = (count / total_excluded * 100) if total_excluded > 0 else 0
            bar_width = int(percentage * 3)  # Scale for display
            html_content += f"""
            <tr>
                <td>{category}</td>
                <td>{count:,}</td>
                <td>{percentage:.1f}%</td>
                <td><div class="bar" style="width: {bar_width}px;">&nbsp;</div></td>
            </tr>
"""
        
        html_content += """
        </table>
        
        <h2>Sample Excluded Records</h2>
        <table>
            <tr>
                <th>Row</th>
                <th>Reason</th>
                <th>Title</th>
                <th>Authors</th>
            </tr>
"""
        
        # Add sample excluded records
        for entry in report['exclusions'][:50]:  # First 50 records
            title = entry.get('title', 'N/A')[:80] + '...' if len(entry.get('title', '')) > 80 else entry.get('title', 'N/A')
            authors = entry.get('authors', 'N/A')[:60] + '...' if len(entry.get('authors', '')) > 60 else entry.get('authors', 'N/A')
            html_content += f"""
            <tr>
                <td>{entry['row_index']}</td>
                <td>{entry['reason']}</td>
                <td>{title}</td>
                <td>{authors}</td>
            </tr>
"""
        
        html_content += """
        </table>
    </div>
</body>
</html>
"""
        
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"   🌐 Interactive HTML report saved: {html_path}")
    
    def print_exclusion_summary(self):
        """Print a summary of exclusion statistics."""
        print(f"\n📊 DATA QUALITY EXCLUSION SUMMARY")
        
        if self.stats["exclusion_reasons"]:
            print(f"   {'Category':<20} {'Count':>8} {'Rate':>8} {'Description'}")
            print("   " + "-" * 70)
            
            for category, count in sorted(self.stats["exclusion_reasons"].items()):
                rate = (count / self.stats["total_records"] * 100) if self.stats["total_records"] > 0 else 0
                
                # Provide human-readable descriptions
                descriptions = {
                    "MISSING_AUTHORS": "No author names provided",
                    "MISSING_AUTHOR_IDS": "No Scopus Author IDs",
                    "MISSING_TITLE": "No title provided",
                    "MISSING_YEAR": "No publication year",
                    "MISSING_DOI": "No DOI provided",
                    "MISSING_AFFILIATIONS": "No institutional affiliations",
                    "MISSING_ABSTRACT": "No abstract provided"
                }
                
                desc = descriptions.get(category, "Other quality issue")
                print(f"   {category:<20} {count:>8,} {rate:>6.1f}%  {desc}")
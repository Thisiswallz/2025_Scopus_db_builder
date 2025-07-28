#!/usr/bin/env python3
"""
Test script for existing journal search functionality in CrossRef client.

This script tests the `search_by_journal_details()` method with realistic
Scopus data to verify it works correctly before enhancing it for Phase 2.
"""

import sys
from pathlib import Path

# Add the project root to path so we can import modules
sys.path.insert(0, str(Path(__file__).parent))

from scopus_db.crossref.crossref_client import CrossRefClient

def test_journal_search():
    """Test journal search with realistic Scopus publication details."""
    
    print("🔬 Testing Existing Journal Search Method")
    print("=" * 60)
    print()
    
    # Use test email for automated testing
    email = "test@example.com"
    print(f"📧 Using test email: {email}")
    
    # Initialize CrossRef client
    client = CrossRefClient(mailto_email=email)
    print(f"✅ CrossRef client initialized with email: {email}")
    print()
    
    # Test cases with real journal titles that exist in CrossRef
    test_cases = [
        {
            "name": "Popular Journal (Nature)",
            "journal": "Nature",
            "volume": "591",
            "issue": "7849",
            "pages": "234-238",
            "year": "2021"
        },
        {
            "name": "Science Journal",
            "journal": "Science",
            "volume": "371",
            "issue": "6529",
            "pages": "456-467",
            "year": "2021"
        },
        {
            "name": "Manufacturing Journal",
            "journal": "Manufacturing Letters",
            "volume": "30",
            "issue": "",
            "pages": "1-10",
            "year": "2021"
        },
        {
            "name": "Engineering Journal",
            "journal": "Engineering",
            "volume": "7",
            "issue": "2",
            "pages": "123-135",
            "year": "2021"
        }
    ]
    
    print("🧪 Running Journal Search Tests...")
    print("-" * 40)
    
    results = []
    for i, test_case in enumerate(test_cases, 1):
        print(f"\nTest {i}: {test_case['name']}")
        print(f"   Journal: {test_case['journal']}")
        print(f"   Details: Vol.{test_case['volume']}, Issue.{test_case['issue']}, Pages.{test_case['pages']}, Year.{test_case['year']}")
        
        try:
            # Test the existing search_by_journal_details method
            result = client.search_by_journal_details(
                journal=test_case['journal'],
                volume=test_case['volume'] if test_case['volume'] else None,
                issue=test_case['issue'] if test_case['issue'] else None,
                pages=test_case['pages'] if test_case['pages'] else None,
                year=test_case['year'] if test_case['year'] else None
            )
            
            if result:
                doi = client.extract_doi(result)
                title = client.extract_title(result)
                pages = client._extract_pages(result)
                
                print(f"   ✅ MATCH FOUND:")
                print(f"      DOI: {doi}")
                print(f"      Title: {title[:80]}..." if title and len(title) > 80 else f"      Title: {title}")
                print(f"      Pages: {pages}")
                
                results.append({
                    "test": test_case['name'],
                    "success": True,
                    "doi": doi,
                    "title": title,
                    "pages": pages
                })
            else:
                print(f"   ❌ No match found")
                results.append({
                    "test": test_case['name'],
                    "success": False,
                    "doi": None,
                    "title": None,
                    "pages": None
                })
                
        except Exception as e:
            print(f"   ⚠️  Error during search: {e}")
            results.append({
                "test": test_case['name'],
                "success": False,
                "error": str(e)
            })
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 TEST RESULTS SUMMARY")
    print("=" * 60)
    
    successful_tests = sum(1 for r in results if r.get('success', False))
    total_tests = len(results)
    
    print(f"Successful searches: {successful_tests}/{total_tests}")
    print(f"Success rate: {(successful_tests/total_tests)*100:.1f}%")
    print()
    
    if successful_tests > 0:
        print("✅ Journal search method is working!")
        print("   Ready to enhance with confidence scoring for Phase 2")
    else:
        print("❌ Journal search method needs debugging")
        print("   Check network connectivity and API access")
    
    # Show API statistics
    stats = client.get_stats()
    print(f"\n📈 API Statistics:")
    print(f"   Total requests: {stats['total_requests']}")
    print(f"   Successful requests: {stats['successful_requests']}")
    print(f"   Failed requests: {stats['failed_requests']}")
    print(f"   Success rate: {stats['success_rate']*100:.1f}%")
    
    return successful_tests > 0

if __name__ == "__main__":
    test_journal_search()
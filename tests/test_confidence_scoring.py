#!/usr/bin/env python3
"""
Test script for confidence scoring infrastructure.

This script tests the new confidence scoring methods with realistic
Scopus and CrossRef data to ensure proper validation.
"""

import sys
from pathlib import Path

# Add the project root to path so we can import modules
sys.path.insert(0, str(Path(__file__).parent))

from scopus_db.crossref.crossref_client import CrossRefClient

def test_confidence_scoring():
    """Test confidence scoring with various match scenarios."""
    
    print("🧠 Testing Confidence Scoring Infrastructure")
    print("=" * 60)
    print()
    
    # Initialize CrossRef client
    client = CrossRefClient(mailto_email="test@example.com")
    
    # Test Case 1: Perfect match scenario
    print("Test 1: Perfect Match Scenario")
    perfect_scopus = {
        'Title': 'A guide to the Nature Index',
        'Authors': 'Smith, J.; Jones, A.',
        'Year': '2021',
        'Volume': '591',
        'Issue': '7849',
        'Page start': '234',
        'Page end': '238',
        'Source title': 'Nature'
    }
    
    # Get a real CrossRef result for comparison
    crossref_result = client.search_by_journal_details("Nature", year="2021")
    
    if crossref_result:
        confidence = client.calculate_match_confidence(perfect_scopus, crossref_result, 'journal')
        print(f"   Confidence Score: {confidence['confidence_score']:.3f}")
        print(f"   Threshold Status: {confidence['threshold_status']}")
        print(f"   Factors:")
        for factor in confidence['confidence_factors']:
            print(f"      • {factor}")
        print()
    else:
        print("   ❌ No CrossRef result to test with")
        print()
    
    # Test Case 2: Year mismatch scenario
    print("Test 2: Year Mismatch Scenario")
    mismatch_scopus = {
        'Title': 'A guide to the Nature Index',
        'Authors': 'Smith, J.; Jones, A.',
        'Year': '2020',  # Wrong year
        'Volume': '591',
        'Issue': '7849',
        'Page start': '234',  
        'Page end': '238',
        'Source title': 'Nature'
    }
    
    if crossref_result:
        confidence = client.calculate_match_confidence(mismatch_scopus, crossref_result, 'journal')
        print(f"   Confidence Score: {confidence['confidence_score']:.3f}")
        print(f"   Threshold Status: {confidence['threshold_status']}")
        print(f"   Factors:")
        for factor in confidence['confidence_factors']:
            print(f"      • {factor}")
        print()
    else:
        print("   ❌ No CrossRef result to test with")
        print()
    
    # Test Case 3: Author parsing
    print("Test 3: Author Parsing Test")
    author_string = "Zhang, W.; Liu, H.; Wang, S.; Chen, X."
    parsed_authors = client.parse_scopus_author_names(author_string)
    print(f"   Input: {author_string}")
    print(f"   Parsed: {parsed_authors}")
    print()
    
    # Test Case 4: Validation with different search methods
    print("Test 4: Search Method Comparison")
    test_scopus = {
        'Title': 'Advanced manufacturing techniques',
        'Authors': 'Johnson, M.; Brown, R.',  
        'Year': '2021',
        'PubMed ID': '34021142',
        'Source title': 'Nature'
    }
    
    if crossref_result:
        for method in ['pmid', 'journal', 'title']:
            confidence = client.calculate_match_confidence(test_scopus, crossref_result, method)
            print(f"   {method.upper()} method: {confidence['confidence_score']:.3f} ({confidence['threshold_status']})")
    print()
    
    # Test Case 5: Validation method
    print("Test 5: High-level Validation Method")
    if crossref_result:
        validation = client.validate_publication_match(
            perfect_scopus, crossref_result, 'journal', confidence_threshold=0.7
        )
        print(f"   Is Valid Match: {validation['is_valid_match']}")
        print(f"   Confidence: {validation['confidence_score']:.3f}")
        print(f"   Threshold: {validation['confidence_threshold']}")
        print(f"   DOI: {validation['doi']}")
        print(f"   Title: {validation['crossref_title'][:60]}...")
    else:
        print("   ❌ No CrossRef result to test with")
    
    print("\n✅ Confidence scoring infrastructure ready for Phase 2!")

if __name__ == "__main__":
    test_confidence_scoring()
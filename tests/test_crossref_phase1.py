#!/usr/bin/env python3
"""
Test script for CrossRef Phase 1 implementation.

This script tests the basic functionality of the CrossRef integration
without processing a full Scopus dataset.
"""

import sys
from pathlib import Path

# Add the project root to path so we can import modules
sys.path.insert(0, str(Path(__file__).parent))

from scopus_db.data_quality_filter import ScopusDataQualityFilter

def test_crossref_initialization():
    """Test that CrossRef client can be initialized properly."""
    print("🧪 Testing CrossRef initialization...")
    
    # Test with valid email
    try:
        filter_obj = ScopusDataQualityFilter(
            enable_crossref_recovery=True,
            crossref_email="test@example.com"
        )
        print("   ✅ CrossRef filter initialized successfully")
        return True
    except Exception as e:
        print(f"   ❌ CrossRef initialization failed: {e}")
        return False

def test_email_validation():
    """Test email validation functionality."""
    print("\n🧪 Testing email validation...")
    
    filter_obj = ScopusDataQualityFilter()
    
    # Test valid email
    if filter_obj._is_valid_email("researcher@university.edu"):
        print("   ✅ Valid email accepted")
    else:
        print("   ❌ Valid email rejected")
        return False
    
    # Test invalid email
    if not filter_obj._is_valid_email("invalid-email"):
        print("   ✅ Invalid email rejected")
    else:
        print("   ❌ Invalid email accepted")
        return False
    
    return True

def test_sample_data_processing():
    """Test processing sample data with CrossRef disabled."""
    print("\n🧪 Testing sample data processing...")
    
    # Create sample data that would normally be excluded for missing DOI
    sample_data = [
        {
            'Title': 'Test Research Paper on Advanced Manufacturing',
            'Authors': 'Smith, J.; Jones, A.',
            'Author(s) ID': '12345678; 87654321',
            'Year': '2021',
            'DOI': '',  # Missing DOI - would normally be excluded
            'Affiliations': 'University of Technology, Department of Engineering',
            'Abstract': 'This is a test abstract for our research paper on advanced manufacturing techniques. It contains sufficient text to pass the minimum requirements for abstract length and provides meaningful research content.',
            'PubMed ID': '34021142'  # This could be used for recovery
        }
    ]
    
    # Test without CrossRef recovery
    print("   Testing without CrossRef recovery...")
    filter_obj = ScopusDataQualityFilter(enable_crossref_recovery=False)
    filtered_data, report = filter_obj.filter_csv_data(sample_data)
    
    if len(filtered_data) == 0:
        print("   ✅ Record correctly excluded due to missing DOI")
    else:
        print("   ❌ Record should have been excluded")
        return False
    
    return True

def main():
    """Run all tests."""
    print("🚀 CrossRef Phase 1 Integration Tests")
    print("=" * 50)
    
    tests = [
        test_email_validation,
        test_crossref_initialization,
        test_sample_data_processing
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
        else:
            print("   ⚠️  Test failed!")
    
    print("\n" + "=" * 50)
    print(f"📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Phase 1 implementation is working correctly.")
        print("\n💡 Next steps:")
        print("   - Try running with a real Scopus CSV file")
        print("   - Test with your actual email address for CrossRef recovery")
        print("   - Monitor recovery success rates")
    else:
        print("❌ Some tests failed. Please review the implementation.")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
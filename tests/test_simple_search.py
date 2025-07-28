#!/usr/bin/env python3
"""
Simple test of CrossRef API connectivity and basic search functionality.
"""

import sys
from pathlib import Path

# Add the project root to path so we can import modules
sys.path.insert(0, str(Path(__file__).parent))

from scopus_db.crossref.crossref_client import CrossRefClient

def test_basic_search():
    """Test basic CrossRef API connectivity."""
    
    client = CrossRefClient(mailto_email="test@example.com")
    
    print("🔬 Testing Basic CrossRef API Connectivity")
    print("=" * 50)
    
    # Test 1: Simple journal name search without filters
    print("\nTest 1: Simple journal search (Nature)")
    try:
        url = f"{client.base_url}/works"
        params = {'query': 'Nature'}
        
        response = client._make_request(url, params)
        print(f"   🔍 Response status: {'Success' if response else 'Failed'}")
        if response:
            print(f"   📊 Response keys: {list(response.keys())}")
            message = response.get('message', {})
            print(f"   📊 Message keys: {list(message.keys())}")
            items = message.get('items', [])
            print(f"   📊 Items found: {len(items)}")
            
        if response and response.get('message', {}).get('items'):
            items = response['message']['items']
            print(f"   ✅ Found {len(items)} results")
            
            # Show first result
            first_item = items[0]
            doi = client.extract_doi(first_item)
            title = client.extract_title(first_item)
            container = first_item.get('container-title', ['Unknown'])[0] if first_item.get('container-title') else 'Unknown'
            
            print(f"   📄 First result:")
            print(f"      DOI: {doi}")
            print(f"      Title: {title[:60]}..." if title and len(title) > 60 else f"      Title: {title}")
            print(f"      Journal: {container}")
        else:
            print("   ❌ No results found")
            
    except Exception as e:
        print(f"   ⚠️  Error: {e}")
    
    # Test 2: Search with year filter
    print("\nTest 2: Search with year filter (Nature, 2021)")
    try:
        url = f"{client.base_url}/works"
        params = {
            'query': 'Nature',
            'filter': 'from-pub-date:2021,until-pub-date:2021'
        }
        
        response = client._make_request(url, params)
        if response and response.get('message', {}).get('items'):
            items = response['message']['items']
            print(f"   ✅ Found {len(items)} results for 2021")
            
            # Show first result
            first_item = items[0]
            doi = client.extract_doi(first_item)
            title = client.extract_title(first_item)
            published = first_item.get('published', {})
            year = published.get('date-parts', [[0]])[0][0] if published.get('date-parts') else 'Unknown'
            
            print(f"   📄 First result:")
            print(f"      DOI: {doi}")
            print(f"      Title: {title[:60]}..." if title and len(title) > 60 else f"      Title: {title}")
            print(f"      Year: {year}")
        else:
            print("   ❌ No results found")
            
    except Exception as e:
        print(f"   ⚠️  Error: {e}")
    
    # Test 3: Check what the API stats show
    stats = client.get_stats()
    print(f"\n📈 API Statistics:")
    print(f"   Total requests: {stats['total_requests']}")
    print(f"   Successful requests: {stats['successful_requests']}")
    print(f"   Failed requests: {stats['failed_requests']}")
    print(f"   Success rate: {stats['success_rate']*100:.1f}%")

if __name__ == "__main__":
    test_basic_search()
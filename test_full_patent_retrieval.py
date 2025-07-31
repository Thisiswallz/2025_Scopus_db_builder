#!/usr/bin/env python3
"""
Test different approaches to get full patent data including citations.
The search API might not return citations, but individual patent retrieval might.
"""

import os
import json
import sys
import urllib.request
import urllib.parse
from pathlib import Path

# Load .env file
def load_env_file():
    env_path = Path('.env')
    if env_path.exists():
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key] = value

load_env_file()


def test_direct_patent_retrieval():
    """Test if we can get full patent data with citations using direct retrieval."""
    
    api_token = os.environ.get('LENS_API')
    if not api_token:
        print("❌ LENS_API not found")
        return
    
    # Our known patent Lens ID
    patent_lens_id = "091-428-408-796-283"
    
    print(f"🔍 Testing direct patent retrieval for: {patent_lens_id}")
    print("=" * 80)
    
    # Try different API endpoints
    endpoints_to_try = [
        f"https://api.lens.org/patent/{patent_lens_id}",
        f"https://api.lens.org/patent/get/{patent_lens_id}",
        f"https://api.lens.org/patent/retrieve/{patent_lens_id}",
        "https://api.lens.org/patent/search"  # Use search with specific lens_id
    ]
    
    headers = {
        'Authorization': f'Bearer {api_token}',
        'Content-Type': 'application/json',
        'User-Agent': 'ScopusDB-LensEnricher/1.0'
    }
    
    for i, endpoint in enumerate(endpoints_to_try, 1):
        print(f"\n🔍 Approach {i}: {endpoint}")
        
        try:
            if endpoint.endswith('/search'):
                # Use search endpoint with lens_id query
                request_body = {
                    "query": {"bool": {"must": [{"term": {"lens_id": patent_lens_id}}]}},
                    "size": 1,
                    "include": ["*"]  # Try to get all fields
                }
                data = json.dumps(request_body).encode('utf-8')
                request = urllib.request.Request(endpoint, data=data, headers=headers)
            else:
                # Try direct GET request
                request = urllib.request.Request(endpoint, headers=headers)
            
            with urllib.request.urlopen(request, timeout=30) as response:
                response_data = json.loads(response.read().decode('utf-8'))
                
                print(f"✅ SUCCESS! Status: {response.status}")
                
                # Check if we have patent data
                if 'data' in response_data and response_data['data']:
                    patent = response_data['data'][0] if isinstance(response_data['data'], list) else response_data['data']
                elif 'lens_id' in response_data:
                    patent = response_data
                else:
                    patent = response_data
                
                print(f"📊 Available fields: {list(patent.keys()) if isinstance(patent, dict) else 'Not a dict'}")
                
                # Look for citation fields
                citation_fields = [
                    'references_cited', 'citations', 'cited_by', 'references',
                    'patent_citations', 'npl_citations', 'biblio'
                ]
                
                found_citations = False
                for field in citation_fields:
                    if field in patent:
                        print(f"✅ Found citation field: {field}")
                        
                        if field == 'references_cited' or field == 'citations':
                            refs = patent[field]
                            if isinstance(refs, dict):
                                print(f"   Structure: {refs.keys()}")
                                citations = refs.get('citations', [])
                                npl_count = refs.get('npl_count', 0)
                                print(f"   Total citations: {len(citations)}")
                                print(f"   NPL citations: {npl_count}")
                                
                                # Look for our test DOI
                                test_doi = "10.1039/b617764f"
                                for cit in citations:
                                    if 'nplcit' in cit:
                                        npl_text = cit['nplcit'].get('text', '')
                                        if test_doi in npl_text:
                                            print(f"   🎯 Found target DOI in citation!")
                                            found_citations = True
                        
                        elif field == 'biblio':
                            # Check if biblio contains references
                            biblio = patent[field]
                            if 'references_cited' in biblio:
                                print(f"   Found references_cited in biblio!")
                                found_citations = True
                
                if found_citations:
                    print(f"🎉 SUCCESS! Found citation data with direct retrieval!")
                    
                    # Save the full data for inspection
                    output_file = f"patent_{patent_lens_id}_full_with_citations.json"
                    with open(output_file, 'w') as f:
                        json.dump(patent, f, indent=2, ensure_ascii=False)
                    print(f"💾 Full data saved to: {output_file}")
                    
                    return True
                else:
                    print(f"❌ No citation fields found in direct retrieval")
                
                break  # Found a working endpoint
                
        except urllib.error.HTTPError as e:
            error_data = e.read().decode('utf-8') if e.fp else "No error data"
            print(f"❌ HTTP {e.code}: {error_data}")
            continue
        except Exception as e:
            print(f"❌ Error: {e}")
            continue
    
    return False


def test_search_with_all_fields():
    """Test search API with wildcard include to get all available fields."""
    
    api_token = os.environ.get('LENS_API')
    
    print(f"\n🔍 Testing search with all fields (*)")
    print("=" * 50)
    
    headers = {
        'Authorization': f'Bearer {api_token}',
        'Content-Type': 'application/json',
        'User-Agent': 'ScopusDB-LensEnricher/1.0'
    }
    
    # Search for our patent and try to get all fields
    request_body = {
        "query": {"bool": {"must": [{"query_string": {"query": 'doc_number:"2013121230" AND jurisdiction:"WO"'}}]}},
        "size": 1,
        "include": ["*"]
    }
    
    try:
        data = json.dumps(request_body).encode('utf-8')
        request = urllib.request.Request("https://api.lens.org/patent/search", data=data, headers=headers)
        
        with urllib.request.urlopen(request, timeout=30) as response:
            response_data = json.loads(response.read().decode('utf-8'))
            
            if response_data.get('data'):
                patent = response_data['data'][0]
                print(f"✅ Fields with include=['*']: {sorted(patent.keys())}")
                
                # Check if we got citation data
                if 'references_cited' in patent:
                    print(f"🎉 SUCCESS! Got references_cited with wildcard include!")
                    return True
                else:
                    print(f"❌ Still no references_cited with wildcard")
            
    except Exception as e:
        print(f"❌ Wildcard search failed: {e}")
    
    return False


if __name__ == "__main__":
    success = test_direct_patent_retrieval()
    
    if not success:
        test_search_with_all_fields()
    
    if not success:
        print(f"\n💡 CONCLUSION:")
        print(f"The Lens API search endpoint may not return citation data.")
        print(f"However, our reverse lookup (DOI → citing patents) works perfectly!")
        print(f"This is sufficient for the enrichment system's primary goal.")
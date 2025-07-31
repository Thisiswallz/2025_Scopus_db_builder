#!/usr/bin/env python3
"""
Test reverse citation lookup: given a DOI, find all patents that cite it.
This is the key functionality for discovering which patents cite Scopus papers.
"""

import os
import json
import sys
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

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from scopus_db.lens.client import LensClient, LensAPIError


def test_doi_reverse_lookup():
    """Test finding patents that cite a specific DOI."""
    
    api_token = os.environ.get('LENS_API')
    if not api_token:
        print("❌ LENS_API not found")
        return
    
    # The DOI we found in the patent citation
    test_doi = "10.1039/b617764f"
    paper_title = "Rapid prototyping of microfluidic devices with a wax printer"
    
    print(f"🔍 Testing reverse lookup for DOI: {test_doi}")
    print(f"📄 Paper: {paper_title}")
    print("=" * 80)
    
    try:
        client = LensClient(api_token, verbose=True)
        
        # Try different query approaches to find patents citing this DOI
        queries_to_try = [
            # Direct DOI search in references
            f'references_cited.nplcit.external_ids:"{test_doi}"',
            
            # DOI in citation text
            f'references_cited.citations.nplcit.text:"{test_doi}"',
            
            # Broader search for the DOI anywhere
            f'"{test_doi}"',
            
            # Search by paper title in citations
            f'references_cited.nplcit.text:"microfluidic devices"',
            
            # Search by author name in citations
            f'references_cited.nplcit.text:"KAIGALA"',
            
            # Try without the field specification
            f'npl_citations:"{test_doi}"',
            
            # Alternative field names
            f'cited_by.npl.external_ids:"{test_doi}"'
        ]
        
        patents_found = []
        
        for i, query in enumerate(queries_to_try, 1):
            print(f"\n🔍 Query {i}: {query}")
            
            try:
                response = client.search_patents(
                    query=query,
                    size=10
                )
                
                print(f"📊 Results: {len(response.results)} patents (total: {response.total})")
                
                if response.results:
                    print(f"✅ SUCCESS! Found patents citing the DOI")
                    
                    for j, patent in enumerate(response.results, 1):
                        doc_key = patent.get('doc_key', 'N/A')
                        title = patent.get('biblio', {}).get('invention_title', [{}])[0].get('text', 'N/A')
                        date_pub = patent.get('date_published', 'N/A')
                        
                        print(f"   {j}. {doc_key} ({date_pub})")
                        print(f"      {title[:100]}...")
                        
                        patents_found.append(patent)
                        
                        # Check if this is our known patent
                        if 'WO_2013121230' in doc_key:
                            print(f"      🎯 CONFIRMED: This is our test patent!")
                    
                    # If we found results, let's examine the citations in detail
                    if patents_found:
                        examine_patent_citations(patents_found[0], test_doi)
                        break  # Found what we need
                
            except LensAPIError as e:
                print(f"❌ API Error: {e}")
                continue
            except Exception as e:
                print(f"❌ Unexpected error: {e}")
                continue
        
        if not patents_found:
            print(f"\n❌ No patents found citing DOI {test_doi}")
            print("💡 Let's try a broader search to understand the data structure...")
            
            # Try to find any patents with citations
            try:
                response = client.search_patents(
                    query="references_cited.npl_count:>0",
                    size=3
                )
                
                if response.results:
                    print(f"\n📄 Found {len(response.results)} patents with NPL citations:")
                    for patent in response.results:
                        examine_patent_citations(patent, test_doi)
                
            except Exception as e:
                print(f"❌ Broader search also failed: {e}")
        
        else:
            print(f"\n🎉 SUCCESS! Reverse citation lookup works!")
            print(f"Found {len(patents_found)} patents citing DOI {test_doi}")
        
    except Exception as e:
        print(f"❌ Client error: {e}")


def examine_patent_citations(patent, target_doi):
    """Examine the citation structure in a patent."""
    print(f"\n📋 EXAMINING CITATIONS IN: {patent.get('doc_key', 'N/A')}")
    print("-" * 60)
    
    # Look for references_cited field
    refs_cited = patent.get('references_cited', {})
    if refs_cited:
        citations = refs_cited.get('citations', [])
        npl_count = refs_cited.get('npl_count', 0)
        
        print(f"📚 Total citations: {len(citations)}")
        print(f"📖 NPL citations: {npl_count}")
        
        # Look through citations for our DOI
        found_target = False
        for i, citation in enumerate(citations, 1):
            if 'nplcit' in citation:
                npl = citation['nplcit']
                citation_text = npl.get('text', '')
                external_ids = npl.get('external_ids', [])
                
                print(f"\n   📄 NPL Citation {i}:")
                print(f"      Text: {citation_text[:150]}...")
                print(f"      External IDs: {external_ids}")
                
                # Check if this citation contains our target DOI
                if target_doi in citation_text or target_doi in external_ids:
                    print(f"      🎯 TARGET DOI FOUND!")
                    found_target = True
                    
                    # Show full citation details
                    print(f"      📋 Full citation text:")
                    print(f"         {citation_text}")
                    print(f"      🔗 Lens ID: {npl.get('lens_id', 'N/A')}")
        
        if not found_target:
            print(f"   ❌ Target DOI {target_doi} not found in this patent's citations")
    
    else:
        print(f"❌ No references_cited field found in this patent")
    
    print("-" * 60)


def test_alternative_approaches():
    """Test alternative approaches to find citing patents."""
    print(f"\n🔬 TESTING ALTERNATIVE APPROACHES")
    print("=" * 50)
    
    api_token = os.environ.get('LENS_API')
    client = LensClient(api_token, verbose=True)
    
    # Try searching for patents with any NPL citations
    try:
        print(f"🔍 Searching for patents with NPL citations...")
        response = client.search_patents(
            query="biblio.references_cited.npl_count:>0",
            size=5
        )
        
        print(f"Found {len(response.results)} patents with NPL citations")
        
        for patent in response.results:
            doc_key = patent.get('doc_key', 'N/A')
            print(f"   - {doc_key}")
    
    except Exception as e:
        print(f"❌ Alternative approach failed: {e}")


if __name__ == "__main__":
    test_doi_reverse_lookup()
    test_alternative_approaches()
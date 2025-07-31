#!/usr/bin/env python3
"""
Check what fields are available in patent search results.
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

from scopus_db.lens.client import LensClient


def check_fields():
    """Check available fields in search results vs. individual patent retrieval."""
    
    api_token = os.environ.get('LENS_API')
    client = LensClient(api_token, verbose=True)
    
    # Get our known patent through search
    print("🔍 1. FIELDS FROM SEARCH RESULTS:")
    print("=" * 50)
    
    response = client.search_patents('doc_number:"2013121230" AND jurisdiction:"WO"', size=1)
    if response.results:
        patent = response.results[0]
        print(f"Available fields from search: {sorted(patent.keys())}")
    
    # Try to get the same patent with specific include fields
    print(f"\n🔍 2. REQUESTING SPECIFIC FIELDS:")
    print("=" * 50)
    
    try:
        response = client.search_patents(
            query='doc_number:"2013121230" AND jurisdiction:"WO"',
            size=1,
            include_fields=['lens_id', 'doc_key', 'references_cited']
        )
        
        if response.results:
            patent = response.results[0]
            print(f"Fields when requesting references_cited: {sorted(patent.keys())}")
            
            if 'references_cited' in patent:
                refs = patent['references_cited']
                print(f"✅ references_cited found!")
                print(f"   Structure: {refs.keys()}")
                print(f"   NPL count: {refs.get('npl_count', 0)}")
                print(f"   Citations: {len(refs.get('citations', []))}")
            else:
                print(f"❌ references_cited not found")
        
    except Exception as e:
        print(f"❌ Error with include_fields: {e}")
    
    # Try different field name variations
    print(f"\n🔍 3. TRYING ALTERNATIVE FIELD NAMES:")
    print("=" * 50)
    
    field_variations = [
        'cited_by',
        'citations',
        'references',
        'biblio.references_cited',
        'patent_citations',
        'npl_citations'
    ]
    
    for field in field_variations:
        try:
            response = client.search_patents(
                query='doc_number:"2013121230" AND jurisdiction:"WO"',
                size=1,
                include_fields=['lens_id', field]
            )
            
            if response.results and field in response.results[0]:
                print(f"✅ {field}: Found!")
            else:
                print(f"❌ {field}: Not found")
                
        except Exception as e:
            print(f"❌ {field}: Error - {e}")


if __name__ == "__main__":
    check_fields()
#!/usr/bin/env python3
"""
Comprehensive analysis of patents citing the microfluidics paper.
Shows filing dates, patent status, owners, inventors, and assignees.
"""

import os
import json
import sys
import urllib.request
from pathlib import Path
from datetime import datetime

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


def get_full_patent_details(lens_id, api_token):
    """Get full patent details including citation data via direct API call."""
    
    headers = {
        'Authorization': f'Bearer {api_token}',
        'Content-Type': 'application/json',
        'User-Agent': 'ScopusDB-LensEnricher/1.0'
    }
    
    try:
        request = urllib.request.Request(f"https://api.lens.org/patent/{lens_id}", headers=headers)
        
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode('utf-8'))
    
    except Exception as e:
        print(f"❌ Error retrieving {lens_id}: {e}")
        return None


def analyze_citing_patents():
    """Analyze all patents citing the microfluidics paper."""
    
    api_token = os.environ.get('LENS_API')
    if not api_token:
        print("❌ LENS_API not found")
        return
    
    client = LensClient(api_token, verbose=False)
    
    # The paper we're analyzing
    target_doi = "10.1039/b617764f"
    paper_title = "Rapid prototyping of microfluidic devices with a wax printer"
    
    print("🔬 COMPREHENSIVE PATENT CITATION ANALYSIS")
    print("=" * 80)
    print(f"📄 Target Paper: {paper_title}")
    print(f"🔗 DOI: {target_doi}")
    print("=" * 80)
    
    # Find all patents citing this DOI
    print(f"🔍 Searching for patents citing {target_doi}...")
    
    try:
        response = client.search_patents(f'"{target_doi}"', size=20)
        citing_patents = response.results
        
        print(f"✅ Found {len(citing_patents)} citing patents (total: {response.total})")
        print("\n📊 DETAILED PATENT ANALYSIS:")
        print("=" * 80)
        
        patent_details = []
        
        for i, patent in enumerate(citing_patents, 1):
            print(f"\n{i:2d}. ANALYZING: {patent.get('doc_key', 'N/A')}")
            print("-" * 60)
            
            # Get full patent details
            full_patent = get_full_patent_details(patent['lens_id'], api_token)
            
            if full_patent:
                details = extract_patent_info(full_patent, target_doi)
                patent_details.append(details)
                print_patent_summary(details)
            else:
                print("❌ Could not retrieve full details")
        
        # Generate summary table
        print("\n" + "=" * 100)
        print("📋 SUMMARY TABLE")
        print("=" * 100)
        
        generate_summary_table(patent_details)
        
        # Save detailed report
        report_file = "citing_patents_analysis.json"
        with open(report_file, 'w') as f:
            json.dump({
                'target_paper': {
                    'title': paper_title,
                    'doi': target_doi
                },
                'analysis_date': datetime.now().isoformat(),
                'total_citing_patents': len(patent_details),
                'patents': patent_details
            }, f, indent=2, ensure_ascii=False)
        
        print(f"\n💾 Detailed analysis saved to: {report_file}")
        
    except Exception as e:
        print(f"❌ Error in analysis: {e}")


def extract_patent_info(patent_data, target_doi):
    """Extract comprehensive patent information."""
    
    # Basic info
    info = {
        'lens_id': patent_data.get('lens_id', 'N/A'),
        'doc_key': patent_data.get('doc_key', 'N/A'),
        'doc_number': patent_data.get('doc_number', 'N/A'),
        'jurisdiction': patent_data.get('jurisdiction', 'N/A'),
        'kind': patent_data.get('kind', 'N/A'),
        'publication_date': patent_data.get('date_published', 'N/A'),
        'publication_type': patent_data.get('publication_type', 'N/A')
    }
    
    # Title
    biblio = patent_data.get('biblio', {})
    titles = biblio.get('invention_title', [])
    if titles:
        info['title'] = titles[0].get('text', 'N/A')
    else:
        info['title'] = 'N/A'
    
    # Filing information
    app_ref = biblio.get('application_reference', {})
    info['application_number'] = app_ref.get('doc_number', 'N/A')
    info['filing_date'] = app_ref.get('date', 'N/A')
    info['filing_jurisdiction'] = app_ref.get('jurisdiction', 'N/A')
    
    # Priority information
    priority = biblio.get('priority_claims', {})
    if priority:
        earliest = priority.get('earliest_claim', {})
        info['priority_date'] = earliest.get('date', 'N/A')
        info['priority_jurisdiction'] = earliest.get('jurisdiction', 'N/A')
        
        # All priority claims
        claims = priority.get('claims', [])
        info['priority_claims'] = []
        for claim in claims:
            info['priority_claims'].append({
                'jurisdiction': claim.get('jurisdiction', 'N/A'),
                'number': claim.get('doc_number', 'N/A'),
                'date': claim.get('date', 'N/A')
            })
    else:
        info['priority_date'] = 'N/A'
        info['priority_jurisdiction'] = 'N/A'
        info['priority_claims'] = []
    
    # Parties (inventors and applicants)
    parties = biblio.get('parties', {})
    
    # Inventors
    inventors = parties.get('inventors', [])
    info['inventors'] = []
    for inventor in inventors:
        name_info = inventor.get('name', {})
        info['inventors'].append({
            'name': name_info.get('name', 'N/A'),
            'residence': inventor.get('residence', 'N/A')
        })
    
    # Applicants/Assignees
    applicants = parties.get('applicants', [])
    info['applicants'] = []
    for applicant in applicants:
        name_info = applicant.get('name', {})
        info['applicants'].append({
            'name': name_info.get('name', 'N/A'),
            'residence': applicant.get('residence', 'N/A'),
            'type': applicant.get('type', 'N/A')
        })
    
    # Legal status
    legal_status = patent_data.get('legal_status', {})
    info['legal_status'] = legal_status.get('patent_status', 'N/A')
    
    # Patent family
    families = patent_data.get('families', {})
    simple_family = families.get('simple_family', {})
    info['family_id'] = simple_family.get('family_id', 'N/A')
    info['family_size'] = simple_family.get('size', 0)
    
    # Citation verification
    info['citation_confirmed'] = False
    info['citation_sequence'] = 'N/A'
    
    refs_cited = biblio.get('references_cited', {})
    if refs_cited:
        citations = refs_cited.get('citations', [])
        for citation in citations:
            if 'nplcit' in citation:
                npl = citation['nplcit']
                external_ids = npl.get('external_ids', [])
                if target_doi in external_ids or target_doi in npl.get('text', ''):
                    info['citation_confirmed'] = True
                    info['citation_sequence'] = citation.get('sequence', 'N/A')
                    break
    
    return info


def print_patent_summary(info):
    """Print a summary of patent information."""
    
    print(f"📄 Title: {info['title'][:80]}...")
    print(f"🆔 Lens ID: {info['lens_id']}")
    print(f"📅 Filing Date: {info['filing_date']} ({info['filing_jurisdiction']})")
    print(f"📅 Publication Date: {info['publication_date']}")
    print(f"📅 Priority Date: {info['priority_date']} ({info['priority_jurisdiction']})")
    print(f"⚖️  Legal Status: {info['legal_status']}")
    print(f"📋 Type: {info['publication_type']}")
    
    # Inventors
    if info['inventors']:
        print(f"👥 Inventors ({len(info['inventors'])}):")
        for inv in info['inventors'][:3]:  # Show first 3
            print(f"   - {inv['name']} ({inv['residence']})")
        if len(info['inventors']) > 3:
            print(f"   ... and {len(info['inventors']) - 3} more")
    
    # Applicants/Assignees
    if info['applicants']:
        print(f"🏢 Applicants/Assignees ({len(info['applicants'])}):")
        for app in info['applicants'][:3]:  # Show first 3
            app_type = f" [{app['type']}]" if app['type'] != 'N/A' else ""
            print(f"   - {app['name']} ({app['residence']}){app_type}")
        if len(info['applicants']) > 3:
            print(f"   ... and {len(info['applicants']) - 3} more")
    
    # Family info
    if info['family_size'] > 1:
        print(f"👨‍👩‍👧‍👦 Patent Family: {info['family_size']} members (ID: {info['family_id']})")
    
    # Citation confirmation
    citation_status = "✅ CONFIRMED" if info['citation_confirmed'] else "❌ NOT FOUND"
    print(f"🔗 Citation Status: {citation_status}")
    if info['citation_confirmed']:
        print(f"   Sequence: #{info['citation_sequence']}")


def generate_summary_table(patent_details):
    """Generate a summary table of all patents."""
    
    # Sort by filing date
    sorted_patents = sorted(patent_details, key=lambda x: x.get('filing_date', '9999-99-99'))
    
    print(f"{'#':<3} {'Doc Key':<25} {'Filing Date':<12} {'Status':<15} {'Applicant':<30}")
    print("-" * 85)
    
    for i, patent in enumerate(sorted_patents, 1):
        doc_key = patent['doc_key'][:24] if len(patent['doc_key']) > 24 else patent['doc_key']
        filing_date = patent['filing_date'][:10] if patent['filing_date'] != 'N/A' else 'N/A'
        status = patent['legal_status'][:14] if len(patent['legal_status']) > 14 else patent['legal_status']
        
        # Get primary applicant
        primary_applicant = 'N/A'
        if patent['applicants']:
            primary_applicant = patent['applicants'][0]['name'][:29]
        
        print(f"{i:<3} {doc_key:<25} {filing_date:<12} {status:<15} {primary_applicant:<30}")
    
    # Statistics
    print("\n📊 FILING STATISTICS:")
    print("-" * 30)
    
    # Count by jurisdiction
    jurisdictions = {}
    for patent in patent_details:
        jurisdiction = patent['filing_jurisdiction']
        jurisdictions[jurisdiction] = jurisdictions.get(jurisdiction, 0) + 1
    
    print("Filing Jurisdictions:")
    for jurisdiction, count in sorted(jurisdictions.items(), key=lambda x: x[1], reverse=True):
        print(f"   {jurisdiction}: {count} patents")
    
    # Count by status
    statuses = {}
    for patent in patent_details:
        status = patent['legal_status']
        statuses[status] = statuses.get(status, 0) + 1
    
    print("\nLegal Status:")
    for status, count in sorted(statuses.items(), key=lambda x: x[1], reverse=True):
        print(f"   {status}: {count} patents")
    
    # Time span
    filing_dates = [p['filing_date'] for p in patent_details if p['filing_date'] != 'N/A']
    if filing_dates:
        earliest = min(filing_dates)
        latest = max(filing_dates)
        print(f"\nFiling Date Range: {earliest[:10]} to {latest[:10]}")


if __name__ == "__main__":
    analyze_citing_patents()
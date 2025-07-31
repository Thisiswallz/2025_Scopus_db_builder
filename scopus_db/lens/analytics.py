"""
Innovation Analytics

Provides analytics and querying capabilities for Lens-enriched databases.
Calculates innovation metrics and generates insights from patent-publication links.
"""

import sqlite3
import json
from typing import Dict, List, Optional, Any, Tuple
from collections import defaultdict, Counter
from datetime import datetime


class InnovationAnalytics:
    """
    Analytics engine for patent-publication relationships.
    
    Provides:
    - Innovation metrics calculation
    - Author patent profiles
    - Institution innovation scores
    - Technology transfer analysis
    - Query interfaces
    """
    
    def __init__(self, database_path: str):
        """
        Initialize analytics engine.
        
        Args:
            database_path: Path to enriched Scopus database
        """
        self.database_path = database_path
        self._verify_lens_data()
    
    def _verify_lens_data(self):
        """Verify that Lens enrichment data exists."""
        with sqlite3.connect(self.database_path) as conn:
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='patents'"
            )
            if not cursor.fetchone():
                raise ValueError("No Lens enrichment data found. Run enrichment first.")
    
    def calculate_innovation_metrics(self) -> Dict[str, Any]:
        """Calculate comprehensive innovation metrics."""
        with sqlite3.connect(self.database_path) as conn:
            metrics = {}
            
            # Basic counts
            cursor = conn.execute("SELECT COUNT(*) FROM documents")
            metrics['total_publications'] = cursor.fetchone()[0]
            
            cursor = conn.execute("SELECT COUNT(*) FROM patents")
            metrics['total_patents'] = cursor.fetchone()[0]
            
            cursor = conn.execute("SELECT COUNT(DISTINCT eid) FROM publication_patent_links")
            metrics['publications_with_patents'] = cursor.fetchone()[0]
            
            # Publication-to-patent rate
            if metrics['total_publications'] > 0:
                metrics['publication_to_patent_rate'] = (
                    metrics['publications_with_patents'] / metrics['total_publications']
                )
            else:
                metrics['publication_to_patent_rate'] = 0.0
            
            # Confidence distribution
            cursor = conn.execute("""
                SELECT 
                    CASE 
                        WHEN confidence_score >= 0.8 THEN 'high'
                        WHEN confidence_score >= 0.6 THEN 'medium'
                        ELSE 'low'
                    END as confidence_level,
                    COUNT(*) as count
                FROM publication_patent_links
                GROUP BY confidence_level
            """)
            
            confidence_dist = {row[0]: row[1] for row in cursor.fetchall()}
            metrics['confidence_distribution'] = confidence_dist
            
            # Link type distribution
            cursor = conn.execute("""
                SELECT link_type, COUNT(*) as count
                FROM publication_patent_links
                GROUP BY link_type
                ORDER BY count DESC
            """)
            
            link_types = {row[0]: row[1] for row in cursor.fetchall()}
            metrics['link_type_distribution'] = link_types
            
            # Average time to patent
            avg_time = self._calculate_average_time_to_patent()
            if avg_time:
                metrics['average_time_to_patent_years'] = avg_time
            
            return metrics
    
    def _calculate_average_time_to_patent(self) -> Optional[float]:
        """Calculate average time from publication to patent."""
        with sqlite3.connect(self.database_path) as conn:
            cursor = conn.execute("""
                SELECT d.pubyear, p.publication_date
                FROM publication_patent_links ppl
                JOIN documents d ON ppl.eid = d.eid
                JOIN patents p ON ppl.lens_id = p.lens_id
                WHERE d.pubyear IS NOT NULL 
                AND p.publication_date IS NOT NULL
                AND ppl.confidence_score >= 0.7
            """)
            
            time_diffs = []
            for row in cursor.fetchall():
                pub_year = row[0]
                patent_date = row[1]
                
                try:
                    # Extract year from patent date
                    if isinstance(patent_date, str):
                        patent_year = int(patent_date[:4])
                    else:
                        patent_year = int(patent_date)
                    
                    if patent_year >= pub_year:
                        time_diffs.append(patent_year - pub_year)
                except (ValueError, TypeError):
                    continue
            
            if time_diffs:
                return sum(time_diffs) / len(time_diffs)
            
            return None
    
    def get_top_innovating_institutions(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get institutions ranked by innovation metrics."""
        with sqlite3.connect(self.database_path) as conn:
            cursor = conn.execute("""
                SELECT 
                    SUBSTR(d.affilname, 1, 100) as institution,
                    COUNT(DISTINCT d.eid) as publications,
                    COUNT(DISTINCT ppl.lens_id) as patents,
                    COUNT(ppl.id) as total_links,
                    AVG(ppl.confidence_score) as avg_confidence,
                    COUNT(CASE WHEN ppl.confidence_score >= 0.8 THEN 1 END) as high_conf_links
                FROM documents d
                JOIN publication_patent_links ppl ON d.eid = ppl.eid
                WHERE d.affilname IS NOT NULL AND d.affilname != ''
                GROUP BY SUBSTR(d.affilname, 1, 100)
                HAVING publications >= 5 AND patents >= 2
                ORDER BY (patents * 1.0 / publications) DESC, patents DESC
                LIMIT ?
            """, (limit,))
            
            institutions = []
            for row in cursor.fetchall():
                institutions.append({
                    'institution': row[0],
                    'publications': row[1],
                    'patents': row[2],
                    'total_links': row[3],
                    'innovation_rate': row[2] / row[1] if row[1] > 0 else 0,
                    'avg_confidence': round(row[4], 3),
                    'high_confidence_links': row[5]
                })
            
            return institutions
    
    def get_author_innovation_profile(self, author_name: str) -> Optional[Dict[str, Any]]:
        """Get innovation profile for a specific author."""
        with sqlite3.connect(self.database_path) as conn:
            # Find publications by author
            cursor = conn.execute("""
                SELECT d.eid, d.title, d.pubyear, d.authors
                FROM documents d
                WHERE d.authors LIKE ?
            """, (f'%{author_name}%',))
            
            publications = []
            eids = []
            for row in cursor.fetchall():
                publications.append({
                    'eid': row[0],
                    'title': row[1],
                    'year': row[2],
                    'authors': row[3]
                })
                eids.append(row[0])
            
            if not publications:
                return None
            
            # Find linked patents
            eid_placeholders = ','.join(['?' for _ in eids])
            cursor = conn.execute(f"""
                SELECT p.lens_id, p.title, p.publication_date, ppl.confidence_score, ppl.link_type
                FROM publication_patent_links ppl
                JOIN patents p ON ppl.lens_id = p.lens_id
                WHERE ppl.eid IN ({eid_placeholders})
                ORDER BY ppl.confidence_score DESC
            """, eids)
            
            patents = []
            for row in cursor.fetchall():
                patents.append({
                    'lens_id': row[0],
                    'title': row[1],
                    'publication_date': row[2],
                    'confidence': row[3],
                    'link_type': row[4]
                })
            
            # Calculate metrics
            total_pubs = len(publications)
            total_patents = len(patents)
            innovation_rate = total_patents / total_pubs if total_pubs > 0 else 0
            
            high_conf_patents = len([p for p in patents if p['confidence'] >= 0.8])
            avg_confidence = sum(p['confidence'] for p in patents) / len(patents) if patents else 0
            
            return {
                'author_name': author_name,
                'total_publications': total_pubs,
                'total_patents': total_patents,
                'innovation_rate': round(innovation_rate, 3),
                'high_confidence_patents': high_conf_patents,
                'average_confidence': round(avg_confidence, 3),
                'publications': publications[:10],  # Limit for display
                'patents': patents[:10]
            }
    
    def find_author_patents(self, author_name: str) -> List[Dict[str, Any]]:
        """Find patents linked to a specific author."""
        with sqlite3.connect(self.database_path) as conn:
            cursor = conn.execute("""
                SELECT DISTINCT 
                    p.lens_id, p.title, p.publication_date, 
                    ppl.confidence_score, ppl.link_type,
                    d.title as pub_title, d.pubyear
                FROM publication_patent_links ppl
                JOIN patents p ON ppl.lens_id = p.lens_id
                JOIN documents d ON ppl.eid = d.eid
                WHERE d.authors LIKE ?
                ORDER BY ppl.confidence_score DESC, p.publication_date DESC
            """, (f'%{author_name}%',))
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    'lens_id': row[0],
                    'patent_title': row[1],
                    'patent_date': row[2],
                    'confidence': row[3],
                    'link_type': row[4],
                    'publication_title': row[5],
                    'publication_year': row[6]
                })
            
            return results
    
    def get_institution_patents(self, institution_name: str) -> List[Dict[str, Any]]:
        """Find patents linked to a specific institution."""
        with sqlite3.connect(self.database_path) as conn:
            cursor = conn.execute("""
                SELECT DISTINCT 
                    p.lens_id, p.title, p.publication_date,
                    ppl.confidence_score, ppl.link_type,
                    d.title as pub_title, d.authors
                FROM publication_patent_links ppl
                JOIN patents p ON ppl.lens_id = p.lens_id
                JOIN documents d ON ppl.eid = d.eid
                WHERE d.affilname LIKE ?
                ORDER BY ppl.confidence_score DESC, p.publication_date DESC
            """, (f'%{institution_name}%',))
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    'lens_id': row[0],
                    'patent_title': row[1],
                    'patent_date': row[2],
                    'confidence': row[3],
                    'link_type': row[4],
                    'publication_title': row[5],
                    'authors': row[6]
                })
            
            return results
    
    def analyze_technology_transfer_patterns(self) -> Dict[str, Any]:
        """Analyze patterns in technology transfer from research to patents."""
        with sqlite3.connect(self.database_path) as conn:
            # Time lag analysis
            cursor = conn.execute("""
                SELECT 
                    (CAST(SUBSTR(p.publication_date, 1, 4) AS INTEGER) - d.pubyear) as time_lag,
                    COUNT(*) as count
                FROM publication_patent_links ppl
                JOIN documents d ON ppl.eid = d.eid
                JOIN patents p ON ppl.lens_id = p.lens_id
                WHERE d.pubyear IS NOT NULL 
                AND p.publication_date IS NOT NULL
                AND ppl.confidence_score >= 0.7
                AND (CAST(SUBSTR(p.publication_date, 1, 4) AS INTEGER) - d.pubyear) BETWEEN 0 AND 10
                GROUP BY time_lag
                ORDER BY time_lag
            """)
            
            time_lag_dist = {row[0]: row[1] for row in cursor.fetchall()}
            
            # Subject area analysis
            cursor = conn.execute("""
                SELECT 
                    SUBSTR(d.authkeywords, 1, 50) as keywords,
                    COUNT(*) as patent_count
                FROM publication_patent_links ppl
                JOIN documents d ON ppl.eid = d.eid
                WHERE d.authkeywords IS NOT NULL AND d.authkeywords != ''
                AND ppl.confidence_score >= 0.7
                GROUP BY SUBSTR(d.authkeywords, 1, 50)
                HAVING patent_count >= 3
                ORDER BY patent_count DESC
                LIMIT 20
            """)
            
            subject_areas = []
            for row in cursor.fetchall():
                subject_areas.append({
                    'keywords': row[0],
                    'patent_count': row[1]
                })
            
            return {
                'time_lag_distribution': time_lag_dist,
                'most_patented_subjects': subject_areas
            }
    
    def get_collaboration_networks(self) -> Dict[str, Any]:
        """Analyze collaboration networks between authors and inventors."""
        with sqlite3.connect(self.database_path) as conn:
            # Find co-authorship patterns in patented publications
            cursor = conn.execute("""
                SELECT 
                    d.authors,
                    GROUP_CONCAT(DISTINCT p.lens_id) as patents
                FROM publication_patent_links ppl
                JOIN documents d ON ppl.eid = d.eid
                JOIN patents p ON ppl.lens_id = p.lens_id
                WHERE ppl.confidence_score >= 0.7
                AND d.authors IS NOT NULL
                GROUP BY d.authors
                HAVING COUNT(DISTINCT p.lens_id) >= 2
            """)
            
            collaborations = []
            for row in cursor.fetchall():
                authors = row[0].split(';')
                patent_count = len(row[1].split(','))
                
                if len(authors) > 1:
                    collaborations.append({
                        'authors': [a.strip() for a in authors][:5],  # Limit for display
                        'patent_count': patent_count,
                        'collaboration_size': len(authors)
                    })
            
            # Sort by patent count
            collaborations.sort(key=lambda x: x['patent_count'], reverse=True)
            
            return {
                'top_collaborations': collaborations[:20],
                'total_collaborative_groups': len(collaborations)
            }
    
    def export_enrichment_summary(self, output_path: str, format: str = 'json'):
        """Export comprehensive enrichment summary."""
        summary = {
            'generated_at': datetime.now().isoformat(),
            'innovation_metrics': self.calculate_innovation_metrics(),
            'top_institutions': self.get_top_innovating_institutions(10),
            'technology_transfer': self.analyze_technology_transfer_patterns(),
            'collaboration_networks': self.get_collaboration_networks()
        }
        
        if format.lower() == 'json':
            with open(output_path, 'w') as f:
                json.dump(summary, f, indent=2)
        elif format.lower() == 'txt':
            self._export_text_summary(summary, output_path)
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def _export_text_summary(self, summary: Dict[str, Any], output_path: str):
        """Export summary in text format."""
        with open(output_path, 'w') as f:
            f.write("LENS ENRICHMENT SUMMARY\n")
            f.write("=" * 50 + "\n\n")
            
            # Innovation metrics
            metrics = summary['innovation_metrics']
            f.write("📊 INNOVATION METRICS\n")
            f.write("-" * 20 + "\n")
            f.write(f"Total Publications: {metrics['total_publications']:,}\n")
            f.write(f"Total Patents Found: {metrics['total_patents']:,}\n")
            f.write(f"Publications with Patents: {metrics['publications_with_patents']:,}\n")
            f.write(f"Publication-to-Patent Rate: {metrics['publication_to_patent_rate']:.1%}\n")
            
            if 'average_time_to_patent_years' in metrics:
                f.write(f"Average Time to Patent: {metrics['average_time_to_patent_years']:.1f} years\n")
            
            f.write("\n")
            
            # Top institutions
            f.write("🏛️  TOP INNOVATING INSTITUTIONS\n")
            f.write("-" * 30 + "\n")
            for i, inst in enumerate(summary['top_institutions'][:10], 1):
                f.write(f"{i:2d}. {inst['institution']}\n")
                f.write(f"    Publications: {inst['publications']}, Patents: {inst['patents']}\n")
                f.write(f"    Innovation Rate: {inst['innovation_rate']:.1%}\n")
                f.write("\n")
    
    def get_database_stats(self) -> Dict[str, int]:
        """Get basic database statistics."""
        with sqlite3.connect(self.database_path) as conn:
            stats = {}
            
            tables = ['documents', 'patents', 'patent_inventors', 'patent_applicants', 
                     'publication_patent_links', 'lens_enrichment_log']
            
            for table in tables:
                try:
                    cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
                    stats[table] = cursor.fetchone()[0]
                except sqlite3.Error:
                    stats[table] = 0
            
            return stats
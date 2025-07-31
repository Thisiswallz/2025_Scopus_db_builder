"""
Patent Matching Algorithms

Implements confidence scoring and name matching algorithms for 
linking Scopus publications with Lens patent data.
"""

import re
import unicodedata
from typing import Dict, List, Optional, Any, Set
from difflib import SequenceMatcher


class PatentMatcher:
    """
    Handles matching logic between publications and patents.
    
    Implements:
    - Name variant generation
    - Institution name cleaning
    - Confidence scoring algorithms
    - Match evaluation
    """
    
    def __init__(self, confidence_thresholds: Dict[str, float]):
        """
        Initialize the matcher.
        
        Args:
            confidence_thresholds: Thresholds for different match types
        """
        self.thresholds = confidence_thresholds
        
        # Common academic title variations
        self.academic_titles = {
            'prof', 'professor', 'dr', 'doctor', 'phd', 'md', 'jr', 'sr',
            'mr', 'mrs', 'ms', 'miss', 'assoc', 'assistant', 'associate'
        }
        
        # Institution type indicators
        self.institution_types = {
            'university', 'univ', 'college', 'institute', 'inst', 'school',
            'research', 'center', 'centre', 'laboratory', 'lab', 'hospital',
            'clinic', 'foundation', 'corporation', 'corp', 'company', 'co',
            'ltd', 'limited', 'inc', 'incorporated', 'llc', 'gmbh'
        }
    
    def generate_name_variants(self, author_name: str) -> List[str]:
        """
        Generate name variants for patent searching.
        
        Args:
            author_name: Original author name
        
        Returns:
            List of name variations to search
        """
        if not author_name or len(author_name) < 3:
            return []
        
        # Clean the name
        cleaned = self._clean_author_name(author_name)
        if not cleaned:
            return []
        
        variants = set()
        variants.add(cleaned)
        
        # Parse name components
        parts = cleaned.split()
        if len(parts) < 2:
            return list(variants)
        
        # Assume last part is family name
        family_name = parts[-1]
        given_names = parts[:-1]
        
        # Generate common variations
        if len(given_names) >= 1:
            first_given = given_names[0]
            
            # Full name variations
            variants.add(f"{first_given} {family_name}")
            variants.add(f"{family_name} {first_given}")
            variants.add(f"{family_name}, {first_given}")
            
            # Initial variations
            if len(first_given) > 1:
                initial = first_given[0]
                variants.add(f"{initial} {family_name}")
                variants.add(f"{initial}. {family_name}")
                variants.add(f"{family_name} {initial}")
                variants.add(f"{family_name}, {initial}")
                variants.add(f"{family_name}, {initial}.")
            
            # Multiple given names
            if len(given_names) > 1:
                all_initials = ''.join([name[0] for name in given_names])
                variants.add(f"{all_initials} {family_name}")
                variants.add(f"{family_name} {all_initials}")
                
                # First name + initial
                if len(given_names[1]) > 0:
                    second_initial = given_names[1][0]
                    variants.add(f"{first_given} {second_initial} {family_name}")
                    variants.add(f"{first_given} {second_initial}. {family_name}")
        
        # Remove empty and short variants
        valid_variants = [v for v in variants if v and len(v) >= 3 and ' ' in v]
        
        # Limit to avoid rate limit issues
        return valid_variants[:5]
    
    def _clean_author_name(self, name: str) -> str:
        """Clean and normalize author name."""
        if not name:
            return ""
        
        # Normalize unicode
        name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode()
        
        # Remove academic titles and suffixes
        words = name.lower().split()
        filtered_words = []
        
        for word in words:
            # Remove punctuation except hyphens
            clean_word = re.sub(r'[^\w\s-]', '', word)
            
            # Skip academic titles
            if clean_word not in self.academic_titles:
                filtered_words.append(clean_word)
        
        # Reconstruct name
        cleaned = ' '.join(filtered_words)
        
        # Basic validation
        if len(cleaned) < 3 or not re.search(r'[a-zA-Z]', cleaned):
            return ""
        
        # Title case
        return ' '.join(word.capitalize() for word in cleaned.split())
    
    def clean_institution_name(self, institution: str) -> str:
        """Clean and normalize institution name for searching."""
        if not institution:
            return ""
        
        # Normalize unicode
        clean = unicodedata.normalize('NFKD', institution).encode('ascii', 'ignore').decode()
        
        # Remove common prefixes/suffixes
        clean = re.sub(r'^(the|an|a)\s+', '', clean, flags=re.IGNORECASE)
        
        # Remove department information (text in parentheses)
        clean = re.sub(r'\([^)]*\)', '', clean)
        
        # Remove address information (after comma)
        clean = clean.split(',')[0]
        
        # Remove extra whitespace
        clean = ' '.join(clean.split())
        
        # Extract main institution name (before first semicolon or dash)
        clean = clean.split(';')[0].split(' - ')[0]
        
        return clean.strip()
    
    def calculate_confidence(
        self,
        publication: Dict[str, Any],
        patent_data: Dict[str, Any],
        match_type: str,
        match_context: str
    ) -> float:
        """
        Calculate confidence score for a publication-patent match.
        
        Args:
            publication: Scopus publication data
            patent_data: Lens patent data
            match_type: Type of match ('author_match', 'institution_match', 'subject_match')
            match_context: Context used for matching (author name, institution, etc.)
        
        Returns:
            Confidence score between 0.0 and 1.0
        """
        if match_type == 'author_match':
            return self._calculate_author_confidence(publication, patent_data, match_context)
        elif match_type == 'institution_match':
            return self._calculate_institution_confidence(publication, patent_data, match_context)
        elif match_type == 'subject_match':
            return self._calculate_subject_confidence(publication, patent_data, match_context)
        else:
            return 0.0
    
    def _calculate_author_confidence(
        self,
        publication: Dict[str, Any],
        patent_data: Dict[str, Any],
        author_context: str
    ) -> float:
        """Calculate confidence for author-based matches."""
        score = 0.0
        
        # Name similarity (40% weight)
        name_score = self._calculate_name_similarity(author_context, patent_data)
        score += name_score * 0.4
        
        # Institution correlation (30% weight)
        institution_score = self._calculate_institution_correlation(publication, patent_data)
        score += institution_score * 0.3
        
        # Publication year proximity (20% weight)
        year_score = self._calculate_year_proximity(publication, patent_data)
        score += year_score * 0.2
        
        # Subject overlap (10% weight)
        subject_score = self._calculate_subject_overlap(publication, patent_data)
        score += subject_score * 0.1
        
        return min(score, 1.0)
    
    def _calculate_institution_confidence(
        self,
        publication: Dict[str, Any],
        patent_data: Dict[str, Any],
        institution_context: str
    ) -> float:
        """Calculate confidence for institution-based matches."""
        score = 0.0
        
        # Institution name similarity (50% weight)
        institution_score = self._calculate_institution_name_similarity(
            institution_context, patent_data
        )
        score += institution_score * 0.5
        
        # Author overlap (25% weight)
        author_score = self._calculate_author_overlap(publication, patent_data)
        score += author_score * 0.25
        
        # Year proximity (15% weight)
        year_score = self._calculate_year_proximity(publication, patent_data)
        score += year_score * 0.15
        
        # Subject overlap (10% weight)
        subject_score = self._calculate_subject_overlap(publication, patent_data)
        score += subject_score * 0.1
        
        return min(score, 1.0)
    
    def _calculate_subject_confidence(
        self,
        publication: Dict[str, Any],
        patent_data: Dict[str, Any],
        keyword_context: str
    ) -> float:
        """Calculate confidence for subject-based matches."""
        score = 0.0
        
        # Subject similarity (60% weight)
        subject_score = self._calculate_subject_overlap(publication, patent_data)
        score += subject_score * 0.6
        
        # Title similarity (25% weight)
        title_score = self._calculate_title_similarity(publication, patent_data)
        score += title_score * 0.25
        
        # Year proximity (15% weight)
        year_score = self._calculate_year_proximity(publication, patent_data)
        score += year_score * 0.15
        
        return min(score, 1.0)
    
    def _calculate_name_similarity(self, author_name: str, patent_data: Dict[str, Any]) -> float:
        """Calculate similarity between author name and patent inventors."""
        if not author_name:
            return 0.0
        
        inventors = patent_data.get('inventors', [])
        if not inventors:
            return 0.0
        
        max_similarity = 0.0
        author_clean = self._clean_author_name(author_name).lower()
        
        for inventor in inventors:
            inventor_name = inventor.get('name', '')
            if inventor_name:
                inventor_clean = self._clean_author_name(inventor_name).lower()
                similarity = SequenceMatcher(None, author_clean, inventor_clean).ratio()
                max_similarity = max(max_similarity, similarity)
        
        return max_similarity
    
    def _calculate_institution_correlation(
        self,
        publication: Dict[str, Any],
        patent_data: Dict[str, Any]
    ) -> float:
        """Calculate correlation between publication institutions and patent applicants."""
        pub_institutions = self._extract_institutions(publication.get('affiliations', ''))
        patent_applicants = patent_data.get('applicants', [])
        
        if not pub_institutions or not patent_applicants:
            return 0.0
        
        max_similarity = 0.0
        
        for pub_inst in pub_institutions:
            pub_clean = self.clean_institution_name(pub_inst).lower()
            
            for applicant in patent_applicants:
                app_name = applicant.get('name', '')
                if app_name:
                    app_clean = self.clean_institution_name(app_name).lower()
                    similarity = SequenceMatcher(None, pub_clean, app_clean).ratio()
                    max_similarity = max(max_similarity, similarity)
        
        return max_similarity
    
    def _calculate_institution_name_similarity(
        self,
        institution_context: str,
        patent_data: Dict[str, Any]
    ) -> float:
        """Calculate similarity between institution name and patent applicants."""
        applicants = patent_data.get('applicants', [])
        if not applicants:
            return 0.0
        
        context_clean = self.clean_institution_name(institution_context).lower()
        max_similarity = 0.0
        
        for applicant in applicants:
            app_name = applicant.get('name', '')
            if app_name:
                app_clean = self.clean_institution_name(app_name).lower()
                similarity = SequenceMatcher(None, context_clean, app_clean).ratio()
                max_similarity = max(max_similarity, similarity)
        
        return max_similarity
    
    def _calculate_author_overlap(
        self,
        publication: Dict[str, Any],
        patent_data: Dict[str, Any]
    ) -> float:
        """Calculate overlap between publication authors and patent inventors."""
        pub_authors = self._extract_authors(publication.get('authors', ''))
        patent_inventors = patent_data.get('inventors', [])
        
        if not pub_authors or not patent_inventors:
            return 0.0
        
        # Create normalized name sets
        pub_names = set()
        for author in pub_authors:
            clean_name = self._clean_author_name(author).lower()
            if clean_name:
                pub_names.add(clean_name)
        
        patent_names = set()
        for inventor in patent_inventors:
            inventor_name = inventor.get('name', '')
            if inventor_name:
                clean_name = self._clean_author_name(inventor_name).lower()
                if clean_name:
                    patent_names.add(clean_name)
        
        if not pub_names or not patent_names:
            return 0.0
        
        # Calculate Jaccard similarity with fuzzy matching
        matches = 0
        for pub_name in pub_names:
            for patent_name in patent_names:
                similarity = SequenceMatcher(None, pub_name, patent_name).ratio()
                if similarity > 0.8:  # High threshold for name matching
                    matches += 1
                    break
        
        return matches / max(len(pub_names), len(patent_names))
    
    def _calculate_year_proximity(
        self,
        publication: Dict[str, Any],
        patent_data: Dict[str, Any]
    ) -> float:
        """Calculate proximity between publication and patent years."""
        pub_year = self._extract_year(publication)
        patent_year = self._extract_patent_year(patent_data)
        
        if not pub_year or not patent_year:
            return 0.0
        
        year_diff = abs(pub_year - patent_year)
        
        # Patent typically comes after publication
        # Score higher for patents 0-5 years after publication
        if patent_year >= pub_year:
            if year_diff <= 1:
                return 1.0
            elif year_diff <= 3:
                return 0.8
            elif year_diff <= 5:
                return 0.6
            elif year_diff <= 10:
                return 0.3
            else:
                return 0.1
        else:
            # Patent before publication is less likely but possible
            if year_diff <= 1:
                return 0.7
            elif year_diff <= 3:
                return 0.4
            elif year_diff <= 5:
                return 0.2
            else:
                return 0.1
    
    def _calculate_subject_overlap(
        self,
        publication: Dict[str, Any],
        patent_data: Dict[str, Any]
    ) -> float:
        """Calculate subject/keyword overlap."""
        pub_keywords = self._extract_keywords(publication.get('keywords', ''))
        patent_title = patent_data.get('title', '')
        patent_abstract = patent_data.get('abstract', '')
        
        if not pub_keywords or not (patent_title or patent_abstract):
            return 0.0
        
        # Combine patent text
        patent_text = f"{patent_title} {patent_abstract}".lower()
        
        # Count keyword matches
        matches = 0
        for keyword in pub_keywords:
            if keyword.lower() in patent_text:
                matches += 1
        
        return matches / len(pub_keywords) if pub_keywords else 0.0
    
    def _calculate_title_similarity(
        self,
        publication: Dict[str, Any],
        patent_data: Dict[str, Any]
    ) -> float:
        """Calculate similarity between publication and patent titles."""
        pub_title = publication.get('title', '').lower()
        patent_title = patent_data.get('title', '').lower()
        
        if not pub_title or not patent_title:
            return 0.0
        
        return SequenceMatcher(None, pub_title, patent_title).ratio()
    
    def _extract_institutions(self, affiliations: str) -> List[str]:
        """Extract institution names from affiliation string."""
        if not affiliations:
            return []
        
        institutions = []
        for affil in affiliations.split(';'):
            cleaned = self.clean_institution_name(affil.strip())
            if cleaned and len(cleaned) > 5:
                institutions.append(cleaned)
        
        return institutions
    
    def _extract_authors(self, authors: str) -> List[str]:
        """Extract author names from authors string."""
        if not authors:
            return []
        
        author_list = []
        for author in authors.split(';'):
            cleaned = self._clean_author_name(author.strip())
            if cleaned:
                author_list.append(cleaned)
        
        return author_list
    
    def _extract_keywords(self, keywords: str) -> List[str]:
        """Extract keywords from keywords string."""
        if not keywords:
            return []
        
        keyword_list = []
        for keyword in keywords.split(';'):
            cleaned = keyword.strip()
            if cleaned and len(cleaned) > 2:
                keyword_list.append(cleaned)
        
        return keyword_list
    
    def _extract_year(self, publication: Dict[str, Any]) -> Optional[int]:
        """Extract publication year."""
        # Try different possible year fields
        for field in ['year', 'pubyear', 'publication_year']:
            if field in publication:
                try:
                    return int(publication[field])
                except (ValueError, TypeError):
                    continue
        
        return None
    
    def _extract_patent_year(self, patent_data: Dict[str, Any]) -> Optional[int]:
        """Extract patent publication year."""
        pub_date = patent_data.get('publication_date')
        if pub_date:
            try:
                # Handle various date formats
                if isinstance(pub_date, str):
                    # Extract year from date string (e.g., "2020-01-01" -> 2020)
                    year_match = re.search(r'\b(19|20)\d{2}\b', pub_date)
                    if year_match:
                        return int(year_match.group())
                elif isinstance(pub_date, int):
                    return pub_date
            except (ValueError, TypeError):
                pass
        
        return None
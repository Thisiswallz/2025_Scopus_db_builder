"""
Lens API Client

Zero-dependency HTTP client for the Lens Patent API with rate limiting,
caching, and comprehensive error handling.
"""

import json
import sqlite3
import time
import urllib.request
import urllib.parse
import urllib.error
import hashlib
import os
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass
class LensResponse:
    """Represents a response from the Lens API."""
    data: Dict[str, Any]
    total: int
    status_code: int
    from_cache: bool = False
    
    @property
    def results(self) -> List[Dict[str, Any]]:
        """Get the results list from the response data."""
        return self.data.get('data', [])


class LensAPIError(Exception):
    """Exception raised for Lens API-related errors."""
    
    def __init__(self, message: str, status_code: Optional[int] = None, response_data: Optional[str] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_data = response_data


class RateLimiter:
    """Simple rate limiter for API requests."""
    
    def __init__(self, requests_per_second: float):
        self.requests_per_second = requests_per_second
        self.min_interval = 1.0 / requests_per_second
        self.last_request_time = 0.0
    
    def wait_if_needed(self):
        """Wait if necessary to respect rate limits."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.min_interval:
            sleep_time = self.min_interval - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()


class ResponseCache:
    """SQLite-based cache for API responses."""
    
    def __init__(self, cache_path: str, ttl_days: int = 30):
        self.cache_path = cache_path
        self.ttl_days = ttl_days
        self._init_cache()
    
    def _init_cache(self):
        """Initialize the cache database."""
        cache_dir = Path(self.cache_path).parent
        cache_dir.mkdir(parents=True, exist_ok=True)
        
        with sqlite3.connect(self.cache_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS lens_cache (
                    key TEXT PRIMARY KEY,
                    response_data TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_expires_at ON lens_cache(expires_at)")
    
    def _make_key(self, url: str, params: Dict[str, Any]) -> str:
        """Generate cache key from URL and parameters."""
        cache_data = f"{url}:{json.dumps(params, sort_keys=True)}"
        return hashlib.md5(cache_data.encode()).hexdigest()
    
    def get(self, url: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Get cached response if available and not expired."""
        key = self._make_key(url, params)
        
        with sqlite3.connect(self.cache_path) as conn:
            cursor = conn.execute(
                "SELECT response_data FROM lens_cache WHERE key = ? AND expires_at > ?",
                (key, datetime.now().isoformat())
            )
            row = cursor.fetchone()
            
            if row:
                return json.loads(row[0])
        
        return None
    
    def set(self, url: str, params: Dict[str, Any], response_data: Dict[str, Any]):
        """Cache response data."""
        key = self._make_key(url, params)
        expires_at = datetime.now() + timedelta(days=self.ttl_days)
        
        with sqlite3.connect(self.cache_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO lens_cache (key, response_data, expires_at) VALUES (?, ?, ?)",
                (key, json.dumps(response_data), expires_at.isoformat())
            )
    
    def cleanup_expired(self):
        """Remove expired cache entries."""
        with sqlite3.connect(self.cache_path) as conn:
            cursor = conn.execute(
                "DELETE FROM lens_cache WHERE expires_at <= ?",
                (datetime.now().isoformat(),)
            )
            return cursor.rowcount
    
    def get_stats(self) -> Dict[str, int]:
        """Get cache statistics."""
        with sqlite3.connect(self.cache_path) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM lens_cache")
            total = cursor.fetchone()[0]
            
            cursor = conn.execute(
                "SELECT COUNT(*) FROM lens_cache WHERE expires_at > ?",
                (datetime.now().isoformat(),)
            )
            valid = cursor.fetchone()[0]
            
            return {'total_entries': total, 'valid_entries': valid, 'expired_entries': total - valid}


class LensClient:
    """
    HTTP client for the Lens Patent API.
    
    Features:
    - Token-based authentication
    - Rate limiting with configurable limits
    - Response caching to avoid duplicate requests
    - Automatic retry with exponential backoff
    - Comprehensive error handling
    """
    
    BASE_URL = "https://api.lens.org"
    PATENT_SEARCH_ENDPOINT = "/patent/search"
    
    def __init__(
        self,
        api_token: str,
        rate_limit: float = 10.0,
        timeout: int = 30,
        retry_attempts: int = 3,
        cache_path: Optional[str] = None,
        cache_ttl_days: int = 30,
        verbose: bool = False
    ):
        """
        Initialize Lens API client.
        
        Args:
            api_token: Lens API authentication token
            rate_limit: Maximum requests per second
            timeout: Request timeout in seconds
            retry_attempts: Number of retry attempts for failed requests
            cache_path: Path to cache database (defaults to temp directory)
            cache_ttl_days: Cache time-to-live in days
            verbose: Enable verbose logging
        """
        self.api_token = api_token
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.verbose = verbose
        
        # Initialize rate limiter
        self.rate_limiter = RateLimiter(rate_limit)
        
        # Initialize cache
        if cache_path is None:
            cache_dir = Path.home() / ".scopus_db_cache"
            cache_path = str(cache_dir / "lens_cache.db")
        
        self.cache = ResponseCache(cache_path, cache_ttl_days)
        
        # Request statistics
        self.stats = {
            'requests_made': 0,
            'cache_hits': 0,
            'errors': 0,
            'total_patents_retrieved': 0
        }
        
        if self.verbose:
            print(f"🔬 Lens API client initialized (rate limit: {rate_limit}/sec)")
    
    def search_patents(
        self,
        query: str,
        size: int = 50,
        from_: int = 0,
        include_fields: Optional[List[str]] = None,
        sort_by: Optional[str] = None
    ) -> LensResponse:
        """
        Search patents using the Lens API.
        
        Args:
            query: Lucene-style search query
            size: Number of results to return (max 1000)
            from_: Starting offset for pagination
            include_fields: Specific fields to include in response
            sort_by: Sort field (e.g., 'publication_date')
        
        Returns:
            LensResponse object containing search results
        
        Raises:
            LensAPIError: If API request fails
        """
        # Validate parameters
        if not query.strip():
            raise LensAPIError("Query cannot be empty")
        
        if size <= 0 or size > 1000:
            raise LensAPIError("Size must be between 1 and 1000")
        
        if from_ < 0:
            raise LensAPIError("From offset must be non-negative")
        
        # Build request body
        request_body = {
            "query": {"bool": {"must": [{"query_string": {"query": query}}]}},
            "size": size,
            "from": from_
        }
        
        # Add sorting if specified
        if sort_by:
            request_body["sort"] = [{sort_by: {"order": "desc"}}]
        
        # Add include fields if specified
        if include_fields:
            request_body["include"] = include_fields
        
        # Check cache first
        cached_response = self.cache.get(self.PATENT_SEARCH_ENDPOINT, request_body)
        if cached_response:
            self.stats['cache_hits'] += 1
            if self.verbose:
                print(f"📄 Cache hit for query: {query[:50]}...")
            return LensResponse(
                data=cached_response,
                total=cached_response.get('total', 0),
                status_code=200,
                from_cache=True
            )
        
        # Make API request
        response_data = self._make_request(self.PATENT_SEARCH_ENDPOINT, request_body)
        
        # Cache successful response
        self.cache.set(self.PATENT_SEARCH_ENDPOINT, request_body, response_data)
        
        # Update statistics
        results_count = len(response_data.get('data', []))
        self.stats['total_patents_retrieved'] += results_count
        
        if self.verbose:
            print(f"🔍 Found {results_count} patents for query: {query[:50]}...")
        
        return LensResponse(
            data=response_data,
            total=response_data.get('total', 0),
            status_code=200,
            from_cache=False
        )
    
    def search_patents_by_inventor(
        self,
        inventor_name: str,
        size: int = 50,
        from_: int = 0
    ) -> LensResponse:
        """Search patents by inventor name."""
        query = f'inventor.name:"{inventor_name}"'
        return self.search_patents(query, size, from_)
    
    def search_patents_by_applicant(
        self,
        applicant_name: str,
        size: int = 50,
        from_: int = 0
    ) -> LensResponse:
        """Search patents by applicant/assignee name."""
        query = f'applicant.name:"{applicant_name}"'
        return self.search_patents(query, size, from_)
    
    def search_patents_by_title_keywords(
        self,
        keywords: str,
        size: int = 50,
        from_: int = 0
    ) -> LensResponse:
        """Search patents by title keywords."""
        query = f'title:({keywords})'
        return self.search_patents(query, size, from_)
    
    def _make_request(self, endpoint: str, request_body: Dict[str, Any]) -> Dict[str, Any]:
        """
        Make HTTP request to Lens API with retry logic.
        
        Args:
            endpoint: API endpoint path
            request_body: JSON request body
        
        Returns:
            Response data as dictionary
        
        Raises:
            LensAPIError: If request fails after all retries
        """
        url = self.BASE_URL + endpoint
        headers = {
            'Authorization': f'Bearer {self.api_token}',
            'Content-Type': 'application/json',
            'User-Agent': 'ScopusDB-LensEnricher/1.0'
        }
        
        data = json.dumps(request_body).encode('utf-8')
        
        last_error = None
        
        for attempt in range(self.retry_attempts):
            try:
                # Rate limiting
                self.rate_limiter.wait_if_needed()
                
                # Create request
                request = urllib.request.Request(url, data=data, headers=headers)
                
                # Make request
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    response_data = response.read().decode('utf-8')
                    self.stats['requests_made'] += 1
                    
                    if self.verbose and attempt > 0:
                        print(f"✅ Request succeeded on attempt {attempt + 1}")
                    
                    return json.loads(response_data)
            
            except urllib.error.HTTPError as e:
                last_error = e
                self.stats['errors'] += 1
                
                # Read error response
                try:
                    error_data = e.read().decode('utf-8')
                except:
                    error_data = "Unable to read error response"
                
                if e.code == 401:
                    raise LensAPIError("Authentication failed - check API token", e.code, error_data)
                elif e.code == 429:
                    # Rate limit exceeded - wait longer
                    wait_time = min(2 ** attempt, 60)  # Exponential backoff, max 60s
                    if self.verbose:
                        print(f"⚠️  Rate limit hit, waiting {wait_time}s (attempt {attempt + 1})")
                    time.sleep(wait_time)
                    continue
                elif e.code >= 500:
                    # Server error - retry
                    if attempt < self.retry_attempts - 1:
                        wait_time = min(2 ** attempt, 30)
                        if self.verbose:
                            print(f"⚠️  Server error {e.code}, retrying in {wait_time}s (attempt {attempt + 1})")
                        time.sleep(wait_time)
                        continue
                
                # Client error or final attempt
                raise LensAPIError(f"HTTP {e.code}: {error_data}", e.code, error_data)
            
            except urllib.error.URLError as e:
                last_error = e
                self.stats['errors'] += 1
                
                if attempt < self.retry_attempts - 1:
                    wait_time = min(2 ** attempt, 30)
                    if self.verbose:
                        print(f"⚠️  Network error, retrying in {wait_time}s (attempt {attempt + 1}): {e}")
                    time.sleep(wait_time)
                    continue
                
                raise LensAPIError(f"Network error: {e}", response_data=str(e))
            
            except json.JSONDecodeError as e:
                last_error = e
                self.stats['errors'] += 1
                raise LensAPIError(f"Invalid JSON response: {e}")
            
            except Exception as e:
                last_error = e
                self.stats['errors'] += 1
                
                if attempt < self.retry_attempts - 1:
                    wait_time = min(2 ** attempt, 30)
                    if self.verbose:
                        print(f"⚠️  Unexpected error, retrying in {wait_time}s (attempt {attempt + 1}): {e}")
                    time.sleep(wait_time)
                    continue
                
                raise LensAPIError(f"Unexpected error: {e}")
        
        # If we get here, all retries failed
        raise LensAPIError(f"Request failed after {self.retry_attempts} attempts: {last_error}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get client statistics."""
        cache_stats = self.cache.get_stats()
        
        return {
            **self.stats,
            **cache_stats,
            'cache_hit_rate': self.stats['cache_hits'] / max(self.stats['requests_made'] + self.stats['cache_hits'], 1)
        }
    
    def cleanup_cache(self) -> int:
        """Clean up expired cache entries."""
        return self.cache.cleanup_expired()
    
    def test_connection(self) -> bool:
        """Test API connection with a simple query."""
        try:
            # Simple test query
            response = self.search_patents("title:test", size=1)
            return True
        except LensAPIError:
            return False
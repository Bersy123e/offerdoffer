import json
import sqlite3
import time
import logging
from typing import Dict, List, Optional, Any, Union
import pickle
from logger import setup_logger

logger = setup_logger()

class QueryCache:
    def __init__(self, db_path: str = "cache.db", expire_time: int = 86400):
        """
        Initialize QueryCache with database path and expiration time.
        
        Args:
            db_path: Path to SQLite database
            expire_time: Cache expiration time in seconds (default: 1 day)
        """
        self.db_path = db_path
        self.expire_time = expire_time
        self.hit_count = 0
        self.miss_count = 0
        self._initialize_db()
    
    def _initialize_db(self):
        """Initialize SQLite database with cache table."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Create cache table
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS query_cache (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    query TEXT UNIQUE,
                    result BLOB,
                    timestamp INTEGER
                )
                ''')
                
                # Create index for faster lookups
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_query ON query_cache(query)')
                
                conn.commit()
            logger.info("Cache database initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing cache database: {str(e)}")
            raise
    
    def get(self, query: str) -> Optional[List[Dict]]:
        """
        Get cached result for a query.
        
        Args:
            query: Query string
            
        Returns:
            Cached result or None if not found or expired
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT result, timestamp FROM query_cache WHERE query = ?",
                    (query,)
                )
                
                row = cursor.fetchone()
                
                if row:
                    result_blob, timestamp = row
                    
                    # Check if expired
                    if time.time() - timestamp > self.expire_time:
                        logger.info(f"Cache entry expired for query: {query}")
                        self._remove(query)
                        self.miss_count += 1
                        return None
                    
                    # Deserialize result
                    result = pickle.loads(result_blob)
                    
                    self.hit_count += 1
                    logger.info(f"Cache hit for query: {query}")
                    return result
                else:
                    self.miss_count += 1
                    logger.info(f"Cache miss for query: {query}")
                    return None
        
        except Exception as e:
            logger.error(f"Error getting from cache: {str(e)}")
            return None
    
    def set(self, query: str, result: List[Dict]) -> bool:
        """
        Set cache entry for a query.
        
        Args:
            query: Query string
            result: Query result to cache
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Serialize result
            result_blob = pickle.dumps(result)
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR REPLACE INTO query_cache (query, result, timestamp) VALUES (?, ?, ?)",
                    (query, result_blob, int(time.time()))
                )
                
                conn.commit()
            logger.info(f"Cached result for query: {query}")
            return True
            
        except Exception as e:
            logger.error(f"Error setting cache: {str(e)}")
            return False
    
    def _remove(self, query: str) -> bool:
        """
        Remove cache entry for a query.
        
        Args:
            query: Query string
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM query_cache WHERE query = ?", (query,))
                conn.commit()
            logger.info(f"Removed cache entry for query: {query}")
            return True
            
        except Exception as e:
            logger.error(f"Error removing from cache: {str(e)}")
            return False
    
    def clear(self) -> bool:
        """
        Clear all cache entries.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM query_cache")
                conn.commit()
            logger.info("Cache cleared")
            return True
            
        except Exception as e:
            logger.error(f"Error clearing cache: {str(e)}")
            return False
    
    def get_stats(self) -> Dict[str, Union[int, float]]:
        """
        Get cache statistics.
        
        Returns:
            Dictionary with cache statistics
        """
        total_requests = self.hit_count + self.miss_count
        hit_rate = self.hit_count / total_requests if total_requests > 0 else 0
        
        # Get cache size
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM query_cache")
            cache_size = cursor.fetchone()[0]
        
        return {
            "hit_count": self.hit_count,
            "miss_count": self.miss_count,
            "hit_rate": hit_rate * 100,  # As percentage
            "cache_size": cache_size
        }
    
    def close(self):
        """Close database connection."""
        logger.info("Cache database connection closed") 
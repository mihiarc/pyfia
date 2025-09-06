"""
Caching support for pyFIA lazy evaluation.

This module provides comprehensive caching functionality including memory
and disk caching for lazy frames, query plan caching, and cache decorators
for common operations.
"""

import hashlib
import json
import pickle
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
import weakref
import functools
import warnings

import polars as pl
from pydantic import BaseModel, Field, ConfigDict


@dataclass
class CacheKey:
    """
    Represents a cache key for lazy operations.
    
    Cache keys are generated from operation parameters and data characteristics
    to ensure consistent and unique identification of cached results.
    """
    
    operation: str
    params: Dict[str, Any]
    data_hash: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)
    
    def __post_init__(self):
        """Ensure params are serializable."""
        # Convert non-serializable params to strings
        self.params = self._make_serializable(self.params)
    
    def _make_serializable(self, obj: Any) -> Any:
        """Convert object to serializable form."""
        if isinstance(obj, (str, int, float, bool, type(None))):
            return obj
        elif isinstance(obj, (list, tuple)):
            return [self._make_serializable(item) for item in obj]
        elif isinstance(obj, dict):
            return {k: self._make_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, pl.Expr):
            return f"expr:{str(obj)}"
        elif isinstance(obj, (pl.DataFrame, pl.LazyFrame)):
            # Use schema for frames
            return f"frame:{obj.schema}"
        else:
            return str(obj)
    
    @property
    def key(self) -> str:
        """Generate unique cache key."""
        content = {
            "operation": self.operation,
            "params": self.params,
            "data_hash": self.data_hash
        }
        key_str = json.dumps(content, sort_keys=True)
        return hashlib.sha256(key_str.encode()).hexdigest()
    
    def with_data_hash(self, data: Union[pl.DataFrame, pl.LazyFrame]) -> 'CacheKey':
        """
        Create new cache key with data hash.
        
        Parameters
        ----------
        data : Union[pl.DataFrame, pl.LazyFrame]
            Data to hash
            
        Returns
        -------
        CacheKey
            New cache key with data hash
        """
        # Generate data hash from schema and shape
        if isinstance(data, pl.LazyFrame):
            # For lazy frames, use schema
            data_info = str(data.schema)
        else:
            # For dataframes, include shape
            data_info = f"{data.schema}:{data.shape}"
        
        data_hash = hashlib.md5(data_info.encode()).hexdigest()[:16]
        
        return CacheKey(
            operation=self.operation,
            params=self.params,
            data_hash=data_hash,
            timestamp=self.timestamp
        )


@dataclass
class CacheEntry:
    """Represents a cached value with metadata."""
    
    key: str
    value: Any
    size_bytes: int
    created_at: datetime
    last_accessed: datetime
    access_count: int = 0
    ttl_seconds: Optional[int] = None
    
    @property
    def is_expired(self) -> bool:
        """Check if cache entry has expired."""
        if self.ttl_seconds is None:
            return False
        age = (datetime.now() - self.created_at).total_seconds()
        return age > self.ttl_seconds
    
    def touch(self):
        """Update last accessed time and increment access count."""
        self.last_accessed = datetime.now()
        self.access_count += 1


class MemoryCache:
    """
    In-memory cache with LRU eviction and size limits.
    
    This cache stores results in memory with configurable size limits
    and least-recently-used (LRU) eviction policy.
    """
    
    def __init__(self, 
                 max_size_mb: float = 1024,
                 max_entries: int = 1000,
                 ttl_seconds: Optional[int] = 3600):
        """
        Initialize memory cache.
        
        Parameters
        ----------
        max_size_mb : float
            Maximum cache size in megabytes
        max_entries : int
            Maximum number of cache entries
        ttl_seconds : Optional[int]
            Default time-to-live in seconds
        """
        self.max_size_bytes = int(max_size_mb * 1024 * 1024)
        self.max_entries = max_entries
        self.default_ttl = ttl_seconds
        
        self._cache: Dict[str, CacheEntry] = {}
        self._current_size = 0
        self._stats = {
            "hits": 0,
            "misses": 0,
            "evictions": 0,
            "expired": 0
        }
    
    def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache.
        
        Parameters
        ----------
        key : str
            Cache key
            
        Returns
        -------
        Optional[Any]
            Cached value or None if not found/expired
        """
        entry = self._cache.get(key)
        
        if entry is None:
            self._stats["misses"] += 1
            return None
        
        if entry.is_expired:
            self._stats["expired"] += 1
            self.evict(key)
            return None
        
        entry.touch()
        self._stats["hits"] += 1
        return entry.value
    
    def put(self, key: str, value: Any, 
            size_bytes: Optional[int] = None,
            ttl_seconds: Optional[int] = None):
        """
        Put value in cache.
        
        Parameters
        ----------
        key : str
            Cache key
        value : Any
            Value to cache
        size_bytes : Optional[int]
            Size of value in bytes
        ttl_seconds : Optional[int]
            Time-to-live for this entry
        """
        # Estimate size if not provided
        if size_bytes is None:
            size_bytes = self._estimate_size(value)
        
        # Check if we need to evict entries
        while (self._current_size + size_bytes > self.max_size_bytes or 
               len(self._cache) >= self.max_entries):
            if not self._evict_lru():
                break
        
        # Create cache entry
        entry = CacheEntry(
            key=key,
            value=value,
            size_bytes=size_bytes,
            created_at=datetime.now(),
            last_accessed=datetime.now(),
            ttl_seconds=ttl_seconds or self.default_ttl
        )
        
        # Update cache
        if key in self._cache:
            self._current_size -= self._cache[key].size_bytes
        
        self._cache[key] = entry
        self._current_size += size_bytes
    
    def evict(self, key: str) -> bool:
        """
        Evict entry from cache.
        
        Parameters
        ----------
        key : str
            Key to evict
            
        Returns
        -------
        bool
            True if entry was evicted
        """
        entry = self._cache.pop(key, None)
        if entry:
            self._current_size -= entry.size_bytes
            self._stats["evictions"] += 1
            return True
        return False
    
    def _evict_lru(self) -> bool:
        """Evict least recently used entry."""
        if not self._cache:
            return False
        
        # Find LRU entry
        lru_key = min(self._cache.keys(), 
                     key=lambda k: self._cache[k].last_accessed)
        
        return self.evict(lru_key)
    
    def _estimate_size(self, value: Any) -> int:
        """Estimate size of value in bytes."""
        if isinstance(value, pl.DataFrame):
            # Estimate based on shape and dtypes
            return value.estimated_size()
        elif isinstance(value, pl.LazyFrame):
            # Lazy frames are small - just the query plan
            return 1024  # 1KB estimate
        else:
            # Use pickle size for other objects
            try:
                return len(pickle.dumps(value))
            except:
                return 1024  # Default 1KB
    
    def clear(self):
        """Clear all cache entries."""
        self._cache.clear()
        self._current_size = 0
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total_requests = self._stats["hits"] + self._stats["misses"]
        hit_rate = self._stats["hits"] / total_requests if total_requests > 0 else 0
        
        return {
            **self._stats,
            "entries": len(self._cache),
            "size_mb": self._current_size / (1024 * 1024),
            "hit_rate": hit_rate
        }


class DiskCache:
    """
    Disk-based cache for larger results.
    
    This cache stores results on disk using Parquet format for DataFrames
    and pickle for other objects.
    """
    
    def __init__(self, 
                 cache_dir: Optional[Path] = None,
                 max_size_gb: float = 10.0,
                 ttl_days: int = 7):
        """
        Initialize disk cache.
        
        Parameters
        ----------
        cache_dir : Optional[Path]
            Directory for cache files
        max_size_gb : float
            Maximum cache size in gigabytes
        ttl_days : int
            Time-to-live in days
        """
        if cache_dir is None:
            cache_dir = Path(tempfile.gettempdir()) / "pyfia_cache"
        
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        self.max_size_bytes = int(max_size_gb * 1024 * 1024 * 1024)
        self.ttl_seconds = ttl_days * 24 * 3600
        
        self._index_file = self.cache_dir / "cache_index.json"
        self._index = self._load_index()
        
        # Clean expired entries on startup
        self._clean_expired()
    
    def _load_index(self) -> Dict[str, Dict[str, Any]]:
        """Load cache index from disk."""
        if self._index_file.exists():
            try:
                with open(self._index_file, 'r') as f:
                    return json.load(f)
            except:
                return {}
        return {}
    
    def _save_index(self):
        """Save cache index to disk."""
        with open(self._index_file, 'w') as f:
            json.dump(self._index, f, indent=2, default=str)
    
    def get(self, key: str) -> Optional[Any]:
        """
        Get value from disk cache.
        
        Parameters
        ----------
        key : str
            Cache key
            
        Returns
        -------
        Optional[Any]
            Cached value or None if not found
        """
        entry_info = self._index.get(key)
        if not entry_info:
            return None
        
        # Check expiration
        created_at = datetime.fromisoformat(entry_info["created_at"])
        if (datetime.now() - created_at).total_seconds() > self.ttl_seconds:
            self.evict(key)
            return None
        
        # Load from disk
        cache_file = self.cache_dir / entry_info["filename"]
        if not cache_file.exists():
            self.evict(key)
            return None
        
        try:
            if entry_info["type"] == "dataframe":
                return pl.read_parquet(cache_file)
            else:
                with open(cache_file, 'rb') as f:
                    return pickle.load(f)
        except Exception as e:
            warnings.warn(f"Failed to load cached file {cache_file}: {e}")
            self.evict(key)
            return None
    
    def put(self, key: str, value: Any):
        """
        Put value in disk cache.
        
        Parameters
        ----------
        key : str
            Cache key
        value : Any
            Value to cache
        """
        # Generate filename
        filename = f"{key[:8]}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Determine type and save
        if isinstance(value, pl.DataFrame):
            cache_file = self.cache_dir / f"{filename}.parquet"
            value.write_parquet(cache_file, compression="zstd")
            value_type = "dataframe"
        else:
            cache_file = self.cache_dir / f"{filename}.pkl"
            with open(cache_file, 'wb') as f:
                pickle.dump(value, f)
            value_type = "pickle"
        
        # Update index
        self._index[key] = {
            "filename": cache_file.name,
            "type": value_type,
            "size_bytes": cache_file.stat().st_size,
            "created_at": datetime.now().isoformat()
        }
        self._save_index()
        
        # Check size limits
        self._enforce_size_limit()
    
    def evict(self, key: str) -> bool:
        """
        Evict entry from disk cache.
        
        Parameters
        ----------
        key : str
            Key to evict
            
        Returns
        -------
        bool
            True if entry was evicted
        """
        entry_info = self._index.pop(key, None)
        if entry_info:
            cache_file = self.cache_dir / entry_info["filename"]
            if cache_file.exists():
                cache_file.unlink()
            self._save_index()
            return True
        return False
    
    def _clean_expired(self):
        """Remove expired entries from cache."""
        expired_keys = []
        
        for key, entry_info in self._index.items():
            created_at = datetime.fromisoformat(entry_info["created_at"])
            if (datetime.now() - created_at).total_seconds() > self.ttl_seconds:
                expired_keys.append(key)
        
        for key in expired_keys:
            self.evict(key)
    
    def _enforce_size_limit(self):
        """Enforce size limit by removing oldest entries."""
        total_size = sum(e["size_bytes"] for e in self._index.values())
        
        if total_size <= self.max_size_bytes:
            return
        
        # Sort by creation time
        sorted_entries = sorted(
            self._index.items(),
            key=lambda x: x[1]["created_at"]
        )
        
        # Remove oldest until under limit
        for key, entry_info in sorted_entries:
            if total_size <= self.max_size_bytes:
                break
            
            total_size -= entry_info["size_bytes"]
            self.evict(key)
    
    def clear(self):
        """Clear all cache entries."""
        for key in list(self._index.keys()):
            self.evict(key)
        self._index.clear()
        self._save_index()


class LazyFrameCache:
    """
    Combined memory and disk cache for lazy frames and results.
    
    This cache provides a two-tier caching system with fast memory cache
    backed by larger disk cache.
    """
    
    def __init__(self,
                 memory_size_mb: float = 512,
                 disk_size_gb: float = 10,
                 cache_dir: Optional[Path] = None):
        """
        Initialize lazy frame cache.
        
        Parameters
        ----------
        memory_size_mb : float
            Memory cache size in MB
        disk_size_gb : float
            Disk cache size in GB
        cache_dir : Optional[Path]
            Directory for disk cache
        """
        self.memory_cache = MemoryCache(max_size_mb=memory_size_mb)
        self.disk_cache = DiskCache(
            cache_dir=cache_dir,
            max_size_gb=disk_size_gb
        )
        
        # Query plan cache
        self._query_plan_cache: Dict[str, str] = {}
        self._plan_cache_limit = 1000
    
    def get(self, cache_key: CacheKey) -> Optional[Any]:
        """
        Get value from cache (memory first, then disk).
        
        Parameters
        ----------
        cache_key : CacheKey
            Cache key object
            
        Returns
        -------
        Optional[Any]
            Cached value or None
        """
        key = cache_key.key
        
        # Try memory cache first
        value = self.memory_cache.get(key)
        if value is not None:
            return value
        
        # Try disk cache
        value = self.disk_cache.get(key)
        if value is not None:
            # Promote to memory cache
            self.memory_cache.put(key, value)
            return value
        
        return None
    
    def put(self, cache_key: CacheKey, value: Any, 
            persist_to_disk: bool = True):
        """
        Put value in cache.
        
        Parameters
        ----------
        cache_key : CacheKey
            Cache key object
        value : Any
            Value to cache
        persist_to_disk : bool
            Whether to persist to disk cache
        """
        key = cache_key.key
        
        # Always put in memory cache
        self.memory_cache.put(key, value)
        
        # Optionally persist to disk
        if persist_to_disk and isinstance(value, (pl.DataFrame, dict, list)):
            self.disk_cache.put(key, value)
    
    def cache_query_plan(self, lazy_frame: pl.LazyFrame) -> str:
        """
        Cache and return query plan for a lazy frame.
        
        Parameters
        ----------
        lazy_frame : pl.LazyFrame
            Lazy frame to cache plan for
            
        Returns
        -------
        str
            Query plan string
        """
        # Generate plan key from frame schema
        plan_key = hashlib.md5(
            str(lazy_frame.schema).encode()
        ).hexdigest()[:16]
        
        if plan_key in self._query_plan_cache:
            return self._query_plan_cache[plan_key]
        
        # Generate and cache plan
        plan = lazy_frame.explain(optimized=True)
        
        # Limit cache size
        if len(self._query_plan_cache) >= self._plan_cache_limit:
            # Remove oldest entry
            oldest = next(iter(self._query_plan_cache))
            del self._query_plan_cache[oldest]
        
        self._query_plan_cache[plan_key] = plan
        return plan
    
    def clear(self, memory_only: bool = False):
        """
        Clear cache.
        
        Parameters
        ----------
        memory_only : bool
            If True, only clear memory cache
        """
        self.memory_cache.clear()
        self._query_plan_cache.clear()
        
        if not memory_only:
            self.disk_cache.clear()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get combined cache statistics."""
        return {
            "memory": self.memory_cache.get_stats(),
            "disk": {
                "entries": len(self.disk_cache._index),
                "size_gb": sum(
                    e["size_bytes"] for e in self.disk_cache._index.values()
                ) / (1024 ** 3)
            },
            "query_plans": len(self._query_plan_cache)
        }


class QueryPlanCache:
    """
    Specialized cache for Polars query plans.
    
    This cache stores optimized query plans to avoid recomputation
    of optimization passes.
    """
    
    def __init__(self, max_entries: int = 1000):
        """
        Initialize query plan cache.
        
        Parameters
        ----------
        max_entries : int
            Maximum number of cached plans
        """
        self.max_entries = max_entries
        self._cache: Dict[str, Tuple[str, datetime]] = {}
        self._stats = {"hits": 0, "misses": 0}
    
    def get_plan(self, lazy_frame: pl.LazyFrame) -> Optional[str]:
        """
        Get cached query plan.
        
        Parameters
        ----------
        lazy_frame : pl.LazyFrame
            Lazy frame to get plan for
            
        Returns
        -------
        Optional[str]
            Cached plan or None
        """
        key = self._generate_key(lazy_frame)
        
        if key in self._cache:
            self._stats["hits"] += 1
            plan, _ = self._cache[key]
            # Move to end (LRU)
            del self._cache[key]
            self._cache[key] = (plan, datetime.now())
            return plan
        
        self._stats["misses"] += 1
        return None
    
    def put_plan(self, lazy_frame: pl.LazyFrame, plan: str):
        """
        Cache query plan.
        
        Parameters
        ----------
        lazy_frame : pl.LazyFrame
            Lazy frame the plan belongs to
        plan : str
            Query plan string
        """
        key = self._generate_key(lazy_frame)
        
        # Enforce size limit
        if len(self._cache) >= self.max_entries:
            # Remove oldest
            oldest = next(iter(self._cache))
            del self._cache[oldest]
        
        self._cache[key] = (plan, datetime.now())
    
    def _generate_key(self, lazy_frame: pl.LazyFrame) -> str:
        """Generate cache key from lazy frame."""
        # Use schema and basic query structure
        schema_str = str(lazy_frame.schema)
        # Try to get a simple representation of the query
        try:
            query_str = str(lazy_frame.explain(optimized=False))[:200]
        except:
            query_str = ""
        
        content = f"{schema_str}:{query_str}"
        return hashlib.md5(content.encode()).hexdigest()
    
    def clear(self):
        """Clear all cached plans."""
        self._cache.clear()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total = self._stats["hits"] + self._stats["misses"]
        hit_rate = self._stats["hits"] / total if total > 0 else 0
        
        return {
            **self._stats,
            "entries": len(self._cache),
            "hit_rate": hit_rate
        }


# Cache decorators

def cached_operation(cache_params: Optional[List[str]] = None,
                    ttl_seconds: Optional[int] = 3600,
                    persist_to_disk: bool = True):
    """
    Decorator for caching operation results.
    
    Parameters
    ----------
    cache_params : Optional[List[str]]
        Parameter names to include in cache key
    ttl_seconds : Optional[int]
        Time-to-live for cache entries
    persist_to_disk : bool
        Whether to persist to disk cache
        
    Returns
    -------
    Callable
        Decorated function
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            # Check if caching is enabled
            if not (hasattr(self, '_cache') and 
                   getattr(self, '_cache_enabled', True)):
                return func(self, *args, **kwargs)
            
            # Build cache key
            import inspect
            sig = inspect.signature(func)
            bound_args = sig.bind(self, *args, **kwargs)
            bound_args.apply_defaults()
            
            # Extract cache parameters
            params = {}
            if cache_params:
                for param in cache_params:
                    if param in bound_args.arguments:
                        params[param] = bound_args.arguments[param]
            
            cache_key = CacheKey(
                operation=func.__name__,
                params=params
            )
            
            # Check cache
            cached = self._cache.get(cache_key.key)
            if cached is not None:
                return cached
            
            # Execute function
            result = func(self, *args, **kwargs)
            
            # Cache result (remove persist_to_disk as it's not supported)
            self._cache.put(
                cache_key.key, 
                result
            )
            
            return result
        
        return wrapper
    return decorator


def cached_lazy_operation(cache_params: Optional[List[str]] = None):
    """
    Decorator specifically for lazy frame operations.
    
    This decorator handles caching of lazy frame operations, storing
    the query plan rather than the actual data.
    
    Parameters
    ----------
    cache_params : Optional[List[str]]
        Parameter names to include in cache key
        
    Returns
    -------
    Callable
        Decorated function
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            # Execute function to get lazy frame
            result = func(self, *args, **kwargs)
            
            # If result is a lazy frame and caching is enabled
            if (isinstance(result, pl.LazyFrame) and
                hasattr(self, '_query_plan_cache') and
                getattr(self, '_cache_query_plans', True)):
                
                # Cache the query plan
                self._query_plan_cache.put_plan(
                    result,
                    result.explain(optimized=True)
                )
            
            return result
        
        return wrapper
    return decorator


class CacheConfig(BaseModel):
    """Configuration for caching behavior."""
    
    model_config = ConfigDict(
        validate_assignment=True,
        extra="forbid"
    )
    
    # Memory cache settings
    memory_cache_enabled: bool = Field(
        default=True,
        description="Enable memory caching"
    )
    memory_cache_size_mb: float = Field(
        default=512,
        ge=0,
        description="Memory cache size in MB"
    )
    memory_cache_ttl_seconds: Optional[int] = Field(
        default=3600,
        ge=0,
        description="Memory cache TTL in seconds"
    )
    
    # Disk cache settings
    disk_cache_enabled: bool = Field(
        default=True,
        description="Enable disk caching"
    )
    disk_cache_size_gb: float = Field(
        default=10,
        ge=0,
        description="Disk cache size in GB"
    )
    disk_cache_ttl_days: int = Field(
        default=7,
        ge=1,
        description="Disk cache TTL in days"
    )
    disk_cache_dir: Optional[Path] = Field(
        default=None,
        description="Directory for disk cache"
    )
    
    # Query plan cache settings
    cache_query_plans: bool = Field(
        default=True,
        description="Cache optimized query plans"
    )
    query_plan_cache_size: int = Field(
        default=1000,
        ge=100,
        description="Maximum number of cached query plans"
    )
    
    # Cache behavior
    cache_operations: List[str] = Field(
        default_factory=lambda: [
            "load_table", "get_trees", "get_conditions",
            "aggregate", "join", "filter"
        ],
        description="Operations to cache"
    )
    persist_aggregations: bool = Field(
        default=True,
        description="Persist aggregation results to disk"
    )
    
    def create_cache(self) -> LazyFrameCache:
        """Create cache instance from configuration."""
        return LazyFrameCache(
            memory_size_mb=self.memory_cache_size_mb,
            disk_size_gb=self.disk_cache_size_gb,
            cache_dir=self.disk_cache_dir
        )
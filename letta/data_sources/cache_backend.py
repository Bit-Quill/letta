"""
Abstract base class for cache backend implementations.

This module defines the `CacheBackend` interface that all cache implementations
(e.g., Redis, Valkey) must adhere to. It ensures consistent behavior and
API across different backends.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Set, Union




class CacheBackend(ABC):
    """Abstract base class for cache backend implementations."""

    # Connection lifecycle
    @abstractmethod
    async def get_client(self) -> Any:
        """Get or create client instance."""
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close connection and cleanup resources."""
        pass

    @abstractmethod
    async def __aenter__(self):
        """Async context manager entry."""
        pass

    @abstractmethod
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        pass

    # Health checks
    @abstractmethod
    async def ping(self) -> bool:
        """Check if backend is accessible."""
        pass

    @abstractmethod
    async def wait_for_ready(self, timeout: int = 30, interval: float = 0.5) -> None:
        """Wait for backend to be ready."""
        pass

    # Basic key-value operations
    @abstractmethod
    async def get(self, key: str, default: Any = None) -> Any:
        """Get value by key."""
        pass

    @abstractmethod
    async def set(
        self,
        key: str,
        value: Union[str, int, float],
        ex: Optional[int] = None,
        px: Optional[int] = None,
        nx: bool = False,
        xx: bool = False,
    ) -> bool:
        """Set key-value with options."""
        pass

    @abstractmethod
    async def delete(self, *keys: str) -> int:
        """Delete one or more keys."""
        pass

    @abstractmethod
    async def exists(self, *keys: str) -> int:
        """Check if keys exist."""
        pass

    # Set operations
    @abstractmethod
    async def sadd(self, key: str, *members: Union[str, int, float]) -> int:
        """Add members to set."""
        pass

    @abstractmethod
    async def smembers(self, key: str) -> Set[str]:
        """Get all set members."""
        pass

    @abstractmethod
    async def smismember(self, key: str, values: Union[List[Any], Any]) -> Union[List[int], int]:
        """Check if values are members of set."""
        pass

    @abstractmethod
    async def srem(self, key: str, *members: Union[str, int, float]) -> int:
        """Remove members from set."""
        pass

    @abstractmethod
    async def scard(self, key: str) -> int:
        """Get set cardinality."""
        pass

    # Atomic operations
    @abstractmethod
    async def incr(self, key: str) -> int:
        """Increment key value."""
        pass

    @abstractmethod
    async def decr(self, key: str) -> int:
        """Decrement key value."""
        pass

    # Stream operations
    @abstractmethod
    async def xadd(
        self, stream: str, fields: Dict[str, Any], id: str = "*", maxlen: Optional[int] = None, approximate: bool = True
    ) -> str:
        """Add entry to stream."""
        pass

    @abstractmethod
    async def xread(self, streams: Dict[str, str], count: Optional[int] = None, block: Optional[int] = None) -> List[Dict]:
        """Read from streams."""
        pass

    @abstractmethod
    async def xrange(self, stream: str, start: str = "-", end: str = "+", count: Optional[int] = None) -> List[Dict]:
        """Read range of entries from stream."""
        pass

    @abstractmethod
    async def xrevrange(self, stream: str, start: str = "+", end: str = "-", count: Optional[int] = None) -> List[Dict]:
        """Read range of entries from stream in reverse."""
        pass

    @abstractmethod
    async def xlen(self, stream: str) -> int:
        """Get stream length."""
        pass

    @abstractmethod
    async def xdel(self, stream: str, *ids: str) -> int:
        """Delete entries from stream."""
        pass

    @abstractmethod
    async def xinfo_stream(self, stream: str) -> Dict:
        """Get stream information."""
        pass

    @abstractmethod
    async def xtrim(self, stream: str, maxlen: int, approximate: bool = True) -> int:
        """Trim stream to maximum length."""
        pass

    # Distributed locking
    @abstractmethod
    async def acquire_conversation_lock(
        self,
        conversation_id: str,
        token: str,
    ) -> Optional[Any]:
        """Acquire distributed lock for conversation."""
        pass

    @abstractmethod
    async def release_conversation_lock(self, conversation_id: str) -> bool:
        """Release conversation lock."""
        pass

    # Letta-specific operations
    @abstractmethod
    async def check_inclusion_and_exclusion(self, member: str, group: str) -> bool:
        """Check if member is included/excluded from group."""
        pass

    @abstractmethod
    async def create_inclusion_exclusion_keys(self, group: str) -> None:
        """Create inclusion/exclusion keys for group."""
        pass


# Global singleton instance
_client_instance = None


async def _create_redis_backend():
    """Create Redis backend instance."""
    try:
        from letta.data_sources.redis_client import AsyncRedisClient
        from letta.settings import settings

        return AsyncRedisClient(
            host=settings.redis_host,
            port=settings.redis_port,
        )
    except ImportError:
        import logging
        from letta.data_sources.noop_client import NoopAsyncCacheClient

        logging.warning("Redis package not installed, falling back to noop cache backend")
        return NoopAsyncCacheClient()


async def _create_valkey_backend():
    """Create Valkey backend instance."""
    import logging
    from letta.data_sources.valkey_client import ValkeyBackend as AsyncValkeyClient
    from letta.settings import settings

    logging.info(f"Creating Valkey backend with host={settings.valkey_host!r} (type={type(settings.valkey_host)}), port={settings.valkey_port!r} (type={type(settings.valkey_port)})")
    
    return AsyncValkeyClient(
        host=settings.valkey_host,
        port=settings.valkey_port,
    )


async def get_cache_client() -> CacheBackend:
    """
    Factory function to get appropriate cache backend.

    Returns singleton instance based on configuration.
    """
    import logging
    from letta.data_sources.noop_client import NoopBackend as NoopAsyncCacheClient
    from letta.settings import settings

    global _client_instance

    if _client_instance is None:
        backend_type = settings.cache_backend_type

        # Backward compatibility: if backend_type not set but redis_host is, use redis
        if backend_type is None and settings.redis_host is not None:
            backend_type = "redis"

        if backend_type == "redis":
            _client_instance = await _create_redis_backend()
        elif backend_type == "valkey":
            _client_instance = await _create_valkey_backend()
        else:
            _client_instance = NoopAsyncCacheClient()
        logging.info(f"Using cache backend: {backend_type or 'noop'}")

    return _client_instance


# Backward compatibility alias
async def get_redis_client() -> CacheBackend:
    """Backward compatibility alias for get_cache_client."""
    return await get_cache_client()
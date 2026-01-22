"""
A no-op cache backend that does nothing, for environments where no cache is configured.
"""

from typing import Any, Dict, List, Optional, Set, Union
import uuid

from .cache_backend import CacheBackend


class NoopBackend(CacheBackend):
    """A no-op cache client."""

    def __init__(self):
        pass

    async def set(
        self,
        key: str,
        value: Union[str, int, float],
        ex: Optional[int] = None,
        px: Optional[int] = None,
        nx: bool = False,
        xx: bool = False,
    ) -> bool:
        return False

    async def get(self, key: str, default: Any = None) -> Any:
        return default

    async def exists(self, *keys: str) -> int:
        return 0

    async def sadd(self, key: str, *members: Union[str, int, float]) -> int:
        return 0

    async def smismember(self, key: str, values: list[Any] | Any) -> list[int] | int:
        return [0] * len(values) if isinstance(values, list) else 0

    async def delete(self, *keys: str) -> int:
        return 0

    async def acquire_conversation_lock(
        self,
        conversation_id: str,
        token: str,
    ) -> Optional["Lock"]:
        return None

    async def release_conversation_lock(self, conversation_id: str) -> bool:
        return False

    async def check_inclusion_and_exclusion(self, member: str, group: str) -> bool:
        return True

    async def create_inclusion_exclusion_keys(self, group: str) -> None:
        return None

    async def scard(self, key: str) -> int:
        return 0

    async def smembers(self, key: str) -> Set[str]:
        return set()

    async def srem(self, key: str, *members: Union[str, int, float]) -> int:
        return 0

    # Stream operations
    async def xadd(self, stream: str, fields: Dict[str, Any], id: str = "*", maxlen: Optional[int] = None, approximate: bool = True) -> str:
        return ""

    async def xread(self, streams: Dict[str, str], count: Optional[int] = None, block: Optional[int] = None) -> List[Dict]:
        return []

    async def xrange(self, stream: str, start: str = "-", end: str = "+", count: Optional[int] = None) -> List[Dict]:
        return []

    async def xrevrange(self, stream: str, start: str = "+", end: str = "-", count: Optional[int] = None) -> List[Dict]:
        return []

    async def xlen(self, stream: str) -> int:
        return 0

    async def xdel(self, stream: str, *ids: str) -> int:
        return 0

    async def xinfo_stream(self, stream: str) -> Dict:
        return {}

    async def xtrim(self, stream: str, maxlen: int, approximate: bool = True) -> int:
        return 0
      
    async def __aenter__(self):
        return self
      
    async def __aexit__(self, exc_type, exc_val, exc_tb):
       return None

    async def close(self):
        return None
      
    async def decr(self, key: str) -> int:
        return 0

    async def incr(self, key: str) -> int:
        return 0

    async def ping(self) -> bool:
        return True 
    
    async def get_client(self) -> Any:
        return await super().get_client()
      
    async def wait_for_ready(self, timeout: int = 30, interval: float = 0.5) -> None:
        return None
"""
A no-op cache backend that does nothing, for environments where no cache is configured.
"""

from typing import Any, Dict, List, Optional, Set, Union

from .cache_backend import CacheBackend


class NoopBackend(CacheBackend):
    """A no-op cache client that does nothing."""

    async def get_client(self) -> None:
        return None

    async def close(self) -> None:
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    async def ping(self) -> bool:
        return True  # No-op is always "ready"

    async def wait_for_ready(self, timeout: int = 30, interval: float = 0.5) -> None:
        pass

    async def get(self, key: str, default: Any = None) -> Any:
        return default

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

    async def delete(self, *keys: str) -> int:
        return 0

    async def exists(self, *keys: str) -> int:
        return 0

    async def sadd(self, key: str, *members: Union[str, int, float]) -> int:
        return 0

    async def smembers(self, key: str) -> Set[str]:
        return set()

    async def smismember(self, key: str, values: Union[List[Any], Any]) -> Union[List[int], int]:
        if isinstance(values, list):
            return [0] * len(values)
        return 0

    async def srem(self, key: str, *members: Union[str, int, float]) -> int:
        return 0

    async def scard(self, key: str) -> int:
        return 0

    async def incr(self, key: str) -> int:
        return 0

    async def decr(self, key: str) -> int:
        return 0

    async def xadd(
        self, stream: str, fields: Dict[str, Any], id: str = "*", maxlen: Optional[int] = None, approximate: bool = True
    ) -> str:
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

    async def acquire_conversation_lock(self, conversation_id: str, token: str) -> Optional[Any]:
        return None

    async def release_conversation_lock(self, conversation_id: str) -> bool:
        return False

    async def check_inclusion_and_exclusion(self, member: str, group: str) -> bool:
        return True  # Default to allowed

    async def create_inclusion_exclusion_keys(self, group: str) -> None:
        pass
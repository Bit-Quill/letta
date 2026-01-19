"""
A no-op cache backend that does nothing, for environments where no cache is configured.
"""

from typing import Any, Dict, List, Optional, Set, Union
import uuid

from .cache_backend import CacheBackend


class NoopBackend(CacheBackend):
    """A no-op cache client that stores data in memory for testing."""

    def __init__(self):
        """Initialize in-memory storage."""
        self._store: Dict[str, Any] = {}
        self._sets: Dict[str, Set[str]] = {}
        self._locks: Dict[str, str] = {}  # conversation_id -> token

    async def get_client(self) -> "NoopBackend":
        """Return self since we're already the client."""
        return self

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
        return self._store.get(key, default)

    async def set(
        self,
        key: str,
        value: Union[str, int, float],
        ex: Optional[int] = None,
        px: Optional[int] = None,
        nx: bool = False,
        xx: bool = False,
    ) -> bool:
        # Handle NX (only if not exists)
        if nx and key in self._store:
            return False
        
        # Handle XX (only if exists)
        if xx and key not in self._store:
            return False
        
        self._store[key] = str(value)
        return True

    async def delete(self, *keys: str) -> int:
        count = 0
        for key in keys:
            if key in self._store:
                del self._store[key]
                count += 1
            if key in self._sets:
                del self._sets[key]
                count += 1
        return count

    async def exists(self, *keys: str) -> int:
        count = 0
        for key in keys:
            if key in self._store or key in self._sets:
                count += 1
        return count

    async def sadd(self, key: str, *members: Union[str, int, float]) -> int:
        if key not in self._sets:
            self._sets[key] = set()
        
        count = 0
        for member in members:
            str_member = str(member)
            if str_member not in self._sets[key]:
                self._sets[key].add(str_member)
                count += 1
        
        return count

    async def smembers(self, key: str) -> Set[str]:
        return self._sets.get(key, set()).copy()

    async def smismember(self, key: str, values: Union[List[Any], Any]) -> Union[List[int], int]:
        members = self._sets.get(key, set())
        
        if isinstance(values, list):
            return [1 if str(v) in members else 0 for v in values]
        return 1 if str(values) in members else 0

    async def srem(self, key: str, *members: Union[str, int, float]) -> int:
        if key not in self._sets:
            return 0
        
        count = 0
        for member in members:
            str_member = str(member)
            if str_member in self._sets[key]:
                self._sets[key].remove(str_member)
                count += 1
        
        return count

    async def scard(self, key: str) -> int:
        return len(self._sets.get(key, set()))

    async def incr(self, key: str) -> int:
        if key not in self._store:
            self._store[key] = "0"
        
        try:
            value = int(self._store[key])
            value += 1
            self._store[key] = str(value)
            return value
        except (ValueError, TypeError):
            raise ValueError(f"value is not an integer or out of range")

    async def decr(self, key: str) -> int:
        if key not in self._store:
            self._store[key] = "0"
        
        try:
            value = int(self._store[key])
            value -= 1
            self._store[key] = str(value)
            return value
        except (ValueError, TypeError):
            raise ValueError(f"value is not an integer or out of range")

    async def xadd(
        self, stream: str, fields: Dict[str, Any], id: str = "*", maxlen: Optional[int] = None, approximate: bool = True
    ) -> str:
        # Generate an entry ID if not provided
        if id == "*":
            import time
            timestamp = int(time.time() * 1000)
            id = f"{timestamp}-0"
        
        # Store the stream entry
        if stream not in self._store:
            self._store[stream] = []
        
        entry = (id, fields.copy())
        self._store[stream].append(entry)
        
        # Handle maxlen trimming
        if maxlen and len(self._store[stream]) > maxlen:
            if approximate:
                # Approximate: just remove old entries
                self._store[stream] = self._store[stream][-maxlen:]
            else:
                # Exact: remove oldest entries to match maxlen
                self._store[stream] = self._store[stream][-maxlen:]
        
        return id

    async def xread(self, streams: Dict[str, str], count: Optional[int] = None, block: Optional[int] = None) -> List[Dict]:
        result = []
        
        for stream, start_id in streams.items():
            if stream not in self._store or not isinstance(self._store[stream], list):
                continue
            
            entries = self._store[stream]
            
            # Filter entries starting from start_id
            filtered = []
            if start_id == "0":
                filtered = entries
            else:
                for entry_id, entry_fields in entries:
                    if entry_id > start_id:
                        filtered.append((entry_id, entry_fields))
            
            if count:
                filtered = filtered[:count]
            
            if filtered:
                result.append([stream.encode(), filtered])
        
        return result

    async def xrange(self, stream: str, start: str = "-", end: str = "+", count: Optional[int] = None) -> List[Dict]:
        if stream not in self._store or not isinstance(self._store[stream], list):
            return []
        
        entries = self._store[stream]
        
        if count:
            entries = entries[:count]
        
        return entries

    async def xrevrange(self, stream: str, start: str = "+", end: str = "-", count: Optional[int] = None) -> List[Dict]:
        if stream not in self._store or not isinstance(self._store[stream], list):
            return []
        
        entries = list(reversed(self._store[stream]))
        
        if count:
            entries = entries[:count]
        
        return entries

    async def xlen(self, stream: str) -> int:
        if stream not in self._store or not isinstance(self._store[stream], list):
            return 0
        
        return len(self._store[stream])

    async def xdel(self, stream: str, *ids: str) -> int:
        if stream not in self._store or not isinstance(self._store[stream], list):
            return 0
        
        count = 0
        entries = self._store[stream]
        for id_to_delete in ids:
            for i, (entry_id, _) in enumerate(entries):
                if entry_id == id_to_delete:
                    entries.pop(i)
                    count += 1
                    break
        
        return count

    async def xinfo_stream(self, stream: str) -> Dict:
        if stream not in self._store or not isinstance(self._store[stream], list):
            return {"length": 0}
        
        return {"length": len(self._store[stream])}

    async def xtrim(self, stream: str, maxlen: int, approximate: bool = True) -> int:
        if stream not in self._store or not isinstance(self._store[stream], list):
            return 0
        
        entries = self._store[stream]
        original_len = len(entries)
        
        if original_len > maxlen:
            self._store[stream] = entries[-maxlen:]
            return original_len - maxlen
        
        return 0

    async def ttl(self, key: str) -> int:
        """Return a positive TTL for locks (simulating expiry)."""
        # For locks (keys starting with "conversation:lock:"), return a positive TTL
        if key.startswith("conversation:lock:"):
            return 300  # 5 minutes
        # For other keys, return -1 (no expiry)
        return -1

    async def acquire_conversation_lock(self, conversation_id: str, token: str) -> Optional[Any]:
        from letta.errors import ConversationBusyError
        
        if conversation_id in self._locks:
            existing_token = self._locks[conversation_id]
            if existing_token != token:
                raise ConversationBusyError(conversation_id=conversation_id)
        
        # Store the lock with a token
        lock_id = str(uuid.uuid4())
        self._locks[conversation_id] = token
        return lock_id

    async def release_conversation_lock(self, conversation_id: str) -> bool:
        if conversation_id in self._locks:
            del self._locks[conversation_id]
            return True
        return False

    async def check_inclusion_and_exclusion(self, member: str, group: str) -> bool:
        return True  # Default to allowed

    async def create_inclusion_exclusion_keys(self, group: str) -> None:
        """Create inclusion and exclusion set keys for a group."""
        include_key = f"{group}:include"
        exclude_key = f"{group}:exclude"
        
        # Create empty sets for inclusion and exclusion
        if include_key not in self._sets:
            self._sets[include_key] = set()
        if exclude_key not in self._sets:
            self._sets[exclude_key] = set()


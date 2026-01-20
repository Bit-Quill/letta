"""
Valkey backend implementation using the valkey-glide client.
"""

import asyncio
from typing import Any, Dict, List, Optional, Set, Union, Mapping

from glide import (
    ExpirySet,
    ExpiryType,
    GlideClient,
    GlideClientConfiguration,
    NodeAddress,
    RequestError,
    ConditionalChange,
    StreamTrimOptions,
    StreamAddOptions,
    )

from letta.data_sources.cache_backend import CacheBackend
from letta.errors import ConversationBusyError
from letta.log import get_logger

logger = get_logger(__name__)


class ValkeyBackend(CacheBackend):
    """Valkey backend implementation."""

    def __init__(self, host: str, port: int, **kwargs):
        self.host = host
        self.port = port
        self.client_config = GlideClientConfiguration(
            addresses=[NodeAddress(host=host, port=port)],
            # Other config options can be added here
        )
        self._client: Optional[GlideClient] = None
        

    async def get_client(self) -> GlideClient:
      if self._client is None:
          self._client = await GlideClient.create(self.client_config)
      return self._client


    async def close(self) -> None:
        if self._client:
            # glide-py does not have an explicit close method,
            # connection is managed by the underlying Rust core.
            self._client = None

    async def __aenter__(self):
        await self.get_client()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def ping(self) -> bool:
        try:
            client = await self.get_client()
            result = await client.ping()
            return result.decode("utf-8") == "PONG"
        except Exception as e:
            logger.error(f"Valkey ping failed: {e}")
            return False

    async def wait_for_ready(self, timeout: int = 30, interval: float = 0.5) -> None:
        end_time = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < end_time:
            if await self.ping():
                return
            await asyncio.sleep(interval)
        raise ConnectionError(f"Valkey not ready at {self.host}:{self.port} after {timeout}s")

    async def get(self, key: str, default: Any = None) -> Any:
        try:
            client = await self.get_client()
            value = await client.get(key)
            if value is None:
                return default
            if isinstance(value, bytes):
                return value.decode("utf-8")
            return value
        except RequestError:
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
        client = await self.get_client()

        if nx and xx:
            raise ValueError("Cannot set both nx and xx")

        conditional_set = None
        if nx:
            conditional_set = ConditionalChange("NX")
        elif xx:
            conditional_set = ConditionalChange("XX")
      

        expiry = None
        if ex is not None:
            expiry = ExpirySet(ExpiryType.SEC, ex)
        elif px is not None:
            expiry = ExpirySet(ExpiryType.MSEC, px)

        result = await client.set(
            key, str(value), conditional_set=conditional_set, expiry=expiry
        )

        if (nx and result is None) or (xx and result is None):
            return False

        return result == "OK"

    async def delete(self, *keys: str) -> int:
        client = await self.get_client()
        if not keys:
            return 0
        return await client.delete(list(keys))

    async def exists(self, *keys: str) -> int:
        client = await self.get_client()
        return await client.exists(list(keys))

    async def sadd(self, key: str, *members: Union[str, int, float]) -> int:
        client = await self.get_client()
        return await client.sadd(key, [str(m) for m in members])

    async def smembers(self, key: str) -> Set[str]:
        client = await self.get_client()
        result =await client.smembers(key) 
        return {member.decode("utf-8") if isinstance(member, bytes) else member for member in result}

    async def smismember(self, key: str, values: Union[List[Any], Any]) -> Union[List[int], int]:
        client = await self.get_client()
        is_single = not isinstance(values, list)
        if is_single:
            values = [values]
        
        result = await client.smismember(key, [str(v) for v in values])
        
        if is_single:
            return int(result[0]) if result else 0
        
        return [int(r) for r in result]

    async def srem(self, key: str, *members: Union[str, int, float]) -> int:
        client = await self.get_client()
        return await client.srem(key, [str(m) for m in members])

    async def scard(self, key: str) -> int:
        client = await self.get_client()
        return await client.scard(key)

    async def incr(self, key: str) -> int:
        client = await self.get_client()
        return await client.incr(key)

    async def decr(self, key: str) -> int:
        client = await self.get_client()
        return await client.decr(key)

    async def xadd(
        self, stream: str, fields: Dict[str, Any], id: str = "*", maxlen: Optional[int] = None, approximate: bool = True
    ) -> str:
        client = await self.get_client()
        field_items = list(fields.items())

        trim_options = None
        if maxlen is not None:
            trim_options = StreamTrimOptions(exact=not approximate, threshold=maxlen, kind="MAXLEN")

        # glide-py xadd has a slightly different signature
        options = StreamAddOptions(id=id, trim=trim_options)
        result = await client.xadd(stream, field_items, options=options)
        return result.decode("utf-8") if isinstance(result, bytes) else result if result else ""
    
    def convert(mapping: Optional[Mapping[bytes, Mapping[bytes, List[List[bytes]]]]]) -> List[Dict]:
        if not mapping:
            return []
        return [
            {
                outer_key.decode(): {
                    inner_key.decode(): [
                        [item.decode() for item in inner_list]
                        for inner_list in value
                    ]
                    for inner_key, value in inner_mapping.items()
                }
            }
            for outer_key, inner_mapping in mapping.items()
        ]
  
    async def xread(self, streams: Dict[str, str], count: Optional[int] = None, block: Optional[int] = None) -> List[Dict]:
        client = await self.get_client()
        options = StreamAddOptions(
            block, count
        )
        result = await client.xread(streams)
        logger.info(f"xread result: {result}")
        return self.convert(result)

    async def xrange(self, stream: str, start: str = "-", end: str = "+", count: Optional[int] = None) -> List[Dict]:
        client = await self.get_client()
        return await client.xrange(stream, start, end, count=count)

    async def xrevrange(self, stream: str, start: str = "+", end: str = "-", count: Optional[int] = None) -> List[Dict]:
        client = await self.get_client()
        return await client.xrevrange(stream, start, end, count=count)

    async def xlen(self, stream: str) -> int:
        client = await self.get_client()
        return await client.xlen(stream)

    async def xdel(self, stream: str, *ids: str) -> int:
        client = await self.get_client()
        return await client.xdel(stream, list(ids))

    async def xinfo_stream(self, stream: str) -> Dict:
        client = await self.get_client()
        # glide-py does not have a direct xinfo_stream, this would need a custom command
        # Returning empty dict as a placeholder
        return {}

    async def xtrim(self, stream: str, maxlen: int, approximate: bool = True) -> int:
        client = await self.get_client()
        return await client.xtrim(stream, "MAXLEN", maxlen, approximate=approximate)

    async def acquire_conversation_lock(self, conversation_id: str, token: str) -> Optional[Any]:
        # This is a complex operation that might need a more direct implementation
        # with glide-py if the high-level lock from redis-py isn't available.
        # Placeholder implementation:
        key = f"conversation:lock:{conversation_id}"
        from letta.constants import CONVERSATION_LOCK_TTL_SECONDS

        client = await self.get_client()
        # Use SET NX to acquire the lock
        result = await self.set(key, token, ex=CONVERSATION_LOCK_TTL_SECONDS, nx=True)

        if result:
            return True  # Lock acquired

        # If lock is not acquired, raise ConversationBusyError
        lock_holder_token = await self.get(key)
        raise ConversationBusyError(conversation_id=conversation_id, lock_holder_token=lock_holder_token)

    async def release_conversation_lock(self, conversation_id: str) -> bool:
        key = f"conversation:lock:{conversation_id}"
        await self.delete(key)
        return True

    async def check_inclusion_and_exclusion(self, member: str, group: str) -> bool:
        client = await self.get_client()
        # This requires a transaction or Lua script for atomicity
        # Simplified version:
        is_excluded = await client.smismember(f"{group}:exclude", member)
        if is_excluded:
            return False
        has_includes = await client.exists([f"{group}:include"])
        if not has_includes:
            return True
        is_included = await client.smismember(f"{group}:include", member)
        return bool(is_included)

    async def create_inclusion_exclusion_keys(self, group: str) -> None:
        client = await self.get_client()
        # This is a no-op if the keys already exist, which is fine.
        await client.sadd(f"{group}:include", "placeholder-for-creation")
        await client.srem(f"{group}:include", "placeholder-for-creation")
        await client.sadd(f"{group}:exclude", "placeholder-for-creation")
        await client.srem(f"{group}:exclude", "placeholder-for-creation")

    async def ttl(self, key: str) -> int:
        client = await self.get_client()
        return await client.ttl(key)
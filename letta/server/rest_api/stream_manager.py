"""Abstract stream manager interface for SSE streaming with different cache backends."""

from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, AsyncIterator
from typing import Optional

from letta.schemas.user import User
from letta.services.run_manager import RunManager


class SSEStreamWriter(ABC):
    """Abstract base class for SSE stream writers."""

    @abstractmethod
    async def start(self):
        """Start the background flush task."""
        pass

    @abstractmethod
    async def stop(self):
        """Stop the background flush task and flush remaining data."""
        pass

    @abstractmethod
    async def write_chunk(
        self,
        run_id: str,
        data: str,
        is_complete: bool = False,
    ) -> int:
        """
        Write an SSE chunk to the buffer for a specific run.

        Args:
            run_id: The run ID to write to
            data: SSE-formatted chunk data
            is_complete: Whether this is the final chunk

        Returns:
            The sequence ID assigned to this chunk
        """
        pass

    @abstractmethod
    async def mark_complete(self, run_id: str):
        """Mark a stream as complete and flush."""
        pass


async def create_background_stream_processor(
    stream_generator: AsyncGenerator[str | bytes | tuple[str | bytes, int], None],
    cache_client,
    run_id: str,
    writer: Optional[SSEStreamWriter] = None,
    run_manager: Optional[RunManager] = None,
    actor: Optional[User] = None,
    conversation_id: Optional[str] = None,
) -> None:
    """
    Process a stream in the background and store chunks to cache backend.

    This function delegates to the appropriate backend-specific implementation.

    Args:
        stream_generator: The async generator yielding SSE chunks
        cache_client: Cache client instance
        run_id: The run ID to store chunks under
        writer: Optional pre-configured writer
        run_manager: Optional run manager for updating run status
        actor: Optional actor for run status updates
        conversation_id: Optional conversation ID for releasing lock on terminal states
    """
    # Import here to avoid circular dependencies
    from letta.data_sources.cache_backend import CacheBackend
    from letta.data_sources.noop_client import NoopBackend as NoopAsyncCacheClient

    if isinstance(cache_client, (NoopAsyncCacheClient, CacheBackend)):
        # No-op: just consume the stream
        async for _ in stream_generator:
            pass
        return

    # Try Redis implementation
    try:
        from letta.data_sources.redis_client import AsyncRedisClient
        from letta.server.rest_api.redis_stream_manager import (
            create_background_stream_processor as redis_processor,
        )

        if isinstance(cache_client, AsyncRedisClient):
            await redis_processor(stream_generator, cache_client, run_id, writer, run_manager, actor, conversation_id)
            return
    except ImportError:
        pass

    # Try Valkey implementation
    try:
        from letta.data_sources.valkey_client import AsyncValkeyClient
        from letta.server.rest_api.valkey_stream_manager import (
            create_background_stream_processor as valkey_processor,
        )

        if isinstance(cache_client, AsyncValkeyClient):
            await valkey_processor(stream_generator, cache_client, run_id, writer, run_manager, actor, conversation_id)
            return
    except ImportError:
        pass

    # Fallback: consume stream without storing
    async for _ in stream_generator:
        pass


async def sse_stream_generator(
    cache_client,
    run_id: str,
    starting_after: Optional[int] = None,
    poll_interval: float = 0.1,
    batch_size: int = 100,
) -> AsyncIterator[str]:
    """
    Generate SSE events from cache backend stream chunks.

    This function delegates to the appropriate backend-specific implementation.

    Args:
        cache_client: Cache client instance
        run_id: The run ID to read chunks for
        starting_after: Sequential ID to start reading from
        poll_interval: Seconds to wait between polls
        batch_size: Number of entries to read per batch

    Yields:
        SSE-formatted chunks from the stream
    """
    # Import here to avoid circular dependencies
    from letta.data_sources.cache_backend import NoopBackend
    from letta.data_sources.noop_client import NoopAsyncCacheClient

    if isinstance(cache_client, (NoopBackend, NoopAsyncCacheClient)):
        # No-op: yield nothing
        return

    # Try Redis implementation
    try:
        from letta.data_sources.redis_client import AsyncRedisClient
        from letta.server.rest_api.redis_stream_manager import redis_sse_stream_generator

        if isinstance(cache_client, AsyncRedisClient):
            async for chunk in redis_sse_stream_generator(cache_client, run_id, starting_after, poll_interval, batch_size):
                yield chunk
            return
    except ImportError:
        pass

    # Try Valkey implementation
    try:
        from letta.data_sources.valkey_client import AsyncValkeyClient
        from letta.server.rest_api.valkey_stream_manager import valkey_sse_stream_generator

        if isinstance(cache_client, AsyncValkeyClient):
            async for chunk in valkey_sse_stream_generator(cache_client, run_id, starting_after, poll_interval, batch_size):
                yield chunk
            return
    except ImportError:
        pass

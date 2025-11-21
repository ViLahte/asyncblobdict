from typing import Protocol


class AsyncBlobHandle(Protocol):
    """Represents a single blob in storage."""

    async def download(self) -> bytes:
        """Download blob contents as bytes."""
        ...

    async def upload(
        self, data: bytes, overwrite: bool = True, if_match: str | None = None
    ) -> None:
        """Upload bytes to blob."""
        ...

    async def delete(self) -> None:
        """Delete blob."""
        ...

    async def get_etag(self) -> str | None:
        """Return blob's ETag or None if not available."""
        ...


class AsyncContainerHandle(Protocol):
    """Represents a container/bucket in storage."""

    def get_blob(self, blob_name: str) -> AsyncBlobHandle:
        """Return a handle to a blob."""
        ...

    async def list_blob_names(self, prefix: str = "") -> list[str]:
        """List blob names in container."""
        ...


class AsyncStorageAdapter(Protocol):
    """Protocol for a storage backend adapter."""

    def get_container(self, container_name: str) -> AsyncContainerHandle:
        """Return a handle to a container."""
        ...

    async def close(self) -> None:
        """Close any resources/connections."""
        ...

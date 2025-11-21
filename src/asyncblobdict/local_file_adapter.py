import asyncio
import hashlib
from pathlib import Path

from .async_blob_store import ConcurrencyError

from .storage_protocols import (
    AsyncBlobHandle,
    AsyncContainerHandle,
    AsyncStorageAdapter,
)


class LocalFileAdapter(AsyncStorageAdapter):
    """
    Local filesystem adapter for AsyncBlobStore.
    Stores blobs as files in a given base directory.
    """

    def __init__(self, base_path: str):
        self.base_path = Path(base_path).resolve()
        self.base_path.mkdir(parents=True, exist_ok=True)

    def get_container(self, container_name: str) -> AsyncContainerHandle:
        container_path = self.base_path / container_name
        container_path.mkdir(parents=True, exist_ok=True)
        return _LocalContainerHandle(container_path)

    async def close(self) -> None:
        # Nothing to close for local filesystem
        pass


class _LocalContainerHandle(AsyncContainerHandle):
    def __init__(self, container_path: Path):
        self.container_path = container_path

    def get_blob(self, blob_name: str) -> AsyncBlobHandle:
        # Allow nested paths in blob names
        return _LocalBlobHandle(self.container_path / blob_name)

    async def list_blob_names(self, prefix: str = "") -> list[str]:
        files: list[str] = []
        for path in self.container_path.rglob("*"):
            if path.is_file():
                rel_path = path.relative_to(self.container_path).as_posix()
                if rel_path.startswith(prefix):
                    files.append(rel_path)
        return files


class _LocalBlobHandle(AsyncBlobHandle):
    def __init__(self, file_path: Path):
        self.file_path = file_path
        self._lock = asyncio.Lock()

    async def download(self) -> bytes:
        if not self.file_path.exists():
            raise FileNotFoundError()
        async with self._lock:
            return self.file_path.read_bytes()

    async def upload(
        self, data: bytes, overwrite: bool = True, if_match: str | None = None
    ) -> None:
        if self.file_path.exists() and not overwrite:
            raise FileExistsError(f"Blob {self.file_path} already exists")

        if if_match is not None:
            current_etag = await self.get_etag()
            if current_etag != if_match:
                raise ConcurrencyError(f"ETag mismatch for blob '{self.file_path}'")

        # Ensure parent directory exists
        self.file_path.parent.mkdir(parents=True, exist_ok=True)

        async with self._lock:
            self.file_path.write_bytes(data)

    async def delete(self) -> None:
        if not self.file_path.exists():
            raise FileNotFoundError()
        self.file_path.unlink()

    async def get_etag(self) -> str | None:
        if not self.file_path.exists():
            raise FileNotFoundError()
        content = self.file_path.read_bytes()
        return hashlib.md5(content).hexdigest()
